import os
import json
import datetime
import logging
import asyncio
from typing import Optional, Dict, Any, cast

from gateway.config import BASE_DIR, GatewayConfig
from gateway.shared.models import Job, JobStatus
from gateway.shared.interfaces import (
    JobDispatcher,
    JobQueueReader,
    JobQueueWriter,
    DownstreamClient,
    EventBus,
)
from gateway.shared.exceptions import DownstreamError
from gateway.shared.events import DispatchSuccessEvent, DispatchFailedEvent

logger = logging.getLogger(__name__)


class ComfyUIJobDispatcher(JobDispatcher):
    """ComfyUI 技术规范任务分派器，负责调用下游发送任务以及维护队列副作用。"""

    def __init__(
        self,
        config: GatewayConfig,
        queue_reader: JobQueueReader,
        queue_writer: JobQueueWriter,
        downstream: DownstreamClient,
        event_bus: EventBus,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._config = config
        self._queue_reader = queue_reader
        self._queue_writer = queue_writer
        self._downstream = downstream
        self._event_bus = event_bus
        self._loop = loop
        self._dispatching: bool = False
        self._exiting: bool = False

    def dispatch(self, skip: Optional[int]) -> None:
        """线程安全地触发一次任务派发尝试。"""
        logger.debug("dispatch called: skip=%s", skip)
        self._loop.call_soon_threadsafe(self._schedule_dispatch, skip)

    def _schedule_dispatch(self, skip: Optional[int]) -> None:
        logger.debug("_schedule_dispatch: creating task, skip=%s", skip)
        asyncio.create_task(self._try_send(skip))

    async def _try_send(self, skip: Optional[int]) -> None:
        """执行派发任务的具体副作用。"""
        logger.debug(
            "_try_send: entered, skip=%s dispatching=%s exiting=%s",
            skip,
            self._dispatching,
            self._exiting,
        )
        if self._dispatching or self._exiting:
            logger.debug(
                "_try_send: skipped (dispatching=%s exiting=%s)",
                self._dispatching,
                self._exiting,
            )
            return
        self._dispatching = True
        try:
            if skip is None:
                logger.debug("_try_send: skip is None, nothing to dispatch")
                return

            job = self._queue_writer.pop(skip)
            if job is None:
                logger.warning("_try_send: pop(skip=%s) returned None", skip)
                return

            logger.info("_try_send: dispatching job %s (skip=%s)", job.prompt_id, skip)

            extra_data = json.loads(job.extra_data)
            body: Dict[str, Any] = {
                "prompt": job.prompt,
                "prompt_id": job.prompt_id,
                "extra_data": job.extra_data,
            }
            if extra_data.get("client_id"):
                body["client_id"] = extra_data["client_id"]

            try:
                await self._downstream.send_prompt(job.prompt_id, body)
                logger.info(f"📤 Sent workflow {job.prompt_id} to downstream")
                self._event_bus.publish(DispatchSuccessEvent(prompt_id=job.prompt_id))
            except DownstreamError as de:
                logger.error(
                    f"Failed to send workflow {job.prompt_id}: {de.status_code} - {de.message}"
                )
                is_permanent = 400 <= de.status_code <= 500
                self._event_bus.publish(
                    DispatchFailedEvent(
                        prompt_id=job.prompt_id, is_permanent=is_permanent
                    )
                )

                if is_permanent:
                    self._queue_writer.update_status(
                        JobStatus.FAILED, prompt_id=job.prompt_id
                    )
                    try:
                        self._save_failed_workflow(
                            job, de.message, body, extra_data, de.status_code
                        )
                    except (OSError, TypeError) as save_err:
                        logger.error(
                            f"Failed to save failed workflow details: {save_err}"
                        )
                else:
                    self._queue_writer.requeue_running()
            except (json.JSONDecodeError, OSError) as e:
                logger.error(f"Error sending workflow: {e}")
                self._event_bus.publish(
                    DispatchFailedEvent(prompt_id=job.prompt_id, is_permanent=False)
                )
                self._queue_writer.requeue_running()
        finally:
            self._dispatching = False

    def _save_failed_workflow(
        self,
        task: Job,
        error_msg: str,
        body: Dict[str, Any],
        extra_data: Dict[str, Any],
        status_code: int,
    ) -> None:
        """将失败的工作流保存至网关数据目录的 failed_workflows 子目录下备份，避免死循环。"""
        date_str = datetime.date.today().isoformat()
        dir_name = f"{date_str}-{status_code}-{task.prompt_id}"
        failed_dir = os.path.join(self._config.data_dir, "failed_workflows", dir_name)
        os.makedirs(failed_dir, exist_ok=True)

        with open(os.path.join(failed_dir, "error.txt"), "w", encoding="utf-8") as f:
            f.write(error_msg)

        with open(os.path.join(failed_dir, "request.json"), "w", encoding="utf-8") as f:
            json.dump(body, f, ensure_ascii=False, indent=2)

        workflow: Optional[Dict[str, Any]] = None
        extra_pnginfo = extra_data.get("extra_pnginfo")
        if isinstance(extra_pnginfo, dict):
            extra_pnginfo_dict = cast(Dict[str, Any], extra_pnginfo)
            workflow = cast(
                Optional[Dict[str, Any]], extra_pnginfo_dict.get("workflow")
            )
        if not workflow:
            workflow = cast(Optional[Dict[str, Any]], extra_data.get("workflow"))

        if workflow:
            with open(
                os.path.join(failed_dir, "workflow.json"), "w", encoding="utf-8"
            ) as f:
                json.dump(workflow, f, ensure_ascii=False, indent=2)

        rel_failed_dir = os.path.relpath(failed_dir, BASE_DIR).replace(os.sep, "/")
        logger.info(f"💾 Failed workflow {task.prompt_id} saved to {rel_failed_dir}")

    def handle_failed_job(self, job: Job, error_msg: str) -> None:
        """处理执行失败的坏任务（备份至 failed_workflows 目录）。"""
        try:
            extra_data = json.loads(job.extra_data)
            body: Dict[str, Any] = {
                "prompt": job.prompt,
                "prompt_id": job.prompt_id,
                "extra_data": job.extra_data,
            }
            if extra_data.get("client_id"):
                body["client_id"] = extra_data["client_id"]
            self._save_failed_workflow(job, error_msg, body, extra_data, 500)
        except (OSError, json.JSONDecodeError, TypeError) as e:
            logger.error(f"Failed to handle failed task: {e}")

    def shutdown(self) -> None:
        """关闭分派器，拒绝新的派发。"""
        self._exiting = True
