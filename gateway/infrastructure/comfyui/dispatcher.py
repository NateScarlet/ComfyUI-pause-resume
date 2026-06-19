import os
import json
import datetime
import logging
import asyncio
from typing import Optional, Dict, Any, cast

from gateway.config import BASE_DIR, GatewayConfig
from gateway.shared.models import Task
from gateway.shared.interfaces import (
    TaskDispatcher,
    TaskQueueReader,
    TaskQueueWriter,
    DownstreamClient,
    EventBus,
)
from gateway.shared.exceptions import DownstreamError
from gateway.domain.gateway import Gateway
from gateway.shared.events import DispatchSuccessEvent, DispatchFailedEvent

logger = logging.getLogger(__name__)


class ComfyUITaskDispatcher(TaskDispatcher):
    """ComfyUI 技术规范任务分派器，负责调用下游发送任务以及维护队列副作用。"""

    def __init__(
        self,
        config: GatewayConfig,
        queue_reader: TaskQueueReader,
        queue_writer: TaskQueueWriter,
        downstream: DownstreamClient,
        event_bus: EventBus,
    ) -> None:
        self._config = config
        self._queue_reader = queue_reader
        self._queue_writer = queue_writer
        self._downstream = downstream
        self._event_bus = event_bus
        self._gateway: Optional[Gateway] = None
        self._dispatching: bool = False
        self._exiting: bool = False

    def set_gateway(self, gateway: Gateway) -> None:
        """延迟注入领域聚合根实例。"""
        self._gateway = gateway

    @property
    def gateway(self) -> Gateway:
        assert self._gateway is not None, "Gateway must be set before use"
        return self._gateway

    def try_dispatch(self) -> None:
        """线程安全地触发一次任务派发尝试。"""
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            return

        if loop.is_running():
            loop.call_soon_threadsafe(self._schedule_dispatch)

    def _schedule_dispatch(self) -> None:
        asyncio.create_task(self._try_send_task())

    async def _try_send_task(self) -> None:
        """执行派发任务的具体副作用。"""
        if self._dispatching or self._exiting:
            return
        self._dispatching = True
        try:
            skip = self.gateway.get_dispatch_skip()
            if skip is None:
                return

            task = self._queue_writer.pop_task(skip)
            if task is None:
                return

            extra_data = json.loads(task.extra_data)
            body: Dict[str, Any] = {
                "prompt": task.prompt,
                "prompt_id": task.prompt_id,
                "extra_data": task.extra_data,
            }
            if extra_data.get("client_id"):
                body["client_id"] = extra_data["client_id"]

            try:
                await self._downstream.send_prompt(task.prompt_id, body)
                logger.info(f"📤 Sent workflow {task.prompt_id} to downstream")
                self._event_bus.publish(DispatchSuccessEvent())
            except DownstreamError as de:
                logger.error(
                    f"Failed to send workflow {task.prompt_id}: {de.status_code} - {de.message}"
                )
                is_permanent = 400 <= de.status_code <= 500
                self._event_bus.publish(DispatchFailedEvent(is_permanent=is_permanent))

                if is_permanent:
                    self._queue_writer.clear_running()
                    try:
                        self._save_failed_workflow(
                            task, de.message, body, extra_data, de.status_code
                        )
                    except Exception as save_err:
                        logger.error(
                            f"Failed to save failed workflow details: {save_err}"
                        )
                else:
                    self._queue_writer.requeue_running()
            except Exception as e:
                logger.error(f"Error sending workflow: {e}")
                self._event_bus.publish(DispatchFailedEvent(is_permanent=False))
                self._queue_writer.requeue_running()
        finally:
            self._dispatching = False

    def _save_failed_workflow(
        self,
        task: Task,
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

    def shutdown(self) -> None:
        """关闭分派器，拒绝新的派发。"""
        self._exiting = True
