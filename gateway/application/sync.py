import json
import asyncio
import logging
from typing import Dict, Any, List, Tuple, Optional, Union, cast

from gateway.shared.interfaces import (
    JobQueueWriter,
    JobQueueReader,
    DownstreamClient,
    EventBus,
)
from gateway.shared.events import DispatchSuccessEvent, DownstreamExecutingChangedEvent
from gateway.shared.models import RawJSON
from gateway.shared.outputs_parser import parse_outputs_count, THREE_D_EXTENSIONS

logger = logging.getLogger(__name__)

PREVIEWABLE_MEDIA_TYPES = frozenset({"images", "video", "audio", "3d", "text"})


class JobDownstreamSyncer:
    """从物理下游 ComfyUI 异步同步任务省略信息的同步器。

    订阅派发成功事件和执行状态变化事件，在任务状态变化时从下游拉取完整信息并同步到网关。
    """

    def __init__(
        self,
        queue_reader: JobQueueReader,
        queue_writer: JobQueueWriter,
        downstream: DownstreamClient,
        event_bus: EventBus,
    ):
        self._queue_reader = queue_reader
        self._queue_writer = queue_writer
        self._downstream = downstream
        self._event_bus = event_bus

        # 订阅事件以触发同步副作用
        self._unsub_dispatch = self._event_bus.subscribe(
            DispatchSuccessEvent, self._on_dispatch_success
        )
        self._unsub_executing = self._event_bus.subscribe(
            DownstreamExecutingChangedEvent, self._on_executing_changed
        )

    def _on_dispatch_success(self, ev: DispatchSuccessEvent) -> None:
        """派发成功事件回调，触发批量同步。"""
        asyncio.create_task(self._sync_all())

    def _on_executing_changed(self, ev: DownstreamExecutingChangedEvent) -> None:
        """下游物理执行状态变化回调，当执行结束时触发批量同步。"""
        if not ev.executing:
            asyncio.create_task(self._sync_all())

    async def _sync_all(self) -> None:
        """从下游批量同步当前队列中所有可能存在的任务的 outputs 和 assets 数据。"""
        if not self._downstream.downstream_ready:
            return

        logger.debug("🔄 Bulk syncing all tasks from downstream...")
        try:
            # 1. 发起并发查询拉取 queue 和 history
            queue_data, history_data = await asyncio.gather(
                self._downstream.get_queue(),
                self._downstream.get_history(max_items=50),
                return_exceptions=True,
            )

            # 2. 批量处理队列中的任务（正在运行 & 挂起）
            if isinstance(queue_data, dict):
                queue_data_dict = queue_data
                for key in ("queue_running", "queue_pending"):
                    raw_list = queue_data_dict.get(key, [])
                    if isinstance(raw_list, list):
                        for item in cast(List[Any], raw_list):
                            if isinstance(item, list):
                                item_list = cast(List[Any], item)
                                if len(item_list) > 4:
                                    prompt_id = str(item_list[1])
                                    outputs_to_execute = cast(List[Any], item_list[4])
                                    ds_job = self._normalize_queue_item(item_list)
                                    await self._sync_single_job_data(
                                        prompt_id,
                                        [str(x) for x in outputs_to_execute],
                                        ds_job,
                                    )

            # 3. 批量处理已完成/已失败的历史记录
            if history_data is not None:
                history_data_dict = cast(Dict[str, Any], history_data)
                for prompt_id_raw, history_item in history_data_dict.items():
                    prompt_id = str(prompt_id_raw)
                    if history_item is not None:
                        history_item_dict = cast(Dict[str, Any], history_item)
                        prompt_tuple = history_item_dict.get("prompt")
                        if isinstance(prompt_tuple, list):
                            prompt_tuple_list = cast(List[Any], prompt_tuple)
                            if len(prompt_tuple_list) > 4:
                                outputs_to_execute = cast(
                                    List[Any], prompt_tuple_list[4]
                                )
                                ds_job = self._normalize_history_item(
                                    prompt_id, history_item_dict
                                )
                                await self._sync_single_job_data(
                                    prompt_id,
                                    [str(x) for x in outputs_to_execute],
                                    ds_job,
                                )

        except Exception as e:
            logger.error(f"❌ Error during bulk sync: {e}", exc_info=True)

    async def _sync_single_job_data(
        self, prompt_id: str, outputs_to_execute: List[str], ds_job: Dict[str, Any]
    ) -> None:
        """同步并更新本地数据库里的单条任务记录。"""
        try:
            task = self._queue_reader.get(prompt_id)
            if not task:
                return

            # 判断是否有新数据需要同步，减少重复存盘 I/O
            has_changes = False
            if not task.outputs_to_execute or list(task.outputs_to_execute) != list(
                outputs_to_execute
            ):
                has_changes = True

            # 比较旧的状态，或者任何执行起止时间是否不同
            if task.execution_start_time != ds_job.get(
                "execution_start_time"
            ) or task.execution_end_time != ds_job.get("execution_end_time"):
                has_changes = True

            if not has_changes:
                return

            new_outputs = (
                outputs_to_execute if outputs_to_execute else task.outputs_to_execute
            )

            # 分拆物理字段值
            ds_outputs = ds_job.get("outputs")
            outputs_json = (
                RawJSON(json.dumps(ds_outputs, ensure_ascii=False))
                if ds_outputs
                else None
            )

            ds_preview = ds_job.get("preview_output")
            preview_json = (
                RawJSON(json.dumps(ds_preview, ensure_ascii=False))
                if ds_preview
                else None
            )

            ds_error = ds_job.get("execution_error")
            error_json = (
                RawJSON(json.dumps(ds_error, ensure_ascii=False)) if ds_error else None
            )

            from dataclasses import replace

            updated_task = replace(
                task,
                outputs_to_execute=new_outputs,
                outputs=outputs_json,
                preview_output=preview_json,
                execution_start_time=ds_job.get("execution_start_time"),
                execution_end_time=ds_job.get("execution_end_time"),
                execution_error=error_json,
            )
            self._queue_writer.save(updated_task)
            logger.info(
                f"✅ Synced and saved task {prompt_id} details from downstream."
            )
        except Exception as e:
            logger.error(f"Error syncing single task {prompt_id}: {e}", exc_info=True)

    def _normalize_queue_item(self, item: List[Any]) -> Dict[str, Any]:
        """将物理下游原生 queue 元组规范化为 API Job 结构。"""
        priority = item[0]
        prompt_id = str(item[1])
        extra_data = item[3]
        create_time = None
        workflow_id = None
        if isinstance(extra_data, dict):
            extra_dict = cast(Dict[str, Any], extra_data)
            create_time = extra_dict.get("create_time")
            workflow_id = (
                extra_dict.get("extra_pnginfo", {}).get("workflow", {}).get("id")
            )

        return {
            "id": prompt_id,
            "status": "in_progress",
            "priority": priority,
            "create_time": create_time,
            "workflow_id": workflow_id,
            "outputs_count": 0,
        }

    def _normalize_history_item(
        self, prompt_id: str, item: Dict[str, Any]
    ) -> Dict[str, Any]:
        """将物理下游的原生 history 项规范化为 API Job 字典结构。"""
        prompt_tuple = item.get("prompt")
        if not isinstance(prompt_tuple, list):
            return {}
        prompt_tuple_list = cast(List[Any], prompt_tuple)
        if len(prompt_tuple_list) < 5:
            return {}

        priority = prompt_tuple_list[0]
        extra_data = prompt_tuple_list[3]
        create_time = None
        workflow_id = None
        if isinstance(extra_data, dict):
            extra_dict = cast(Dict[str, Any], extra_data)
            create_time = extra_dict.get("create_time")
            workflow_id = (
                extra_dict.get("extra_pnginfo", {}).get("workflow", {}).get("id")
            )

        status_info = cast(Dict[str, Any], item.get("status", {}))
        status_str = status_info.get("status_str") if status_info else None

        # 提取 outputs assets 预览与计数 (通过扁平的辅助函数)
        outputs = cast(Dict[str, Any], item.get("outputs", {}))
        outputs_count, preview_output = self._parse_outputs_assets(outputs)

        # 提取执行起止时间及报错 (通过扁平的辅助函数)
        execution_start_time, execution_end_time, execution_error = (
            self._parse_execution_times(status_info)
        )

        if status_str == "success":
            status = "completed"
        elif status_str == "error":
            status = "failed"
        else:
            status = "completed"

        res: Dict[str, Any] = {
            "id": prompt_id,
            "status": status,
            "priority": priority,
            "create_time": create_time,
            "outputs_count": outputs_count,
            "workflow_id": workflow_id,
            "outputs": outputs,
            "execution_status": status_info,
        }
        if preview_output is not None:
            res["preview_output"] = preview_output
        if execution_start_time is not None:
            res["execution_start_time"] = execution_start_time
        if execution_end_time is not None:
            res["execution_end_time"] = execution_end_time
        if execution_error is not None:
            res["execution_error"] = execution_error

        return res

    def _has_3d_extension(self, filename: str) -> bool:
        lower = filename.lower()
        return any(lower.endswith(ext) for ext in THREE_D_EXTENSIONS)

    def _normalize_output_item(self, item: Any) -> Optional[Dict[str, Any]]:
        if item is None:
            return None
        if isinstance(item, str):
            if self._has_3d_extension(item):
                return {
                    "filename": item,
                    "type": "output",
                    "subfolder": "",
                    "mediaType": "3d",
                }
            return None
        if isinstance(item, dict):
            return cast(Dict[str, Any], item)
        return None

    def _is_previewable(self, media_type: str, item: Dict[str, Any]) -> bool:
        if media_type in PREVIEWABLE_MEDIA_TYPES:
            return True
        fmt = item.get("format", "")
        if fmt and (fmt.startswith("video/") or fmt.startswith("audio/")):
            return True
        filename = item.get("filename", "").lower()
        if any(filename.endswith(ext) for ext in THREE_D_EXTENSIONS):
            return True
        return False

    def _create_text_preview(self, value: str) -> Dict[str, Any]:
        if len(value) <= 1024:
            return {"content": value}
        return {"content": value[:1024], "truncated": True}

    def _parse_outputs_assets(
        self, outputs: Dict[str, Any]
    ) -> Tuple[int, Optional[Dict[str, Any]]]:
        """从 outputs 字典中提取生成文件数量和预览项。"""
        count = parse_outputs_count(outputs)
        preview_output = None
        fallback_preview = None

        for node_id, node_outputs in outputs.items():
            node_outputs_dict = cast(Dict[str, Any], node_outputs)
            for media_type, items in node_outputs_dict.items():
                if media_type == "animated" or not isinstance(items, list):
                    continue

                for item in cast(List[Any], items):
                    if not isinstance(item, dict):
                        item_dict = self._normalize_output_item(item)
                        if item_dict is None:
                            if media_type == "text" and preview_output is None:
                                if isinstance(item, (list, tuple)):
                                    item_seq = cast(
                                        Union[List[Any], Tuple[Any, ...]], item
                                    )
                                    text_value = str(item_seq[0]) if item_seq else ""
                                else:
                                    text_value = str(item)
                                text_preview = self._create_text_preview(text_value)
                                enriched = {
                                    **text_preview,
                                    "nodeId": str(node_id),
                                    "mediaType": media_type,
                                }
                                if fallback_preview is None:
                                    fallback_preview = enriched
                            continue
                    else:
                        item_dict = cast(Dict[str, Any], item)

                    if preview_output is not None:
                        continue

                    if self._is_previewable(media_type, item_dict):
                        enriched = {
                            **item_dict,
                            "nodeId": str(node_id),
                        }
                        if "mediaType" not in item_dict:
                            enriched["mediaType"] = media_type
                        if item_dict.get("type") == "output":
                            preview_output = enriched
                        elif fallback_preview is None:
                            fallback_preview = enriched

        return count, preview_output or fallback_preview

    def _parse_execution_times(
        self, status_info: Dict[str, Any]
    ) -> Tuple[Optional[float], Optional[float], Optional[Dict[str, Any]]]:
        """从状态消息中解析出执行开始时间、结束时间及错误字典（严格类型契约，包含明确的类型异常警告）。"""
        execution_start_time = None
        execution_end_time = None
        execution_error = None

        messages = status_info.get("messages", [])
        if not isinstance(messages, list):
            raise ValueError(
                f"Unexpected messages format in status_info: expected list, got {type(messages)}"
            )

        for i, entry in enumerate(cast(List[Any], messages)):
            if not isinstance(entry, (list, tuple)):
                raise ValueError(
                    f"Unexpected entry format in status messages at index {i}: expected list/tuple, got {type(entry)}"
                )

            entry_list = cast(List[Any], entry)
            if len(entry_list) < 2:
                raise ValueError(
                    f"Status message entry at index {i} is too short: expected length >= 2, got {len(entry_list)}"
                )

            event_name = entry_list[0]
            event_data = entry_list[1]
            if not isinstance(event_data, dict):
                raise ValueError(
                    f"Unexpected event data format in status message '{event_name}': expected dict, got {type(event_data)}"
                )

            event_data_dict = cast(Dict[str, Any], event_data)
            timestamp = event_data_dict.get("timestamp")

            if event_name == "execution_start":
                execution_start_time = timestamp
            elif event_name in (
                "execution_success",
                "execution_error",
                "execution_interrupted",
            ):
                execution_end_time = timestamp
                if event_name == "execution_error":
                    execution_error = event_data_dict

        return execution_start_time, execution_end_time, execution_error

    def dispose(self) -> None:
        """注销订阅，防止内存泄漏。"""
        self._unsub_dispatch()
        self._unsub_executing()
