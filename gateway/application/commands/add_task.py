import uuid
import time
import json
from typing import Optional, Dict, Any
from gateway.shared.models import Task, RawJSON
from gateway.shared.interfaces import TaskQueueWriter
from gateway.application.services.downstream import DownstreamAppService


class AddTaskCommandHandler:
    """处理添加任务写指令的 Handler。"""

    def __init__(
        self, queue_writer: TaskQueueWriter, downstream_service: DownstreamAppService
    ):
        self._queue_writer = queue_writer
        self._downstream_service = downstream_service

    def handle(
        self,
        prompt: Dict[str, Any],
        extra_data: Optional[Dict[str, Any]] = None,
        prompt_id: Optional[str] = None,
        number: Optional[float] = None,
        front: bool = False,
    ) -> Dict[str, Any]:
        """将任务保存至物理队列，并触发下游调度尝试。"""
        if extra_data is None:
            extra_data = {}
        if prompt_id is None:
            prompt_id = str(uuid.uuid4())

        with self._downstream_service.queue_lock:
            if number is not None:
                task_number = number
            else:
                task_number = float(self._queue_writer.new_task_number())
                if front:
                    task_number = -task_number

            create_time = int(time.time() * 1000)
            task = Task(
                number=task_number,
                prompt_id=prompt_id,
                prompt=RawJSON(json.dumps(prompt, ensure_ascii=False)),
                extra_data=RawJSON(json.dumps(extra_data, ensure_ascii=False)),
                outputs_to_execute=[],
                create_time=create_time,
            )
            self._queue_writer.add_task(task)

        # 触发基础设施状态同步、WebSocket 广播和尝试分发
        self._downstream_service.sync_state_to_infrastructure()
        self._downstream_service.notify_status_changed()
        self._downstream_service.try_dispatch()

        return {"prompt_id": prompt_id, "number": task_number}
