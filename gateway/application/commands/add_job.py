import uuid
import time
import json
from typing import Optional, Dict, Any
from gateway.shared.models import Job, RawJSON
from gateway.shared.interfaces import JobQueueWriter, EventBus
from gateway.shared.events import QueueModifiedEvent
from gateway.shared.responses import AddJobResponse


class AddJobCommandHandler:
    """处理添加任务写指令的 Handler。"""

    def __init__(self, queue_writer: JobQueueWriter, event_bus: EventBus):
        self._queue_writer = queue_writer
        self._event_bus = event_bus

    def handle(
        self,
        prompt: Dict[str, Any],
        extra_data: Optional[Dict[str, Any]] = None,
        prompt_id: Optional[str] = None,
        number: Optional[float] = None,
        front: bool = False,
    ) -> AddJobResponse:
        """将任务保存至物理队列，并触发下游调度尝试。"""
        if extra_data is None:
            extra_data = {}
        if prompt_id is None:
            prompt_id = str(uuid.uuid4())

        if number is not None:
            job_number = number
        else:
            job_number = float(self._queue_writer.new_number())
            if front:
                job_number = -job_number

        create_time = int(time.time() * 1000)
        job = Job(
            number=job_number,
            prompt_id=prompt_id,
            prompt=RawJSON(json.dumps(prompt, ensure_ascii=False)),
            extra_data=RawJSON(json.dumps(extra_data, ensure_ascii=False)),
            outputs_to_execute=[],
            create_time=create_time,
        )
        self._queue_writer.add(job)

        # 发布事件，由网关自行订阅处理
        self._event_bus.publish(QueueModifiedEvent())

        return AddJobResponse(prompt_id=prompt_id, number=job_number)
