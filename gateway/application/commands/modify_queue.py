from typing import Optional, List
from gateway.shared.interfaces import JobQueueWriter, EventBus
from gateway.shared.events import QueueModifiedEvent


class ModifyQueueCommandHandler:
    """清空队列或删除特定待处理任务的 Command Handler。"""

    def __init__(self, queue_writer: JobQueueWriter, event_bus: EventBus):
        self._queue_writer = queue_writer
        self._event_bus = event_bus

    def handle(
        self, clear: bool = False, delete_ids: Optional[List[str]] = None
    ) -> None:
        """从物理队列中清除或清空任务，并调度同步状态。"""
        if clear:
            self._queue_writer.clear_pending()
        if delete_ids:
            self._queue_writer.delete_pending(delete_ids)

        # 发布事件，由网关自行订阅处理
        self._event_bus.publish(QueueModifiedEvent())
