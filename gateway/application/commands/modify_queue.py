from typing import Optional, List
from gateway.shared.interfaces import TaskQueueWriter
from gateway.domain.gateway import Gateway


class ModifyQueueCommandHandler:
    """清空队列或删除特定待处理任务的 Command Handler。"""

    def __init__(self, queue_writer: TaskQueueWriter, gateway: Gateway):
        self._queue_writer = queue_writer
        self._gateway = gateway

    def handle(
        self, clear: bool = False, delete_ids: Optional[List[str]] = None
    ) -> None:
        """从物理队列中清除或清空任务，并调度同步状态。"""
        if clear:
            self._queue_writer.clear_pending()
        if delete_ids:
            self._queue_writer.delete_pending(delete_ids)

        self._gateway.on_queue_modified()
