from typing import Optional, List
from gateway.shared.interfaces import TaskQueueWriter
from gateway.application.services.downstream import DownstreamAppService


class ModifyQueueCommandHandler:
    """清空队列或删除特定待处理任务的 Command Handler。"""

    def __init__(
        self, queue_writer: TaskQueueWriter, downstream_service: DownstreamAppService
    ):
        self._queue_writer = queue_writer
        self._downstream_service = downstream_service

    def handle(
        self, clear: bool = False, delete_ids: Optional[List[str]] = None
    ) -> None:
        """从物理队列中清除或清空任务，并调度同步状态。"""
        with self._downstream_service.queue_lock:
            if clear:
                self._queue_writer.clear_pending()
            if delete_ids:
                self._queue_writer.delete_pending(delete_ids)

        self._downstream_service.sync_state_to_infrastructure()
        self._downstream_service.notify_status_changed()
        self._downstream_service.try_dispatch()
