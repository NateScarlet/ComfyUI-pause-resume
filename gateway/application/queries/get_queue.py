from typing import List
from gateway.shared.interfaces import JobQueueReader
from gateway.shared.models import Job, JobStatus, JobFilters


class GetQueueQueryHandler:
    """获取网关当前真实队列信息的 Query Handler。"""

    def __init__(self, queue_reader: JobQueueReader):
        self._queue_reader = queue_reader

    def handle(self) -> List[Job]:
        """返回原始任务列表，格式转换由表示层负责。"""
        return self._queue_reader.list(
            JobFilters([JobStatus.PENDING, JobStatus.RUNNING])
        )
