from gateway.shared.interfaces import JobQueueReader
from gateway.shared.models import JobFilters


class GetJobCountQueryHandler:
    """获取符合过滤条件的 Job 总数的 Query Handler。"""

    def __init__(self, queue_reader: JobQueueReader):
        # 显式注入底层的任务队列读取接口
        self._queue_reader = queue_reader

    async def handle(self, filter_by: JobFilters) -> int:
        """从网关拦截的任务队列中查询符合条件的总任务数。"""
        return self._queue_reader.count(filter_by=filter_by)
