from typing import Optional
from gateway.shared.interfaces import JobQueueReader
from gateway.shared.models import Job


class GetJobDetailQueryHandler:
    """获取具体 Job 详情信息的 Query Handler。"""

    def __init__(self, queue_reader: JobQueueReader):
        # 显式注入底层的任务队列读取接口
        self._queue_reader = queue_reader

    async def handle(self, job_id: str) -> Optional[Job]:
        """从网关拦截的任务队列中查询具体任务详情。"""
        return self._queue_reader.get(job_id)
