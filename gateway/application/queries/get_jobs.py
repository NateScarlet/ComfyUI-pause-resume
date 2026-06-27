from typing import List, Optional
from gateway.shared.interfaces import TaskQueueReader
from gateway.shared.models import TaskFilters, TaskSummary


class GetJobsQueryHandler:
    """整合网关队列与本地历史 Job 列表的 Query Handler。"""

    def __init__(self, queue_reader: TaskQueueReader):
        # 显式注入底层的任务队列读取接口
        self._queue_reader = queue_reader

    async def handle(
        self,
        filter_by: TaskFilters,
        limit: Optional[int] = None,
        offset: int = 0,
        desc: bool = True,
    ) -> List[TaskSummary]:
        """从网关本地队列中获取历史任务，支持排序、过滤和分页展示。"""
        return self._queue_reader.get_task_summaries(
            filter_by=filter_by,
            limit=limit,
            offset=offset,
            desc=desc,
        )
