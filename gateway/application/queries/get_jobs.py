import logging
from typing import Dict, Optional, List, Tuple
from gateway.shared.interfaces import TaskQueueReader
from gateway.shared.models import Task, TaskStatus, TaskFilters

logger = logging.getLogger(__name__)


class GetJobsQueryHandler:
    """整合网关队列与本地历史 Job 列表 of Query Handler。"""

    def __init__(self, queue_reader: TaskQueueReader):
        self._queue_reader = queue_reader

    async def handle(
        self, query_params: Dict[str, str]
    ) -> Tuple[List[Tuple[TaskStatus, Task]], int]:
        """从网关本地队列中获取历史任务，支持排序、过滤和分页展示。"""
        valid_statuses = {"pending", "in_progress", "completed", "failed", "cancelled"}
        status_param = query_params.get("status")
        if status_param:
            status_filter = [
                s.strip().lower() for s in status_param.split(",") if s.strip()
            ]
            statuses: List[TaskStatus] = []
            invalid_statuses: List[str] = []
            for sf in status_filter:
                if sf == "pending":
                    statuses.append(TaskStatus.PENDING)
                elif sf == "in_progress":
                    statuses.append(TaskStatus.RUNNING)
                elif sf == "completed":
                    statuses.append(TaskStatus.COMPLETED)
                elif sf == "failed":
                    statuses.append(TaskStatus.FAILED)
                elif sf == "cancelled":
                    statuses.append(TaskStatus.CANCELLED)
                else:
                    invalid_statuses.append(sf)
            if invalid_statuses:
                raise ValueError(
                    f"Invalid status value(s): {', '.join(invalid_statuses)}. "
                    f"Valid values: {', '.join(sorted(valid_statuses))}"
                )
        else:
            statuses = [
                TaskStatus.PENDING,
                TaskStatus.RUNNING,
                TaskStatus.COMPLETED,
                TaskStatus.FAILED,
                TaskStatus.CANCELLED,
            ]

        limit = query_params.get("limit")
        offset = query_params.get("offset")

        limit_val = None
        if limit:
            try:
                limit_val = int(limit)
            except ValueError:
                pass

        offset_val = 0
        if offset:
            try:
                offset_val = int(offset)
            except ValueError:
                pass

        sort_order = query_params.get("sort_order", "desc").lower()
        reverse = sort_order == "desc"

        workflow_id_param = query_params.get("workflow_id")
        filter_by = TaskFilters(statuses=statuses, workflow_id=workflow_id_param)

        filtered_tasks = self._queue_reader.get_tasks(
            filter_by=filter_by,
            limit=limit_val,
            offset=offset_val,
            desc=reverse,
        )
        total = self._queue_reader.get_task_count(filter_by=filter_by)

        return filtered_tasks, total


class GetJobDetailQueryHandler:
    """获取具体 Job 详情信息的 Query Handler。"""

    def __init__(self, queue_reader: TaskQueueReader):
        self._queue_reader = queue_reader

    async def handle(self, job_id: str) -> Optional[Tuple[TaskStatus, Task]]:
        """从网关拦截的任务队列中查询具体任务详情。"""
        return self._queue_reader.get_task_by_id(job_id)
