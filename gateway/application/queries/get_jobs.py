import json
import logging
from typing import List, Dict, Any, Optional, Set, cast
from gateway.shared.interfaces import TaskQueueReader, DownstreamClient
from gateway.shared.models import Task, TaskStatus

logger = logging.getLogger(__name__)


class GetJobsQueryHandler:
    """整合网关队列与下游 ComfyUI 原生历史 Job 列表的 Query Handler。"""

    def __init__(
        self, queue_reader: TaskQueueReader, downstream_client: DownstreamClient
    ):
        self._queue_reader = queue_reader
        self._downstream_client = downstream_client

    async def handle(self, query_params: Dict[str, str]) -> Dict[str, Any]:
        """合并网关任务与下游 API 的历史任务，支持排序、过滤和分页展示。"""
        downstream_jobs = await self._downstream_client.get_jobs(query_params)

        def make_job_dict(task: Task, status_str: str) -> Dict[str, Any]:
            extra_data = cast(Dict[str, Any], json.loads(task.extra_data))
            extra_pnginfo = cast(Dict[str, Any], extra_data.get("extra_pnginfo", {}))
            workflow = cast(Dict[str, Any], extra_pnginfo.get("workflow", {}))
            workflow_id = workflow.get("id")
            return {
                "id": task.prompt_id,
                "status": status_str,
                "priority": task.number,
                "create_time": task.create_time,
                "outputs_count": 0,
                "workflow_id": workflow_id,
            }

        all_tasks = self._queue_reader.get_tasks()
        running_tasks = [t for s, t in all_tasks if s == TaskStatus.RUNNING]
        pending_tasks = [t for s, t in all_tasks if s == TaskStatus.PENDING]

        gateway_running_jobs = [make_job_dict(t, "in_progress") for t in running_tasks]
        gateway_pending_jobs = [make_job_dict(t, "pending") for t in pending_tasks]

        status_param = query_params.get("status")
        if status_param:
            status_filter = [
                s.strip().lower() for s in status_param.split(",") if s.strip()
            ]
            gateway_running_jobs = [
                j for j in gateway_running_jobs if j["status"] in status_filter
            ]
            gateway_pending_jobs = [
                j for j in gateway_pending_jobs if j["status"] in status_filter
            ]

        workflow_id_param = query_params.get("workflow_id")
        if workflow_id_param:
            gateway_running_jobs = [
                j for j in gateway_running_jobs if j["workflow_id"] == workflow_id_param
            ]
            gateway_pending_jobs = [
                j for j in gateway_pending_jobs if j["workflow_id"] == workflow_id_param
            ]

        seen_ids: Set[str] = set()
        merged_jobs: List[Dict[str, Any]] = []

        for j in gateway_running_jobs + gateway_pending_jobs:
            job_id_val = j.get("id")
            if isinstance(job_id_val, str) and job_id_val not in seen_ids:
                seen_ids.add(job_id_val)
                merged_jobs.append(j)

        for j in downstream_jobs:
            job_id_val = j.get("id")
            if isinstance(job_id_val, str) and job_id_val not in seen_ids:
                seen_ids.add(job_id_val)
                merged_jobs.append(j)

        sort_by = query_params.get("sort_by", "created_at").lower()
        sort_order = query_params.get("sort_order", "desc").lower()

        reverse = sort_order == "desc"
        if sort_by == "execution_duration":

            def get_sort_key(job: Dict[str, Any]) -> float:
                start = job.get("execution_start_time", 0)
                end = job.get("execution_end_time", 0)
                try:
                    return float(end) - float(start) if end and start else 0.0
                except (ValueError, TypeError):
                    return 0.0

        else:

            def get_sort_key(job: Dict[str, Any]) -> float:
                try:
                    return float(job.get("create_time", 0))
                except (ValueError, TypeError):
                    return 0.0

        merged_jobs = sorted(merged_jobs, key=get_sort_key, reverse=reverse)

        total = len(merged_jobs)
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

        if limit_val is not None:
            jobs_page = merged_jobs[offset_val : offset_val + limit_val]
        else:
            jobs_page = merged_jobs[offset_val:]

        has_more = (offset_val + len(jobs_page)) < total

        return {
            "jobs": jobs_page,
            "pagination": {
                "offset": offset_val,
                "limit": limit_val,
                "total": total,
                "has_more": has_more,
            },
        }


class GetJobDetailQueryHandler:
    """获取具体 Job 详情信息的 Query Handler。"""

    def __init__(self, queue_reader: TaskQueueReader):
        self._queue_reader = queue_reader

    async def handle(self, job_id: str) -> Optional[Dict[str, Any]]:
        """从网关拦截的任务队列中查询具体任务详情。"""
        all_tasks = self._queue_reader.get_tasks()
        running_tasks = [t for s, t in all_tasks if s == TaskStatus.RUNNING]
        pending_tasks = [t for s, t in all_tasks if s == TaskStatus.PENDING]

        target_task: Optional[Task] = None
        status_str = None
        for t in running_tasks:
            if t.prompt_id == job_id:
                target_task = t
                status_str = "in_progress"
                break
        if not target_task:
            for t in pending_tasks:
                if t.prompt_id == job_id:
                    target_task = t
                    status_str = "pending"
                    break

        if target_task:
            extra_data = cast(Dict[str, Any], json.loads(target_task.extra_data))
            extra_pnginfo = cast(Dict[str, Any], extra_data.get("extra_pnginfo", {}))
            workflow = cast(Dict[str, Any], extra_pnginfo.get("workflow", {}))
            workflow_id = workflow.get("id")

            return {
                "id": target_task.prompt_id,
                "status": status_str,
                "priority": target_task.number,
                "create_time": target_task.create_time,
                "outputs_count": 0,
                "workflow_id": workflow_id,
                "workflow": {
                    "prompt": target_task.prompt,
                    "extra_data": target_task.extra_data,
                },
            }
        return None
