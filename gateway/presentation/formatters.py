import json
from typing import Dict, Any, cast

from gateway.shared.models import Job, JobStatus, JobSummary
from gateway.shared.outputs_parser import parse_outputs_count, extract_workflow_id


def format_job_status(status: JobStatus) -> str:
    """将领域状态枚举转换为 API 状态字符串。"""
    return "in_progress" if status == JobStatus.RUNNING else status.value


def format_job_summary(summary: JobSummary) -> Dict[str, Any]:
    """将 JobSummary 转换为 API 规范的 job 字典。"""
    outputs_count = 0
    if summary.outputs:
        outputs = json.loads(summary.outputs)
        if isinstance(outputs, dict):
            outputs_count = parse_outputs_count(cast(Dict[str, Any], outputs))

    preview_output = None
    if summary.preview_output:
        preview_output = json.loads(summary.preview_output)

    execution_error = None
    if summary.execution_error:
        execution_error = json.loads(summary.execution_error)

    res: Dict[str, Any] = {
        "id": summary.prompt_id,
        "status": format_job_status(summary.status),
        "priority": summary.number,
        "create_time": summary.create_time,
        "outputs_count": outputs_count,
        "workflow_id": summary.workflow_id,
    }
    if preview_output is not None:
        res["preview_output"] = preview_output
    if summary.execution_start_time is not None:
        res["execution_start_time"] = summary.execution_start_time
    if summary.execution_end_time is not None:
        res["execution_end_time"] = summary.execution_end_time
    if execution_error is not None:
        res["execution_error"] = execution_error
    return res


def format_job_detail(job: Job) -> Dict[str, Any]:
    """将 Job 转换为 API 规范的 job_detail 字典。"""
    workflow_id = extract_workflow_id(job.extra_data) if job.extra_data else None

    outputs_val = None
    outputs_count = 0
    if job.outputs:
        outputs_val = json.loads(job.outputs)
        if isinstance(outputs_val, dict):
            outputs_count = parse_outputs_count(cast(Dict[str, Any], outputs_val))

    preview_output_val = None
    if job.preview_output:
        preview_output_val = json.loads(job.preview_output)

    execution_error_val = None
    if job.execution_error:
        execution_error_val = json.loads(job.execution_error)

    job_detail: Dict[str, Any] = {
        "id": job.prompt_id,
        "status": format_job_status(job.status),
        "priority": job.number,
        "create_time": job.create_time,
        "outputs_count": outputs_count,
        "workflow_id": workflow_id,
        "workflow": {
            "prompt": job.prompt,
            "extra_data": job.extra_data,
        },
    }

    if outputs_val is not None:
        job_detail["outputs"] = outputs_val
    if preview_output_val is not None:
        job_detail["preview_output"] = preview_output_val
    if job.execution_start_time is not None:
        job_detail["execution_start_time"] = job.execution_start_time
    if job.execution_end_time is not None:
        job_detail["execution_end_time"] = job.execution_end_time
    if execution_error_val is not None:
        job_detail["execution_error"] = execution_error_val
    return job_detail
