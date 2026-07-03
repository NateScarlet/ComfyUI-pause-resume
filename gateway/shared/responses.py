from dataclasses import dataclass


@dataclass(frozen=True)
class AddJobResponse:
    """添加任务的写操作返回结果。"""

    prompt_id: str
    number: float


@dataclass(frozen=True)
class CancelJobResponse:
    """取消任务的写操作返回结果。"""

    cancelled: bool
