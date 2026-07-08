from dataclasses import dataclass
from enum import Enum
from typing import Sequence, List, Any, Optional


class JobStatus(str, Enum):
    """任务在队列中的状态。"""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class RawJSON(str):
    """标记一段已经是合法 JSON 的字符串，在序列化时直接嵌入而无需转义，降低转换开销。

    类似 Go 语言的 json.RawMessage。
    """

    pass


@dataclass
class TimeBucket:
    """时间桶，用于双桶轮换算法。"""

    avg_ms: int  # 平均执行时间（毫秒）
    count: int  # 已记录的任务数量


@dataclass
class EstimationState:
    """预估时间状态，包含双桶数据。"""

    active: TimeBucket  # 当前有效窗口
    staging: TimeBucket  # 新数据收集缓冲区
    # 阶段由 active.count 推导：count < N 为初始阶段，count >= N 为轮换阶段


@dataclass(frozen=True)
class JobSummary:
    """任务队列中任务的轻量级摘要，用于列表页展示。

    去掉了体积庞大的 prompt 和 outputs_to_execute 字段。
    """

    number: float
    prompt_id: str
    status: JobStatus
    workflow_id: Optional[str]
    create_time: int
    extra_data: Optional[RawJSON] = None

    # 物理下游执行反馈字段 (解耦各自独立)
    outputs: Optional[RawJSON] = None
    preview_output: Optional[RawJSON] = None
    execution_start_time: Optional[float] = None
    execution_end_time: Optional[float] = None
    execution_error: Optional[RawJSON] = None


@dataclass(frozen=True)
class Job:
    """任务队列中的任务实体，采用不可变设计，防止并发修改。

    为了提升网关在并发状态下的吞吐性能，prompt 和 extra_data 以 RawJSON 存储。
    """

    number: float
    prompt_id: str
    prompt: RawJSON
    # 注意：extra_data 严禁随意添加网关内部的业务数据，否则会导致客户端在 Requeue 或导入/导出时造成严重污染。有新数据需要持久化时必须新增数据库字段。
    extra_data: RawJSON
    outputs_to_execute: Sequence[str]
    create_time: int
    status: JobStatus = JobStatus.PENDING

    # 物理下游执行反馈字段 (解耦各自独立)
    outputs: Optional[RawJSON] = None
    preview_output: Optional[RawJSON] = None
    execution_start_time: Optional[float] = None
    execution_end_time: Optional[float] = None
    execution_error: Optional[RawJSON] = None

    def to_list(self) -> List[Any]:
        """将当前任务实体序列化为 ComfyUI 原生 /queue 接口期望的 5 项列表格式。"""
        return [
            self.number,
            self.prompt_id,
            self.prompt,
            self.extra_data,
            list(self.outputs_to_execute),
        ]


@dataclass
class JobFilters:
    """队列任务过滤条件，支持底层数据库粗筛和内存细筛。"""

    statuses: Optional[List[JobStatus]] = None
    workflow_id: Optional[str] = None

    def matches(self, job: Job) -> bool:
        """内存细筛：判断一个任务是否真正匹配此过滤器。"""
        if self.statuses is not None and job.status not in self.statuses:
            return False
        if self.workflow_id is not None:
            import json

            try:
                extra_data = json.loads(job.extra_data)
                extra_pnginfo = extra_data.get("extra_pnginfo", {})
                workflow = extra_pnginfo.get("workflow", {})
                w_id = workflow.get("id")
                if w_id != self.workflow_id:
                    return False
            except (json.JSONDecodeError, TypeError, KeyError):
                return False
        return True
