from dataclasses import dataclass
from enum import Enum
from typing import Sequence, List, Any


class TaskStatus(str, Enum):
    """任务在队列中的状态。"""

    PENDING = "pending"
    RUNNING = "running"


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
class Task:
    """网关任务队列中的任务实体，采用不可变设计，防止并发修改。

    为了提升网关在并发状态下的吞吐性能，prompt 和 extra_data 以 RawJSON 存储。
    """

    number: float
    prompt_id: str
    prompt: RawJSON
    extra_data: RawJSON
    outputs_to_execute: Sequence[str]
    create_time: int

    def to_list(self) -> List[Any]:
        """将当前任务实体序列化为 ComfyUI 原生 /queue 接口期望的 5 项列表格式。"""
        return [
            self.number,
            self.prompt_id,
            self.prompt,
            self.extra_data,
            list(self.outputs_to_execute),
        ]
