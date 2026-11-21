from dataclasses import dataclass
from typing import Union

# 下游执行日志翻译器对外产出的信号。所有层都从共享层导入该类型，
# 避免基础设施层之间互相依赖，也不使用 Dict[str, Any] 传递语义。


@dataclass(frozen=True)
class ExecutingTrue:
    """日志出现「收到任务」标记：下游开始执行。"""

    pass


@dataclass(frozen=True)
class ExecutingFalse:
    """日志出现正常收尾标记：下游不再执行。"""

    pass


@dataclass(frozen=True)
class StallWatchStart:
    """日志出现首个执行异常标记：开始等待收尾，需要挂起卡死计时。

    不带 payload：错误摘要由翻译器内部维护，超时后才随 Stall 产出。
    """

    pass


@dataclass(frozen=True)
class StallWatchCancel:
    """撤销挂起的卡死计时。

    当日志观察者随下游进程作废时，Adapter 在丢弃处把这个信号发入自己的
    单一 seam，统一在那里释放该进程持有的计时句柄。
    """

    pass


@dataclass(frozen=True)
class Stall:
    """收尾超时仍未出现正常收尾日志：下游卡死，携带该任务的错误摘要。"""

    summary: str


Signal = Union[
    ExecutingTrue,
    ExecutingFalse,
    StallWatchStart,
    StallWatchCancel,
    Stall,
]
