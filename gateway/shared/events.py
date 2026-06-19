from dataclasses import dataclass


@dataclass(frozen=True)
class StateChangedEvent:
    """网关暂停/恢复状态发生变更的事件。"""

    paused: bool


@dataclass(frozen=True)
class StatusChangedEvent:
    """网关队列待处理或正在运行任务数量等状态发生变更的事件。"""

    pass


@dataclass(frozen=True)
class DownstreamExecutingChangedEvent:
    """下游 ComfyUI 执行繁忙状态发生变化的物理事件。"""

    executing: bool


@dataclass(frozen=True)
class DownstreamReadyChangedEvent:
    """下游 ComfyUI 启动就绪状态发生变化的物理事件。"""

    ready: bool


@dataclass(frozen=True)
class DownstreamCrashedEvent:
    """下游 ComfyUI 物理进程非预期崩溃的事件。"""

    pass


@dataclass(frozen=True)
class QueueModifiedEvent:
    """网关待处理队列的任务被添加或修改（删除/清空）时发布的事件。"""

    pass


@dataclass(frozen=True)
class DispatchSuccessEvent:
    """任务成功派发至下游 ComfyUI 的事件。"""

    pass


@dataclass(frozen=True)
class DispatchFailedEvent:
    """任务派发至下游 ComfyUI 失败的事件。"""

    is_permanent: bool


@dataclass(frozen=True)
class ScriptStateChangedEvent:
    """外挂脚本运行状态（启动/停止/退出）发生变更的事件。"""

    pass
