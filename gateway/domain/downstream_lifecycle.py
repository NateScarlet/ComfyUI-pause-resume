import logging
from typing import Dict, FrozenSet, Tuple

from gateway.shared.events import DownstreamStateChanged
from gateway.shared.interfaces import EventBus, JobQueueReader
from gateway.shared.models import DownstreamState, JobFilters, JobStatus, Reason

logger = logging.getLogger(__name__)

# 合法转换表：(当前状态, 目标状态) → 允许的原因集合；未列出的组合一律拒绝。
# SHUTDOWN 只能由活动（WAKEUP）或显式重启回到 STARTING，任何观测都不能直接跳到 READY。
_LEGAL_TRANSITIONS: Dict[Tuple[DownstreamState, DownstreamState], FrozenSet[Reason]] = {
    (DownstreamState.READY, DownstreamState.STARTING): frozenset(
        {Reason.RESTART, Reason.CRASH, Reason.STALL, Reason.READY_POLL}
    ),
    (DownstreamState.READY, DownstreamState.SHUTDOWN): frozenset(
        {Reason.SHUTDOWN, Reason.STOP}
    ),
    (DownstreamState.STARTING, DownstreamState.READY): frozenset({Reason.READY_POLL}),
    (DownstreamState.STARTING, DownstreamState.SHUTDOWN): frozenset(
        {Reason.SHUTDOWN, Reason.STOP}
    ),
    (DownstreamState.SHUTDOWN, DownstreamState.STARTING): frozenset(
        {Reason.WAKEUP, Reason.RESTART}
    ),
}


class DownstreamLifecycle:
    """下游生命周期的单一权威状态，所有改写都必须经 transition seam。"""

    def __init__(
        self,
        initial_state: DownstreamState,
        event_bus: EventBus,
        queue_reader: JobQueueReader,
    ) -> None:
        self._state = initial_state
        self._event_bus = event_bus
        self._queue_reader = queue_reader

    @property
    def state(self) -> DownstreamState:
        """当前权威生命周期状态，对外只读。"""
        return self._state

    def transition(self, state: DownstreamState, reason: Reason) -> bool:
        """尝试迁移到目标状态；合法则改写并发布权威事件，非法则只记录 rejected 日志。

        日志由本 seam 唯一输出，包含原因与队列上下文；被拒绝的调用同样记录，
        以保留「每个改写路径确实被执行过」的可追踪性。
        """
        old_state = self._state
        allowed_reasons = _LEGAL_TRANSITIONS.get((old_state, state), frozenset())
        rejected = reason not in allowed_reasons
        pending = self._queue_reader.count(JobFilters([JobStatus.PENDING]))
        suffix = " rejected=True" if rejected else ""
        logger.info(
            "DownstreamLifecycle: %s → %s%s (reason=%s pending=%d)",
            old_state.value,
            state.value,
            suffix,
            reason.value,
            pending,
        )
        if rejected:
            return False
        self._state = state
        self._event_bus.publish(DownstreamStateChanged(state=state, reason=reason))
        return True
