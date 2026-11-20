import unittest
from typing import List, Tuple
from unittest.mock import MagicMock

from gateway.domain.downstream_lifecycle import DownstreamLifecycle
from gateway.shared.events import DownstreamStateChanged
from gateway.shared.interfaces import EventBus, JobQueueReader
from gateway.shared.models import DownstreamState, Reason


class _RecordingEventBus(EventBus):
    """记录所有已发布事件的事件总线替身。"""

    def __init__(self) -> None:
        self.events: List[object] = []

    def subscribe(self, event_class, callback):
        return lambda: None

    def publish(self, event: object) -> None:
        self.events.append(event)


class TestDownstreamLifecycleTransition(unittest.TestCase):
    """表驱动验证生命周期 seam 的合法转换、拒绝转换与日志契约。"""

    _LOGGER = "gateway.domain.downstream_lifecycle"

    def _make(self, initial: DownstreamState):
        bus = _RecordingEventBus()
        reader = MagicMock(spec=JobQueueReader)
        reader.count.return_value = 3
        return DownstreamLifecycle(initial, event_bus=bus, queue_reader=reader), bus

    def test_legal_transitions_update_state_and_publish_event(self):
        """合法转换必须改写权威状态并发布带原因的权威事件。"""
        cases: List[Tuple[DownstreamState, DownstreamState, Reason]] = [
            (DownstreamState.READY, DownstreamState.STARTING, Reason.RESTART),
            (DownstreamState.READY, DownstreamState.STARTING, Reason.CRASH),
            (DownstreamState.READY, DownstreamState.STARTING, Reason.STALL),
            (DownstreamState.READY, DownstreamState.STARTING, Reason.READY_POLL),
            (DownstreamState.READY, DownstreamState.SHUTDOWN, Reason.SHUTDOWN),
            (DownstreamState.READY, DownstreamState.SHUTDOWN, Reason.STOP),
            (DownstreamState.STARTING, DownstreamState.READY, Reason.READY_POLL),
            (DownstreamState.STARTING, DownstreamState.SHUTDOWN, Reason.SHUTDOWN),
            (DownstreamState.STARTING, DownstreamState.SHUTDOWN, Reason.STOP),
            (DownstreamState.SHUTDOWN, DownstreamState.STARTING, Reason.WAKEUP),
            (DownstreamState.SHUTDOWN, DownstreamState.STARTING, Reason.RESTART),
        ]
        for initial, target, reason in cases:
            with self.subTest(initial=initial, target=target, reason=reason):
                lifecycle, bus = self._make(initial)
                with self.assertLogs(self._LOGGER, level="INFO") as captured:
                    changed = lifecycle.transition(target, reason)

                self.assertTrue(changed)
                self.assertEqual(lifecycle.state, target)
                self.assertEqual(
                    bus.events, [DownstreamStateChanged(state=target, reason=reason)]
                )
                self.assertEqual(len(captured.output), 1)
                self.assertIn(f"reason={reason.value}", captured.output[0])
                self.assertIn("pending=3", captured.output[0])
                self.assertNotIn("rejected=", captured.output[0])

    def test_illegal_transitions_are_rejected_without_changing_state(self):
        """非法或陈旧转换只能记录 rejected 日志，不得改状态或发布权威事件。"""
        cases: List[Tuple[DownstreamState, DownstreamState, Reason]] = [
            # 核心回归：停机期间到达的就绪观测必须被拒绝
            (DownstreamState.SHUTDOWN, DownstreamState.READY, Reason.READY_POLL),
            (DownstreamState.SHUTDOWN, DownstreamState.STARTING, Reason.READY_POLL),
            (DownstreamState.READY, DownstreamState.READY, Reason.READY_POLL),
            (DownstreamState.STARTING, DownstreamState.STARTING, Reason.CRASH),
            (DownstreamState.SHUTDOWN, DownstreamState.SHUTDOWN, Reason.SHUTDOWN),
            (DownstreamState.READY, DownstreamState.STARTING, Reason.WAKEUP),
        ]
        for initial, target, reason in cases:
            with self.subTest(initial=initial, target=target, reason=reason):
                lifecycle, bus = self._make(initial)
                with self.assertLogs(self._LOGGER, level="INFO") as captured:
                    changed = lifecycle.transition(target, reason)

                self.assertFalse(changed)
                self.assertEqual(lifecycle.state, initial)
                self.assertEqual(bus.events, [])
                self.assertEqual(len(captured.output), 1)
                self.assertIn("rejected=", captured.output[0])
                self.assertIn(f"reason={reason.value}", captured.output[0])
                self.assertIn("pending=3", captured.output[0])

    def test_multiple_transitions_keep_single_authoritative_state(self):
        """连续转换后只保留最后一个权威状态，每成功一次各发布一条事件。"""
        lifecycle, bus = self._make(DownstreamState.STARTING)

        lifecycle.transition(DownstreamState.READY, Reason.READY_POLL)
        lifecycle.transition(DownstreamState.STARTING, Reason.RESTART)
        lifecycle.transition(DownstreamState.SHUTDOWN, Reason.SHUTDOWN)

        self.assertEqual(lifecycle.state, DownstreamState.SHUTDOWN)
        self.assertEqual(
            bus.events,
            [
                DownstreamStateChanged(
                    state=DownstreamState.READY, reason=Reason.READY_POLL
                ),
                DownstreamStateChanged(
                    state=DownstreamState.STARTING, reason=Reason.RESTART
                ),
                DownstreamStateChanged(
                    state=DownstreamState.SHUTDOWN, reason=Reason.SHUTDOWN
                ),
            ],
        )


if __name__ == "__main__":
    unittest.main()
