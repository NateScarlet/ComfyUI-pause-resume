import unittest
from typing import Callable, List, Tuple

from tests.fakes import FakeMemorySampler, FakeTimer
from tests.test_domain_gateway import _make_gateway
from gateway.shared.events import (
    DownstreamCrashedEvent,
    DownstreamReadyChangedEvent,
    DownstreamStateChanged,
)
from gateway.shared.models import DownstreamState, Reason


def _state_events(g) -> List[DownstreamStateChanged]:
    """取出聚合根发布的全部权威生命周期事件。"""
    return [
        ev
        for ev in g._mock_event_bus.published
        if isinstance(ev, DownstreamStateChanged)
    ]


class TestGatewayLifecycleSeam(unittest.TestCase):
    """验证所有生命周期改写入口都经 seam，且各门控读取同一权威状态。"""

    def test_every_entry_point_goes_through_seam(self):
        """重启/停机/唤醒/就绪观测/崩溃/卡死都发布对应的权威状态事件。"""
        cases: List[Tuple[str, Callable[[], object], DownstreamState, Reason, dict]] = [
            (
                "restart",
                lambda g: g.restart_now(),
                DownstreamState.STARTING,
                Reason.RESTART,
                {"downstream_ready": True},
            ),
            (
                "wakeup",
                lambda g: g.wake_downstream(),
                DownstreamState.STARTING,
                Reason.WAKEUP,
                {"downstream_shutdown": True},
            ),
            (
                "ready_poll",
                lambda g: g._mock_event_bus.publish(
                    DownstreamReadyChangedEvent(ready=True)
                ),
                DownstreamState.READY,
                Reason.READY_POLL,
                {"downstream_ready": False},
            ),
            (
                "crash",
                lambda g: g._mock_event_bus.publish(DownstreamCrashedEvent()),
                DownstreamState.STARTING,
                Reason.CRASH,
                {"downstream_ready": True},
            ),
            (
                "stall",
                lambda g: g._mock_event_bus.publish(
                    DownstreamCrashedEvent(error_summary="CUDA error")
                ),
                DownstreamState.STARTING,
                Reason.STALL,
                {"downstream_ready": True},
            ),
        ]
        for name, action, state, reason, kwargs in cases:
            with self.subTest(name=name):
                g = _make_gateway(**kwargs)
                action(g)
                self.assertEqual(
                    _state_events(g), [DownstreamStateChanged(state=state, reason=reason)]
                )
                self.assertEqual(g.downstream_state, state)

    def test_idle_shutdown_goes_through_seam(self):
        """空闲停机入口同样经 seam 改写权威状态。"""
        timer = FakeTimer()
        g = _make_gateway(
            timer=timer,
            memory_sampler=FakeMemorySampler(available_mb=512),
            allow_idle_shutdown_initial=True,
            idle_restart_timeout=600,
            downstream_ready=True,
            ever_active=True,
        )
        timer.advance(600)

        self.assertEqual(
            _state_events(g),
            [
                DownstreamStateChanged(
                    state=DownstreamState.SHUTDOWN, reason=Reason.SHUTDOWN
                )
            ],
        )
        self.assertEqual(g.downstream_state, DownstreamState.SHUTDOWN)

    def test_shutdown_rejects_ready_observation_without_changing_state(self):
        """停机期间到达的 ready=True 观测被拒绝，权威状态保持 SHUTDOWN。"""
        timer = FakeTimer()
        g = _make_gateway(
            timer=timer,
            memory_sampler=FakeMemorySampler(available_mb=512),
            allow_idle_shutdown_initial=True,
            idle_restart_timeout=600,
            downstream_ready=True,
            ever_active=True,
        )
        timer.advance(600)
        before = list(_state_events(g))

        with self.assertLogs("gateway.domain.downstream_lifecycle", level="INFO") as cap:
            g._mock_event_bus.publish(DownstreamReadyChangedEvent(ready=True))

        self.assertEqual(g.downstream_state, DownstreamState.SHUTDOWN)
        self.assertEqual(_state_events(g), before)
        self.assertIn("rejected=", cap.output[0])
        self.assertIn(f"reason={Reason.READY_POLL.value}", cap.output[0])

    def test_gates_share_single_authoritative_state(self):
        """状态呈现、派发门控与休眠判定在同一时刻读到同一个权威状态。"""
        timer = FakeTimer()
        g = _make_gateway(
            timer=timer,
            memory_sampler=FakeMemorySampler(available_mb=512),
            allow_idle_shutdown_initial=True,
            idle_restart_timeout=600,
            downstream_ready=True,
            pending_count=2,
            ever_active=True,
        )

        # READY：派发放行，不因下游未就绪而阻止休眠
        self.assertEqual(g.downstream_state, DownstreamState.READY)
        self.assertIsNotNone(g.get_dispatch_skip())
        g._mock_power.reset_mock()
        g._mock_event_bus.publish(DownstreamReadyChangedEvent(ready=False))

        # STARTING：派发被拦截，且必须阻止休眠以免打断重启
        self.assertEqual(g.downstream_state, DownstreamState.STARTING)
        self.assertIsNone(g.get_dispatch_skip())
        g._mock_power.prevent_sleep.assert_called()

        g._mock_event_bus.publish(DownstreamReadyChangedEvent(ready=True))
        self.assertEqual(g.downstream_state, DownstreamState.READY)
        self.assertIsNotNone(g.get_dispatch_skip())

        # SHUTDOWN：派发被拦截，但进程已被终止，允许系统休眠。
        # 停机要求队列为空，因此用独立的空队列网关验证这一时刻的一致性。
        shutdown_timer = FakeTimer()
        idle_g = _make_gateway(
            timer=shutdown_timer,
            memory_sampler=FakeMemorySampler(available_mb=512),
            allow_idle_shutdown_initial=True,
            idle_restart_timeout=600,
            downstream_ready=True,
            ever_active=True,
        )
        shutdown_timer.advance(600)
        self.assertEqual(idle_g.downstream_state, DownstreamState.SHUTDOWN)
        self.assertIsNone(idle_g.get_dispatch_skip())
        idle_g._mock_power.reset_mock()
        idle_g._refresh()
        idle_g._mock_power.allow_sleep.assert_called()


if __name__ == "__main__":
    unittest.main()
