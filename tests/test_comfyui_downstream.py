import asyncio
import unittest
from typing import Any, Callable, List, Type, TypeVar
from unittest.mock import AsyncMock, MagicMock

from gateway.config import GatewayConfig
from gateway.infrastructure.comfyui.downstream import ComfyUIDownstreamClient
from gateway.shared.events import DownstreamCrashedEvent, DownstreamReadyChangedEvent
from gateway.shared.interfaces import EventBus

T = TypeVar("T")


class _RecordingEventBus(EventBus):
    """测试用事件总线：同步记录所有发布的事件。"""

    def __init__(self) -> None:
        self.events: List[object] = []

    def subscribe(
        self, event_class: Type[T], callback: Callable[[T], Any]
    ) -> Callable[[], None]:
        return lambda: None

    def publish(self, event: object) -> None:
        self.events.append(event)


class _FakeProcess:
    """模拟已意外退出的下游子进程。"""

    def wait(self) -> int:
        return 1

    def poll(self) -> int:
        return 1


class TestComfyUIDownstreamClientReadiness(unittest.TestCase):
    """测试下游客户端的就绪标志在崩溃与重启各阶段的行为。"""

    def _make_client(
        self, restart_delay_sec: float = 0.05
    ) -> tuple[ComfyUIDownstreamClient, _RecordingEventBus]:
        bus = _RecordingEventBus()
        config = MagicMock(spec=GatewayConfig)
        config.restart_delay_sec = restart_delay_sec
        client = ComfyUIDownstreamClient(
            config=config, event_bus=bus, process_manager=MagicMock()
        )
        return client, bus

    def test_initial_state_is_not_ready(self):
        """初始启动阶段下游必须视为未就绪，API 请求从网关启动起就应进入等待。"""
        client, _ = self._make_client()
        self.assertFalse(client.downstream_ready)

    def test_monitor_marks_unready_before_crash_event_within_delay_window(self):
        """崩溃被检测到的瞬间就应翻转未就绪并先于崩溃事件发布，而非等重启开始。

        空窗期内表现层依赖该标志让 API 请求进入等待；若延迟到重启开始才翻转，
        请求会被转发到已死进程立即失败（issue #7）。
        """

        async def scenario() -> None:
            client, bus = self._make_client()
            # 模拟下游已处于就绪状态后发生非预期崩溃
            client._downstream_ready = True
            client._process = _FakeProcess()

            restart_calls: List[bool] = []

            async def fake_restart() -> None:
                restart_calls.append(True)
                # 结束监控循环，避免测试触发真实进程拉起
                client.exiting = True

            client.restart_downstream = fake_restart  # type: ignore[method-assign]

            await asyncio.wait_for(client.monitor_downstream(), timeout=5)

            # 未就绪事件必须先于崩溃事件发布（崩溃即未就绪）
            ready_events = [
                e
                for e in bus.events
                if isinstance(e, DownstreamReadyChangedEvent) and not e.ready
            ]
            crash_indexes = [
                i for i, e in enumerate(bus.events) if isinstance(e, DownstreamCrashedEvent)
            ]
            self.assertEqual(len(crash_indexes), 1)
            self.assertEqual(len(ready_events), 1)
            self.assertLess(bus.events.index(ready_events[0]), crash_indexes[0])

            # 崩溃周期结束后下游仍应保持未就绪，直到真正重新就绪
            self.assertFalse(client.downstream_ready)
            self.assertEqual(restart_calls, [True])

        asyncio.run(scenario())

    def test_monitor_keeps_ready_flag_untouched_when_no_crash_event_expected(self):
        """监控循环在正常退出（exiting）时不应发布任何状态事件。"""

        async def scenario() -> None:
            client, bus = self._make_client()
            client.exiting = True
            await asyncio.wait_for(client.monitor_downstream(), timeout=5)
            self.assertEqual(bus.events, [])

        asyncio.run(scenario())

    def test_restart_publishes_not_ready_only_on_real_transition(self):
        """重启时仅在真实就绪→未就绪迁移时发布事件；已未就绪时不重复发布。

        崩溃流程中检测阶段已翻转过一次，若重启再次发布会向领域层广播重复迁移。
        """

        async def scenario() -> None:
            # 场景 A：崩溃检测后已未就绪，重启不应重复发布
            client, bus = self._make_client()
            client.start_downstream = MagicMock()  # type: ignore[method-assign]
            client.wait_downstream_ready = AsyncMock()  # type: ignore[method-assign]
            await client.restart_downstream()
            self.assertEqual(
                [e for e in bus.events if isinstance(e, DownstreamReadyChangedEvent)],
                [],
            )
            client.start_downstream.assert_called_once()

            # 场景 B：就绪状态下优雅重启，应发布恰好一次未就绪事件
            client2, bus2 = self._make_client()
            client2._downstream_ready = True
            client2.start_downstream = MagicMock()  # type: ignore[method-assign]
            client2.wait_downstream_ready = AsyncMock()  # type: ignore[method-assign]
            await client2.restart_downstream()
            unready_events = [
                e
                for e in bus2.events
                if isinstance(e, DownstreamReadyChangedEvent) and not e.ready
            ]
            self.assertEqual(len(unready_events), 1)
            self.assertFalse(client2.downstream_ready)

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
