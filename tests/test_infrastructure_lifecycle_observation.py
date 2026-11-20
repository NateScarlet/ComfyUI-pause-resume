import asyncio
import unittest
from unittest.mock import MagicMock

from gateway.config import GatewayConfig
from gateway.infrastructure.comfyui.downstream import ComfyUIDownstreamClient
from gateway.shared.events import DownstreamReadyChangedEvent
from gateway.shared.interfaces import DownstreamClient
from tests.fakes import FakeTimer
from tests.test_comfyui_downstream import _RecordingEventBus


class TestInfrastructureLifecycleObservation(unittest.TestCase):
    """基础设施只发布物理观测事件，不再暴露业务就绪属性或第二套就绪日志。"""

    _LOGGER = "gateway.infrastructure.comfyui.downstream"

    def setUp(self) -> None:
        self.loop = asyncio.new_event_loop()
        self.addCleanup(self.loop.close)

    def _make_client(self):
        bus = _RecordingEventBus()
        config = MagicMock(spec=GatewayConfig)
        config.restart_delay_sec = 0.05
        config.stall_timeout_sec = 1800.0
        client = ComfyUIDownstreamClient(
            config=config,
            event_bus=bus,
            process_manager=MagicMock(),
            timer=FakeTimer(),
            loop=self.loop,
        )
        return client, bus

    def test_downstream_ready_is_not_part_of_client_contract(self):
        """DownstreamClient 抽象与具体实现都不再暴露 downstream_ready。"""
        self.assertNotIn("downstream_ready", DownstreamClient.__abstractmethods__)
        self.assertFalse(hasattr(ComfyUIDownstreamClient, "downstream_ready"))

    def test_unready_observation_is_event_transition_not_business_log(self):
        """物理未就绪观测只在真实迁移时发布一次事件，不输出第二套就绪日志。"""
        client, bus = self._make_client()
        client._downstream_ready = True

        with self.assertNoLogs(self._LOGGER, level="INFO"):
            client._mark_downstream_unready()
            client._mark_downstream_unready()

        ready_events = [
            ev for ev in bus.events if isinstance(ev, DownstreamReadyChangedEvent)
        ]
        self.assertEqual(ready_events, [DownstreamReadyChangedEvent(ready=False)])
        self.assertFalse(client._downstream_ready)


if __name__ == "__main__":
    unittest.main()
