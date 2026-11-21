import asyncio
import json
import unittest
from unittest.mock import MagicMock

from gateway.presentation.status_fanout import ClientStatus, StatusFanout
from gateway.shared.events import (
    DownstreamStateChanged,
    StateChangedEvent,
    StatusChangedEvent,
)
from gateway.shared.models import (
    DownstreamState,
    GatewayState,
    JobFilters,
    JobStatus,
    Reason,
)
from tests.fakes import FakeTimer
from tests.test_gateway_handlers import _SyncEventBus

_ACTIVE_FILTER = JobFilters([JobStatus.PENDING, JobStatus.RUNNING])


class _QueueReader:
    """只实现 fanout 所需 count 的替身，记录每次查询的筛选条件。"""

    def __init__(self, count: int) -> None:
        self.value = count
        self.filters = []

    def count(self, filter_by=None, limit=None) -> int:
        self.filters.append(filter_by)
        return self.value


class _RecordingWs:
    """记录状态帧的 WS 替身。"""

    def __init__(self) -> None:
        self.closed = False
        self.sent_text = []

    async def send_str(self, data: str) -> None:
        self.sent_text.append(json.loads(data))

    async def close(self, code=None, message=b"") -> None:
        self.closed = True


class TestStatusFanout(unittest.TestCase):
    """以 StatusFanout 公共接口为 seam，验证一份快照驱动多个客户端通道。"""

    def _make_fanout(self, paused=False, count=0, downstream=DownstreamState.READY):
        app = MagicMock()
        app.get_state.handle = MagicMock(
            return_value=GatewayState(
                paused=paused,
                allow_idle_shutdown=False,
                downstream_state=downstream,
            )
        )
        reader = _QueueReader(count)
        bus = _SyncEventBus()
        timer = FakeTimer()
        fanout = StatusFanout(app, reader, bus, timer, asyncio.get_running_loop())
        return fanout, reader, bus, timer

    async def _drain(self) -> None:
        """让 call_soon_threadsafe 回调与它创建的多层任务都得到执行。"""
        for _ in range(4):
            await asyncio.sleep(0)

    def test_initial_snapshot_uses_authoritative_state_and_active_count(self):
        """建连初始快照来自 get_state 与 PENDING+RUNNING 实时计数。"""

        async def scenario() -> None:
            fanout, reader, _, _ = self._make_fanout(
                paused=True, count=4, downstream=DownstreamState.STARTING
            )
            q, status = fanout.connect_sse()
            self.assertEqual(
                status,
                ClientStatus(True, 4, DownstreamState.STARTING),
            )
            self.assertEqual(reader.filters, [_ACTIVE_FILTER])
            fanout.disconnect_sse(q)

        asyncio.run(scenario())

    def test_one_status_event_drives_sse_and_ws_channels(self):
        """一次队列变化同时驱动 SSE 帧与 WS 帧，两者字段来自同一快照。"""

        async def scenario() -> None:
            fanout, reader, bus, timer = self._make_fanout(count=2)
            q, _ = fanout.connect_sse()
            ws = _RecordingWs()
            fanout.add_ws_client(ws)

            bus.publish(StatusChangedEvent(queue_remaining=7))
            await self._drain()
            timer.advance(0.1)
            await self._drain()

            self.assertEqual(
                json.loads(q.get_nowait()),
                {"paused": False, "queue_remaining": 7, "downstream": "ready"},
            )
            self.assertEqual(
                ws.sent_text,
                [
                    {
                        "type": "status",
                        "data": {
                            "status": {"exec_info": {"queue_remaining": 7}}
                        },
                    }
                ],
            )
            # 事件本身不触发额外计数查询
            self.assertEqual(reader.filters, [_ACTIVE_FILTER])

        asyncio.run(scenario())

    def test_pause_and_downstream_events_preserve_queue_snapshot(self):
        """暂停/下游事件只改动自己的字段，队列计数沿用快照而不重新查询。"""

        async def scenario() -> None:
            fanout, reader, bus, _ = self._make_fanout(count=3)
            q, _ = fanout.connect_sse()

            bus.publish(StateChangedEvent(paused=True))
            await self._drain()
            bus.publish(
                DownstreamStateChanged(
                    state=DownstreamState.STARTING, reason=Reason.RESTART
                )
            )
            await self._drain()

            self.assertEqual(
                json.loads(q.get_nowait()),
                {"paused": True, "queue_remaining": 3, "downstream": "ready"},
            )
            self.assertEqual(
                json.loads(q.get_nowait()),
                {"paused": True, "queue_remaining": 3, "downstream": "starting"},
            )
            self.assertEqual(reader.filters, [_ACTIVE_FILTER])

        asyncio.run(scenario())

    def test_ws_debounce_coalesces_events_with_fake_timer(self):
        """FakeTimer 推进前多次变化合并为一帧，且不需要真实 sleep。"""

        async def scenario() -> None:
            fanout, _, bus, timer = self._make_fanout(count=0)
            fanout.connect_sse()
            ws = _RecordingWs()
            fanout.add_ws_client(ws)

            for remaining in (1, 2, 3):
                bus.publish(StatusChangedEvent(queue_remaining=remaining))
            await self._drain()
            self.assertEqual(timer.pending_count, 1)

            timer.advance(0.1)
            await self._drain()
            self.assertEqual(len(ws.sent_text), 1)
            self.assertEqual(
                ws.sent_text[0]["data"]["status"]["exec_info"]["queue_remaining"], 3
            )

        asyncio.run(scenario())

    def test_ws_rewrite_reads_realtime_count_not_snapshot(self):
        """WS 改写取实时计数，与事件快照值互不混淆。"""

        async def scenario() -> None:
            fanout, reader, bus, timer = self._make_fanout(count=2)
            q, _ = fanout.connect_sse()
            ws = _RecordingWs()
            fanout.add_ws_client(ws)

            reader.value = 9
            self.assertEqual(fanout.get_queue_remaining(), 9)

            bus.publish(StatusChangedEvent(queue_remaining=2))
            await self._drain()
            timer.advance(0.1)
            await self._drain()
            # WS 广播仍来自快照，而非改写时的实时查询
            self.assertEqual(
                ws.sent_text[0]["data"]["status"]["exec_info"]["queue_remaining"], 2
            )
            self.assertEqual(reader.filters, [_ACTIVE_FILTER, _ACTIVE_FILTER])
            q.get_nowait()

        asyncio.run(scenario())

    def test_dispose_cancels_broadcast_task_that_already_started(self):
        """定时器已触发但广播尚未执行时释放，也不得再向 WS 发送帧。"""

        async def scenario() -> None:
            fanout, _, bus, timer = self._make_fanout(count=0)
            fanout.connect_sse()
            ws = _RecordingWs()
            fanout.add_ws_client(ws)

            bus.publish(StatusChangedEvent(queue_remaining=5))
            await self._drain()
            timer.advance(0.1)  # 广播任务已创建，但还没得到执行机会
            fanout.dispose()
            await self._drain()

            self.assertEqual(ws.sent_text, [])
            current = asyncio.current_task()
            pending = [
                t for t in asyncio.all_tasks() if t is not current and not t.done()
            ]
            self.assertEqual(pending, [])

        asyncio.run(scenario())

    def test_dispose_cancels_subscriptions_and_timer(self):
        """释放后事件不再入队、定时器被取消，且无残留任务。"""

        async def scenario() -> None:
            fanout, _, bus, timer = self._make_fanout(count=0)
            q, _ = fanout.connect_sse()
            ws = _RecordingWs()
            fanout.add_ws_client(ws)

            bus.publish(StatusChangedEvent(queue_remaining=5))
            await self._drain()
            self.assertEqual(timer.pending_count, 1)

            fanout.dispose()
            await self._drain()
            self.assertEqual(timer.pending_count, 0)
            # 先前事件帧之后应收到关闭哨兵
            self.assertEqual(json.loads(q.get_nowait())["queue_remaining"], 5)
            self.assertEqual(q.get_nowait(), "shutdown")

            bus.publish(StatusChangedEvent(queue_remaining=6))
            await self._drain()
            self.assertTrue(q.empty())
            self.assertTrue(ws.closed)

            current = asyncio.current_task()
            pending = [
                t for t in asyncio.all_tasks() if t is not current and not t.done()
            ]
            self.assertEqual(pending, [])

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
