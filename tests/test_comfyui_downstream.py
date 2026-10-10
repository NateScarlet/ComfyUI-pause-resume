import asyncio
import time
import unittest
from typing import Any, Callable, List, Optional, Type, TypeVar
from unittest.mock import AsyncMock, MagicMock

from gateway.config import GatewayConfig
from gateway.infrastructure.comfyui.downstream import ComfyUIDownstreamClient
from gateway.infrastructure.comfyui.execution_log import FAILURE_MARKER
from gateway.shared.events import DownstreamCrashedEvent, DownstreamReadyChangedEvent
from gateway.shared.exceptions import DownstreamStartupTimeout
from gateway.shared.interfaces import EventBus
from tests.fakes import FakeTimer

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
    """模拟下游子进程：可被终止并记录终止调用。"""

    def __init__(self) -> None:
        self.killed = False

    def wait(self) -> int:
        return 1

    def poll(self) -> int:
        return 1

    def kill(self) -> None:
        self.killed = True


class TestComfyUIDownstreamClientReadiness(unittest.TestCase):
    """测试下游客户端的就绪标志在崩溃与重启各阶段的行为。"""

    def setUp(self) -> None:
        self.timer = FakeTimer()
        # 客户端构造需要一个事件循环；默认用未运行的新循环，仅重启入口关心它是否在运行
        self.loop = asyncio.new_event_loop()
        self.addCleanup(self.loop.close)

    def _make_client(
        self,
        restart_delay_sec: float = 0.05,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> tuple[ComfyUIDownstreamClient, _RecordingEventBus]:
        bus = _RecordingEventBus()
        config = MagicMock(spec=GatewayConfig)
        config.restart_delay_sec = restart_delay_sec
        config.stall_timeout_sec = 1800.0
        client = ComfyUIDownstreamClient(
            config=config,
            event_bus=bus,
            process_manager=MagicMock(),
            timer=self.timer,
            loop=loop if loop is not None else self.loop,
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

    def test_restart_retries_until_ready_after_wait_timeout(self):
        """就绪等待超时后应终止半启动进程并按延迟重试拉起，直到某次尝试成功。

        启动超时不得从重启流程向外传播，否则会杀死依赖它的监控协程与
        优雅重启任务，使下游永久卡在未就绪（issue #10）。
        """

        async def scenario() -> None:
            client, _ = self._make_client(restart_delay_sec=0.01)
            old_process = _FakeProcess()
            client._process = old_process
            client._downstream_ready = True  # 模拟重启前下游处于就绪状态

            spawned: List[_FakeProcess] = []

            def fake_start_downstream() -> None:
                # 模拟真实契约：拉起新进程并由客户端持有
                p = _FakeProcess()
                spawned.append(p)
                client._process = p

            wait_calls: List[bool] = []
            ready_during_failures: List[bool] = []

            async def fake_wait() -> None:
                wait_calls.append(True)
                if len(wait_calls) <= 2:
                    ready_during_failures.append(client.downstream_ready)
                    raise DownstreamStartupTimeout("Downstream wait timeout")
                # 第三次成功：模拟真实契约——翻转就绪标志
                client._downstream_ready = True

            client.start_downstream = fake_start_downstream  # type: ignore[method-assign]
            client.wait_downstream_ready = fake_wait  # type: ignore[method-assign]

            t0 = time.perf_counter()
            await asyncio.wait_for(client.restart_downstream(), timeout=5)
            elapsed = time.perf_counter() - t0

            # 前两次尝试超时后各重试一次，第三次成功：共拉起 3 个候选进程
            self.assertEqual(len(spawned), 3)
            self.assertEqual(len(wait_calls), 3)
            # 每个失败候选进程（含重启前持有的旧进程）都应在下次尝试前被终止
            self.assertTrue(old_process.killed)
            self.assertTrue(spawned[0].killed)
            self.assertTrue(spawned[1].killed)
            self.assertFalse(spawned[2].killed)
            # 失败重试期间必须保持未就绪，成功后才翻转
            self.assertEqual(ready_during_failures, [False, False])
            self.assertTrue(client.downstream_ready)
            # 两次失败重试之间应遵守配置的重试延迟下限
            self.assertGreaterEqual(elapsed, 2 * 0.01)

        asyncio.run(scenario())

    def test_restart_stops_spawning_after_shutdown_signal(self):
        """退出标志置位后重启流程应立即停止重试，不再拉起新的候选进程。"""

        async def scenario() -> None:
            client, _ = self._make_client(restart_delay_sec=0.01)
            client._process = _FakeProcess()

            spawn_count = 0

            def fake_start_downstream() -> None:
                nonlocal spawn_count
                spawn_count += 1
                client._process = _FakeProcess()

            async def fake_wait() -> None:
                # 模拟就绪等待期间收到网关退出信号：本次尝试失败且不再继续
                client.exiting = True
                raise DownstreamStartupTimeout("Downstream wait timeout")

            client.start_downstream = fake_start_downstream  # type: ignore[method-assign]
            client.wait_downstream_ready = fake_wait  # type: ignore[method-assign]

            await asyncio.wait_for(client.restart_downstream(), timeout=5)

            # 首次拉起后即因退出信号停止，不得进入下一轮拉起
            self.assertEqual(spawn_count, 1)

        asyncio.run(scenario())

    def test_restart_stops_before_next_attempt_when_exiting_during_backoff(self):
        """退避等待期间收到退出信号时，应在下一轮尝试前停止，不再拉起新的候选进程。"""

        async def scenario() -> None:
            client, _ = self._make_client(restart_delay_sec=0.05)

            spawn_count = 0

            def fake_start_downstream() -> None:
                nonlocal spawn_count
                spawn_count += 1
                client._process = _FakeProcess()

            async def fake_wait() -> None:
                # 退出信号在本轮失败的退避睡眠窗口内到达（0.02s < 重试延迟 0.05s）
                async def set_exit_later() -> None:
                    await asyncio.sleep(0.02)
                    client.exiting = True

                asyncio.create_task(set_exit_later())
                raise DownstreamStartupTimeout("Downstream wait timeout")

            client.start_downstream = fake_start_downstream  # type: ignore[method-assign]
            client.wait_downstream_ready = fake_wait  # type: ignore[method-assign]

            await asyncio.wait_for(client.restart_downstream(), timeout=5)

            self.assertEqual(spawn_count, 1)

        asyncio.run(scenario())

    def test_monitor_survives_restart_attempts_that_time_out(self):
        """崩溃触发的重启经历多次就绪超时后最终成功时，监控协程必须存活完成整个周期。

        回归钉：此前启动超时异常会传播出监控协程并将其永久杀死，
        下游从此失去崩溃检测与自愈能力（issue #10）。
        """

        async def scenario() -> None:
            client, bus = self._make_client(restart_delay_sec=0.01)
            client._downstream_ready = True
            client._process = _FakeProcess()

            spawn_count = 0

            def fake_start_downstream() -> None:
                nonlocal spawn_count
                spawn_count += 1
                client._process = _FakeProcess()

            wait_calls = 0

            async def fake_wait() -> None:
                nonlocal wait_calls
                wait_calls += 1
                if wait_calls <= 2:
                    raise DownstreamStartupTimeout("Downstream wait timeout")
                client._downstream_ready = True
                # 恢复就绪后立即置退出信号，让监控循环自然结束
                client.exiting = True

            client.start_downstream = fake_start_downstream  # type: ignore[method-assign]
            client.wait_downstream_ready = fake_wait  # type: ignore[method-assign]

            # 此前该调用会因 DownstreamStartupTimeout 传播而以异常告终
            await asyncio.wait_for(client.monitor_downstream(), timeout=5)

            crash_events = [
                e for e in bus.events if isinstance(e, DownstreamCrashedEvent)
            ]
            self.assertEqual(len(crash_events), 1)
            self.assertEqual(spawn_count, 3)
            self.assertTrue(client.downstream_ready)

        asyncio.run(scenario())

    def test_stall_marks_unready_reports_crash_and_restarts(self):
        """卡死判定后按崩溃语义处置：立即置未就绪、发布崩溃事件并重启下游。

        卡死时下游进程仍然存活，进程退出监控察觉不到；且工作线程已死，
        不重启下游就会永久失去消费队列的能力（issue #14）。
        """

        async def scenario() -> None:
            client, bus = self._make_client(loop=asyncio.get_running_loop())
            client._downstream_ready = True

            restart_calls: List[bool] = []

            async def fake_restart() -> None:
                restart_calls.append(True)

            client.restart_downstream = fake_restart  # type: ignore[method-assign]

            client._on_downstream_stall("CUDA error: out of memory")
            await asyncio.sleep(0.01)

            self.assertFalse(client.downstream_ready)
            crash_events = [
                e for e in bus.events if isinstance(e, DownstreamCrashedEvent)
            ]
            self.assertEqual(len(crash_events), 1)
            self.assertEqual(crash_events[0].error_summary, "CUDA error: out of memory")
            self.assertEqual(restart_calls, [True])

        asyncio.run(scenario())

    def _feed_failure_line(self, client: ComfyUIDownstreamClient) -> None:
        """按真实路径把一行执行异常日志投递下去：后台读取线程 -> 事件循环 -> 观察者。"""
        assert client._log_watcher is not None
        client._feed_log_line(
            client._log_watcher, f"[ERROR] {FAILURE_MARKER} CUDA error: out of memory"
        )

    def test_terminating_process_drops_pending_stall_verdict(self):
        """老进程被终止时挂起的卡死判定必须丢弃，不得在新进程上误判。"""

        async def scenario() -> None:
            client, bus = self._make_client(loop=asyncio.get_running_loop())
            client._process = _FakeProcess()
            watcher = client._create_log_watcher()

            self._feed_failure_line(client)
            await asyncio.sleep(0)
            await client._terminate_current_process()
            self.timer.advance(3600)

            self.assertEqual(
                [e for e in bus.events if isinstance(e, DownstreamCrashedEvent)], []
            )
            # 老进程残留的日志也不得再喂进作废的观察者
            client._feed_log_line(watcher, f"[ERROR] {FAILURE_MARKER} again")
            await asyncio.sleep(0)
            self.timer.advance(3600)
            self.assertEqual(
                [e for e in bus.events if isinstance(e, DownstreamCrashedEvent)], []
            )

        asyncio.run(scenario())

    def test_crash_detection_drops_pending_stall_verdict(self):
        """崩溃被检测到时就地作废卡死判定，避免同一次失败既记崩溃又记卡死。

        崩溃与重启之间隔着崩溃延迟，期间挂起的卡死计时器一旦到期会把同一次
        执行异常再记一次崩溃，直接让崩溃降级阈值被数两次（issue #14）。
        """

        async def scenario() -> None:
            # 崩溃延迟必须长于卡死超时，才可能让卡死计时器在重启空窗期内到期
            client, bus = self._make_client(
                restart_delay_sec=0.5, loop=asyncio.get_running_loop()
            )
            client._downstream_ready = True
            client._process = _FakeProcess()
            client.start_downstream = MagicMock()  # type: ignore[method-assign]

            async def fake_wait_ready() -> None:
                # 重启完成后立即置退出信号，让监控循环自然结束
                client.exiting = True

            client.wait_downstream_ready = fake_wait_ready  # type: ignore[method-assign]
            client._create_log_watcher()

            self._feed_failure_line(client)
            monitor = asyncio.create_task(client.monitor_downstream())
            # 让监控走完崩溃检测、进入重启延迟等待
            await asyncio.sleep(0.1)
            self.timer.advance(3600)
            await asyncio.wait_for(monitor, timeout=10)

            crash_events = [
                e for e in bus.events if isinstance(e, DownstreamCrashedEvent)
            ]
            self.assertEqual(len(crash_events), 1)

        asyncio.run(scenario())

    def test_shutdown_drops_pending_stall_verdict(self):
        """网关退出时挂起的卡死判定必须丢弃，避免退出流程里再触发一次重启。"""

        async def scenario() -> None:
            client, bus = self._make_client(loop=asyncio.get_running_loop())
            client._process = _FakeProcess()
            client._create_log_watcher()

            self._feed_failure_line(client)
            await asyncio.sleep(0)
            await client.shutdown()
            self.timer.advance(3600)

            self.assertEqual(
                [e for e in bus.events if isinstance(e, DownstreamCrashedEvent)], []
            )

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
