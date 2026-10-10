import unittest
from typing import Any, Callable, List, Type, TypeVar

from gateway.infrastructure.comfyui.execution_log import ComfyUIExecutionLogWatcher
from gateway.shared.events import DownstreamExecutingChangedEvent
from gateway.shared.interfaces import EventBus
from tests.fakes import FakeTimer

T = TypeVar("T")

# 取自 issue #14 的真实下游日志片段：下游抛出执行异常后工作线程死亡，收尾日志永不出现
_OOM_LINE = "[ERROR] !!! Exception during processing !!! CUDA error: out of memory"
_TRACEBACK_LINES = [
    "[ERROR] Traceback (most recent call last):",
    '  File "C:\\ComfyUI\\comfy\\execution.py", line 547, in execute',
    "torch.AcceleratorError: CUDA error: out of memory",
]


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

    def executing_flags(self) -> List[bool]:
        return [
            e.executing
            for e in self.events
            if isinstance(e, DownstreamExecutingChangedEvent)
        ]


class TestComfyUIExecutionLogWatcher(unittest.TestCase):
    """测试下游执行日志观察者的执行状态翻译与卡死判定。"""

    STALL_TIMEOUT = 1800.0

    def _make_watcher(self) -> tuple[
        ComfyUIExecutionLogWatcher, _RecordingEventBus, FakeTimer, List[str]
    ]:
        bus = _RecordingEventBus()
        timer = FakeTimer()
        stalls: List[str] = []
        watcher = ComfyUIExecutionLogWatcher(
            event_bus=bus,
            timer=timer,
            stall_timeout_sec=self.STALL_TIMEOUT,
            on_stall=stalls.append,
        )
        return watcher, bus, timer, stalls

    def test_failure_followed_by_normal_finalization_is_not_a_stall(self):
        """异常标记后下游仍打印正常收尾日志时属于可恢复失败，不得判定卡死。"""
        watcher, bus, timer, stalls = self._make_watcher()

        watcher.feed("got prompt")
        watcher.feed(_OOM_LINE)
        for line in _TRACEBACK_LINES:
            watcher.feed(line)
        watcher.feed("Prompt executed in 12.58 seconds")

        timer.advance(self.STALL_TIMEOUT * 2)

        self.assertEqual(stalls, [])
        self.assertEqual(timer.pending_count, 0)
        # 收尾判定只来自日志文本，异常本身不产生额外信号
        self.assertEqual(bus.executing_flags(), [True, False])

    def test_failure_without_finalization_is_reported_as_stall(self):
        """异常标记后收尾超时内没有任何收尾日志时判定卡死，并带上错误摘要。"""
        watcher, _, timer, stalls = self._make_watcher()

        watcher.feed("got prompt")
        watcher.feed(_OOM_LINE)
        for line in _TRACEBACK_LINES:
            watcher.feed(line)

        timer.advance(self.STALL_TIMEOUT - 1)
        self.assertEqual(stalls, [])

        timer.advance(1)
        self.assertEqual(len(stalls), 1)
        self.assertIn(_OOM_LINE, stalls[0])
        for line in _TRACEBACK_LINES:
            self.assertIn(line, stalls[0])

    def test_long_task_without_failure_marker_is_never_a_stall(self):
        """纯长任务（全程没有异常标记）在任意时长下都不会被判定卡死。"""
        watcher, bus, timer, stalls = self._make_watcher()

        watcher.feed("got prompt")
        timer.advance(self.STALL_TIMEOUT * 100)

        self.assertEqual(stalls, [])
        self.assertEqual(timer.pending_count, 0)
        self.assertEqual(bus.executing_flags(), [True])

    def test_repeated_failure_marker_keeps_the_first_deadline(self):
        """同一次执行抛出多个异常时收尾窗口仍锚定在首个异常，不被后续异常推迟。"""
        watcher, _, timer, stalls = self._make_watcher()

        watcher.feed(_OOM_LINE)
        timer.advance(self.STALL_TIMEOUT - 100)
        watcher.feed("[ERROR] Traceback (most recent call last):")
        timer.advance(99)
        self.assertEqual(stalls, [])

        timer.advance(1)
        self.assertEqual(len(stalls), 1)
        # 错误摘要保留两个异常的痕迹，便于落盘 failed_workflows 时还原现场
        self.assertIn(_OOM_LINE, stalls[0])
        self.assertIn("Traceback (most recent call last):", stalls[0])

    def test_close_cancels_the_pending_stall_timer(self):
        """下游进程生命周期结束时，挂起的卡死计时必须被撤销。"""
        watcher, _, timer, stalls = self._make_watcher()

        watcher.feed(_OOM_LINE)
        watcher.close()
        timer.advance(self.STALL_TIMEOUT * 2)

        self.assertEqual(stalls, [])
        self.assertEqual(timer.pending_count, 0)

    def test_error_summary_stops_at_finalization(self):
        """正常收尾后日志不再并入上一个任务的错误摘要。"""
        watcher, _, timer, stalls = self._make_watcher()

        watcher.feed(_OOM_LINE)
        watcher.feed("Prompt executed in 0.10 seconds")
        watcher.feed("[INFO] 0 models unloaded.")
        # 收尾之后的下一次异常属于另一个任务，其摘要不应混入上一个任务
        watcher.feed("[ERROR] !!! Exception during processing !!! RuntimeError: boom")
        timer.advance(self.STALL_TIMEOUT)

        self.assertEqual(len(stalls), 1)
        self.assertNotIn("0 models unloaded", stalls[0])
        self.assertIn("RuntimeError: boom", stalls[0])


    def test_error_summary_is_bounded(self):
        """异常之后的下游输出没有上界，摘要必须封顶而不是无界增长。"""
        watcher, _, timer, stalls = self._make_watcher()

        watcher.feed(_OOM_LINE)
        for i in range(5000):
            watcher.feed(f"noise {i}")
        timer.advance(self.STALL_TIMEOUT)

        self.assertEqual(len(stalls), 1)
        self.assertIn("error summary truncated", stalls[0])
        self.assertNotIn("noise 4999", stalls[0])

    def test_closed_watcher_ignores_late_log_lines(self):
        """作废后的观察者不再接受任何日志行，避免老进程残留日志污染新生命周期。"""
        watcher, bus, timer, stalls = self._make_watcher()

        watcher.feed("got prompt")
        watcher.close()
        watcher.feed(_OOM_LINE)
        watcher.feed("got prompt")
        timer.advance(self.STALL_TIMEOUT * 2)

        self.assertEqual(stalls, [])
        self.assertEqual(bus.executing_flags(), [True])


if __name__ == "__main__":
    unittest.main()
