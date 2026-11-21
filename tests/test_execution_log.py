import unittest

from gateway.infrastructure.comfyui.execution_log import ComfyUIExecutionLogWatcher
from gateway.shared.signals import (
    ExecutingFalse,
    ExecutingTrue,
    Stall,
    StallWatchStart,
)

# 取自 issue #14 的真实下游日志片段：下游抛出执行异常后工作线程死亡，收尾日志永不出现
_OOM_LINE = "[ERROR] !!! Exception during processing !!! CUDA error: out of memory"
_TRACEBACK_LINES = [
    "[ERROR] Traceback (most recent call last):",
    '  File "C:\\ComfyUI\\comfy\\execution.py", line 547, in execute',
    "torch.AcceleratorError: CUDA error: out of memory",
]


class TestComfyUIExecutionLogWatcher(unittest.TestCase):
    """测试纯翻译器：日志行到信号的翻译、错误摘要与作废语义。"""

    def _make_watcher(self) -> ComfyUIExecutionLogWatcher:
        # 构造函数不接收事件总线、计时器或回调：翻译器只产出信号
        return ComfyUIExecutionLogWatcher()

    def test_got_prompt_produces_executing_true(self):
        """收到任务标记翻译成 EXECUTING_TRUE 信号。"""
        watcher = self._make_watcher()

        self.assertEqual(watcher.feed("got prompt"), ExecutingTrue())

    def test_finished_produces_executing_false(self):
        """正常收尾标记翻译成 EXECUTING_FALSE 信号。"""
        watcher = self._make_watcher()

        self.assertEqual(
            watcher.feed("Prompt executed in 12.58 seconds"), ExecutingFalse()
        )

    def test_first_failure_produces_stall_watch_start(self):
        """首个执行异常标记翻译成 STALL_WATCH_START 信号，不带摘要 payload。"""
        watcher = self._make_watcher()

        self.assertEqual(watcher.feed(_OOM_LINE), StallWatchStart())

    def test_failure_then_finalization_returns_executing_false(self):
        """收尾日志仍翻译成 EXECUTING_FALSE，且撤销本次收尾等待不再判定卡死。"""
        watcher = self._make_watcher()

        watcher.feed(_OOM_LINE)
        self.assertEqual(
            watcher.feed("Prompt executed in 12.58 seconds"), ExecutingFalse()
        )

        self.assertIsNone(watcher.on_stall_timeout())

    def test_awaiting_lines_are_collected_without_extra_signals(self):
        """等待收尾期间的日志并入摘要，不再产生额外信号。"""
        watcher = self._make_watcher()

        watcher.feed(_OOM_LINE)
        for line in _TRACEBACK_LINES:
            self.assertIsNone(watcher.feed(line))

        stall = watcher.on_stall_timeout()
        assert isinstance(stall, Stall)
        for line in _TRACEBACK_LINES:
            self.assertIn(line, stall.summary)

    def test_stall_timeout_returns_summary_only_once(self):
        """仍在等待收尾时超时返回摘要，之后再次调用不再产出信号。"""
        watcher = self._make_watcher()
        watcher.feed(_OOM_LINE)
        for line in _TRACEBACK_LINES:
            watcher.feed(line)

        stall = watcher.on_stall_timeout()
        assert isinstance(stall, Stall)
        self.assertIn(_OOM_LINE, stall.summary)
        for line in _TRACEBACK_LINES:
            self.assertIn(line, stall.summary)
        self.assertIsNone(watcher.on_stall_timeout())

    def test_repeated_failure_keeps_first_summary_and_emits_no_new_signal(self):
        """同一次执行的后续异常并入摘要，不再开启新的收尾窗口。"""
        watcher = self._make_watcher()

        watcher.feed(_OOM_LINE)
        self.assertIsNone(watcher.feed("[ERROR] Traceback (most recent call last):"))
        self.assertIsNone(
            watcher.feed("[ERROR] !!! Exception during processing !!! RuntimeError: boom")
        )

        stall = watcher.on_stall_timeout()
        assert isinstance(stall, Stall)
        self.assertIn(_OOM_LINE, stall.summary)
        self.assertIn("Traceback (most recent call last):", stall.summary)
        self.assertIn("RuntimeError: boom", stall.summary)

    def test_error_summary_stops_at_finalization(self):
        """正常收尾后日志不再并入上一个任务的错误摘要。"""
        watcher = self._make_watcher()

        watcher.feed(_OOM_LINE)
        watcher.feed("Prompt executed in 0.10 seconds")
        watcher.feed("[INFO] 0 models unloaded.")
        # 收尾之后的下一次异常属于另一个任务，应重新开启新的收尾窗口
        self.assertEqual(
            watcher.feed("[ERROR] !!! Exception during processing !!! RuntimeError: boom"),
            StallWatchStart(),
        )

        stall = watcher.on_stall_timeout()
        assert isinstance(stall, Stall)
        self.assertNotIn("0 models unloaded", stall.summary)
        self.assertIn("RuntimeError: boom", stall.summary)

    def test_error_summary_is_bounded(self):
        """异常之后的下游输出没有上界，摘要必须封顶而不是无界增长。"""
        watcher = self._make_watcher()

        watcher.feed(_OOM_LINE)
        for i in range(5000):
            watcher.feed(f"noise {i}")

        stall = watcher.on_stall_timeout()
        assert isinstance(stall, Stall)
        self.assertIn("error summary truncated", stall.summary)
        self.assertNotIn("noise 4999", stall.summary)

    def test_long_task_without_failure_never_produces_stall(self):
        """没有异常标记的长任务不开启收尾窗口，不会产出 STALL。"""
        watcher = self._make_watcher()

        watcher.feed("got prompt")
        for _ in range(100):
            watcher.feed("[INFO] still rendering")

        self.assertIsNone(watcher.on_stall_timeout())

    def test_close_invalidates_watcher_and_ignores_late_lines(self):
        """作废后的观察者忽略迟到日志，不再产出任何信号。"""
        watcher = self._make_watcher()

        watcher.feed("got prompt")
        watcher.close()

        self.assertIsNone(watcher.feed(_OOM_LINE))
        self.assertIsNone(watcher.feed("got prompt"))
        self.assertIsNone(watcher.on_stall_timeout())


if __name__ == "__main__":
    unittest.main()
