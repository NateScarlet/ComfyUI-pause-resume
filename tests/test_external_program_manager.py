import unittest
from unittest.mock import MagicMock, patch

from gateway.infrastructure.system.program import ExternalProgramManager
from gateway.shared.interfaces import EventBus
from gateway.shared.models import QueueState


class ExternalProgramManagerTest(unittest.TestCase):
    """验证外部程序管理器根据领域上报的队列状态正确启停对应进程。"""

    def setUp(self) -> None:
        # 替换底层进程启动原语与监控线程，隔离真实子进程
        self.popen = patch(
            "gateway.infrastructure.system.program.subprocess.Popen"
        ).start()
        self.thread = patch(
            "gateway.infrastructure.system.program.threading.Thread"
        ).start()
        self.addCleanup(patch.stopall)

    def _manager(
        self,
        idle_path: str = "prog_idle",
        busy_path: str = "prog_busy",
        pause_path: str = "prog_pause",
    ) -> ExternalProgramManager:
        return ExternalProgramManager(idle_path, busy_path, pause_path, None)

    def _started_command(self) -> str:
        # Popen 的第一个位置参数是命令参数列表
        args = self.popen.call_args.args[0]
        return args[0]

    def test_paused_state_starts_pause_program(self):
        """队列暂停时启动暂停程序，且不受 ever_active 门控（#8 决策 D1）。"""
        mgr = self._manager()
        mgr.update_state(QueueState.PAUSED, False)
        self.popen.assert_called_once()
        self.assertEqual(self._started_command(), "prog_pause")

    def test_busy_state_starts_busy_and_kills_pause_process(self):
        """队列恢复繁忙时应终止暂停进程并启动繁忙程序。"""
        mgr = self._manager(pause_path="prog_pause")
        pause_proc = MagicMock()
        pause_proc.poll.return_value = None
        self.popen.return_value = pause_proc

        mgr.update_state(QueueState.PAUSED, False)
        mgr.update_state(QueueState.BUSY, True)

        pause_proc.kill.assert_called_once()
        self.assertEqual(self._started_command(), "prog_busy")

    def test_idle_state_gated_by_ever_active(self):
        """空闲状态维持既有门控：下游从未执行过任务时不启动闲置程序。"""
        mgr = self._manager()
        mgr.update_state(QueueState.IDLE, False)
        self.popen.assert_not_called()

        mgr.update_state(QueueState.IDLE, True)
        self.popen.assert_called_once()
        self.assertEqual(self._started_command(), "prog_idle")

    def test_same_state_report_does_not_restart_program(self):
        """相同队列状态与标志的重复上报不应重启外部程序。"""
        mgr = self._manager()
        mgr.update_state(QueueState.PAUSED, False)
        mgr.update_state(QueueState.PAUSED, False)
        self.popen.assert_called_once()

    def test_empty_pause_path_kills_other_programs_without_starting(self):
        """未配置暂停程序时，队列暂停只清理其它进程而不启动任何程序。"""
        mgr = self._manager(pause_path="")
        idle_proc = MagicMock()
        idle_proc.poll.return_value = None
        self.popen.return_value = idle_proc

        mgr.update_state(QueueState.IDLE, True)
        mgr.update_state(QueueState.PAUSED, False)

        idle_proc.kill.assert_called_once()
        self.assertEqual(self.popen.call_count, 1)  # 仅闲置程序那一次

    def test_identical_command_kept_running_across_state_changes(self):
        """暂停程序与闲置程序为同一命令时，队列状态变化应忽略切换并保留进程（#8）。"""
        mgr = self._manager(idle_path="same_prog", pause_path="same_prog")
        running = MagicMock()
        running.poll.return_value = None
        self.popen.return_value = running

        mgr.update_state(QueueState.IDLE, True)
        mgr.update_state(QueueState.PAUSED, False)

        # 进程既未被终止也未被重启，而是原样继续运行
        self.popen.assert_called_once()
        running.kill.assert_not_called()
        self.assertTrue(mgr.is_running())

    def test_is_running_reflects_managed_processes(self):
        """is_running 应反映任一受管进程是否仍在运行。"""
        mgr = self._manager()
        self.assertFalse(mgr.is_running())

        proc = MagicMock()
        proc.poll.return_value = None
        self.popen.return_value = proc
        mgr.update_state(QueueState.BUSY, False)
        self.assertTrue(mgr.is_running())

    def test_cleanup_kills_all_managed_processes(self):
        """cleanup 应终止所有受管进程。"""
        mgr = self._manager()
        proc = MagicMock()
        proc.poll.return_value = None
        self.popen.return_value = proc
        mgr.update_state(QueueState.IDLE, True)

        mgr.cleanup()
        proc.kill.assert_called()


if __name__ == "__main__":
    unittest.main()
