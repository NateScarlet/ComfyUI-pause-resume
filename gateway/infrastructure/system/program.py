import sys
import shlex
import subprocess
import logging
from typing import Optional, Any
from gateway.shared.interfaces import ProcessManager

logger = logging.getLogger(__name__)


class ExternalProgramManager(ProcessManager):
    """管理在空闲和繁忙状态下需要运行的外部程序（例如监控、挖矿程序等）。"""

    def __init__(self, idle_path: str, busy_path: str):
        self._idle_path = idle_path
        self._busy_path = busy_path
        self._idle_process: Optional[subprocess.Popen[Any]] = None
        self._busy_process: Optional[subprocess.Popen[Any]] = None
        self._last_state: tuple[bool, bool] | None = None

    def is_running(self) -> bool:
        """检查是否有任何由本管理器启动的外部程序正在运行。"""
        running = False
        if self._idle_process and self._idle_process.poll() is None:
            running = True
        if self._busy_process and self._busy_process.poll() is None:
            running = True
        return running

    def update_state(self, is_busy: bool, ever_active: bool) -> None:
        """根据网关当前是繁忙还是空闲，更新并调度外部程序的启动/停止状态。"""
        state = (is_busy, ever_active)
        if state != self._last_state:
            self._last_state = state

            if is_busy:
                self._start_busy()
            elif ever_active:
                self._start_idle()

    def _run_program(self, cmd_str: str) -> Optional[subprocess.Popen[Any]]:
        """在后台静默运行指定的命令字符串，避免弹出 Windows 命令行窗口。"""
        if not cmd_str or not cmd_str.strip():
            return None

        is_win = sys.platform == "win32"
        startupinfo = None
        if is_win:
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = 0

        try:
            args = shlex.split(cmd_str, posix=not is_win)
            if not args:
                return None
            if is_win and args[0].lower().endswith((".bat", ".cmd")):
                args = ["cmd", "/c"] + args
            return subprocess.Popen(args, startupinfo=startupinfo)
        except Exception as e:
            logger.error(f"Failed to start program '{cmd_str}': {e}")
            return None

    def _start_idle(self) -> None:
        """启动空闲时运行的外部程序，并杀掉繁忙时的外部程序。"""
        if self._busy_process:
            try:
                self._busy_process.kill()
            except Exception:
                pass
            self._busy_process = None

        if self._idle_path and (
            not self._idle_process or self._idle_process.poll() is not None
        ):
            logger.info(f"🌙 Starting idle program: {self._idle_path}")
            self._idle_process = self._run_program(self._idle_path)

    def _start_busy(self) -> None:
        """启动繁忙时运行的外部程序，并杀掉空闲时的外部程序。"""
        if self._idle_process:
            try:
                self._idle_process.kill()
            except Exception:
                pass
            self._idle_process = None

        if self._busy_path and (
            not self._busy_process or self._busy_process.poll() is not None
        ):
            logger.info(f"🔥 Starting busy program: {self._busy_path}")
            self._busy_process = self._run_program(self._busy_path)

    def cleanup(self) -> None:
        """终止所有由该管理器启动的外部进程（空闲或繁忙程序）。"""
        if self._idle_process:
            try:
                self._idle_process.kill()
            except Exception:
                pass
            self._idle_process = None
        if self._busy_process:
            try:
                self._busy_process.kill()
            except Exception:
                pass
            self._busy_process = None
