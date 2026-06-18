import sys
import shlex
import subprocess
import logging
from typing import Optional, Any

logger = logging.getLogger(__name__)

class ExternalProgramManager:
    """管理在空闲和繁忙状态下需要运行的外部程序（例如挖矿程序、风扇控制或 GPU 监控等）"""
    def __init__(self, idle_path: str, busy_path: str):
        self.idle_path = idle_path
        self.busy_path = busy_path
        self.idle_process: Optional[subprocess.Popen[Any]] = None
        self.busy_process: Optional[subprocess.Popen[Any]] = None
        self.initialized = False
        self.last_busy_state = False
        self.ever_active = False

    def is_running(self) -> bool:
        """检查是否有任何由本管理器启动的外部程序正在运行"""
        running = False
        if self.idle_process and self.idle_process.poll() is None:
            running = True
        if self.busy_process and self.busy_process.poll() is None:
            running = True
        return running

    def update_state(self, is_busy: bool) -> None:
        """根据网关当前是繁忙还是空闲，更新并调度外部程序的启动/停止状态"""
        if not self.initialized or is_busy != self.last_busy_state:
            self.initialized = True
            self.last_busy_state = is_busy
            
            if is_busy:
                self.ever_active = True
                self.start_busy()
            elif self.ever_active:
                self.start_idle()

    def _run_program(self, cmd_str: str) -> Optional[subprocess.Popen[Any]]:
        """在后台静默运行指定的命令字符串，避免弹出 Windows 黑色命令行窗口"""
        if not cmd_str or not cmd_str.strip():
            return None
            
        is_win = sys.platform == "win32"
        startupinfo = None
        if is_win:
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = 0  # 隐藏窗口样式
            
        try:
            args = shlex.split(cmd_str, posix=not is_win)
            if not args:
                return None
            # Windows 下 .bat 和 .cmd 批处理文件需要通过 cmd.exe 显式调用
            if is_win and args[0].lower().endswith(('.bat', '.cmd')):
                args = ["cmd", "/c"] + args
            return subprocess.Popen(args, startupinfo=startupinfo)
        except Exception as e:
            logger.error(f"Failed to start program '{cmd_str}': {e}")
            return None

    def start_idle(self) -> None:
        """启动空闲时运行的外部程序，并杀掉繁忙时的外部程序"""
        if self.busy_process:
            try:
                self.busy_process.kill()
            except Exception:
                pass
            self.busy_process = None

        if self.idle_path and (not self.idle_process or self.idle_process.poll() is not None):
            logger.info(f"🌙 Starting idle program: {self.idle_path}")
            self.idle_process = self._run_program(self.idle_path)

    def start_busy(self) -> None:
        """启动繁忙时运行的外部程序，并杀掉空闲时的外部程序"""
        if self.idle_process:
            try:
                self.idle_process.kill()
            except Exception:
                pass
            self.idle_process = None

        if self.busy_path and (not self.busy_process or self.busy_process.poll() is not None):
            logger.info(f"🔥 Starting busy program: {self.busy_path}")
            self.busy_process = self._run_program(self.busy_path)

    def cleanup(self) -> None:
        """终止所有由该管理器启动的外部进程（空闲或繁忙程序）"""
        if self.idle_process:
            try:
                self.idle_process.kill()
            except Exception:
                pass
            self.idle_process = None
        if self.busy_process:
            try:
                self.busy_process.kill()
            except Exception:
                pass
            self.busy_process = None
