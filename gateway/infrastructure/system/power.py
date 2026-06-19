import ctypes
from gateway.shared.interfaces import SystemPowerController


class PowerManagement(SystemPowerController):
    """Windows 系统休眠管理，防止在执行任务或运行脚本时系统进入睡眠状态。"""

    _ES_CONTINUOUS = 0x80000000
    _ES_SYSTEM_REQUIRED = 0x00000001

    def __init__(self) -> None:
        self._prevented = False

    def prevent_sleep(self) -> None:
        """阻止系统进入休眠。"""
        if self._prevented:
            return
        try:
            ctypes.windll.kernel32.SetThreadExecutionState(
                self._ES_CONTINUOUS | self._ES_SYSTEM_REQUIRED
            )
            self._prevented = True
        except (AttributeError, OSError):
            pass

    def allow_sleep(self) -> None:
        """恢复系统默认休眠行为。"""
        if not self._prevented:
            return
        try:
            ctypes.windll.kernel32.SetThreadExecutionState(self._ES_CONTINUOUS)
            self._prevented = False
        except (AttributeError, OSError):
            pass
