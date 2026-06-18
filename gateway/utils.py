import ctypes

class PowerManagement:
    """Windows 系统休眠管理，防止在执行任务或运行脚本时系统进入睡眠状态"""
    ES_CONTINUOUS = 0x80000000
    ES_SYSTEM_REQUIRED = 0x00000001
    
    @staticmethod
    def prevent_sleep() -> None:
        """阻止系统进入休眠"""
        try:
            ctypes.windll.kernel32.SetThreadExecutionState(
                PowerManagement.ES_CONTINUOUS | PowerManagement.ES_SYSTEM_REQUIRED)
        except Exception:
            pass

    @staticmethod
    def allow_sleep() -> None:
        """恢复系统默认休眠行为"""
        try:
            ctypes.windll.kernel32.SetThreadExecutionState(
                PowerManagement.ES_CONTINUOUS)
        except Exception:
            pass
