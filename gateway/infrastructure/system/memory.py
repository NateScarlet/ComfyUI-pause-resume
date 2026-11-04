import ctypes
from ctypes import wintypes

from gateway.shared.interfaces import MemorySampler


class _MEMORYSTATUSEX(ctypes.Structure):
    """Windows GlobalMemoryStatusEx 的入参/出参结构。"""

    _fields_ = [
        ("dwLength", wintypes.DWORD),
        ("dwMemoryLoad", wintypes.DWORD),
        ("ullTotalPhys", ctypes.c_ulonglong),
        ("ullAvailPhys", ctypes.c_ulonglong),
        ("ullTotalPageFile", ctypes.c_ulonglong),
        ("ullAvailPageFile", ctypes.c_ulonglong),
        ("ullTotalVirtual", ctypes.c_ulonglong),
        ("ullAvailVirtual", ctypes.c_ulonglong),
        ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
    ]


class WindowsMemorySampler(MemorySampler):
    """基于 Windows GlobalMemoryStatusEx 的可用物理内存采样实现。"""

    def available_memory_mb(self) -> int:
        """返回当前系统可用物理内存（MiB）。"""
        status = _MEMORYSTATUSEX()
        status.dwLength = ctypes.sizeof(_MEMORYSTATUSEX)
        if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            raise OSError(ctypes.get_last_error(), "GlobalMemoryStatusEx failed")
        return int(status.ullAvailPhys // (1024 * 1024))
