import logging
import threading
from typing import Optional, Any

from gateway.application.facade import AppFacade
from gateway.shared.interfaces import DownstreamClient, JobQueueReader
from gateway.shared.models import JobStatus, JobFilters
from gateway.shared.events import StateChangedEvent, StatusChangedEvent

logger = logging.getLogger(__name__)

try:
    import pystray  # type: ignore[import-untyped]
    from PIL import Image, ImageDraw  # type: ignore[import-untyped]

    _tray_available = True
except ImportError:
    _tray_available = False
    pystray = None  # type: ignore[assignment]
    Image = None  # type: ignore[assignment]
    ImageDraw = None  # type: ignore[assignment]


def _create_icon(color: str) -> Any:
    """创建指定颜色的圆形托盘图标。"""
    if Image is None or ImageDraw is None:
        raise RuntimeError("Pillow 未安装")
    img = Image.new("RGBA", (64, 64), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    draw.ellipse([4, 4, 60, 60], fill=color)
    return img


_icon_running: Optional[Any] = None
_icon_stopping: Optional[Any] = None
_icon_paused: Optional[Any] = None


def _ensure_icons() -> None:
    """延迟创建图标单例，避免模块导入时即依赖 Pillow。"""
    global _icon_running, _icon_stopping, _icon_paused
    if _icon_running is None:
        _icon_running = _create_icon("#4CAF50")
    if _icon_stopping is None:
        _icon_stopping = _create_icon("#FFC107")
    if _icon_paused is None:
        _icon_paused = _create_icon("#F44336")


def _create_pystray_icon(icon: Any, title: str, menu: Any) -> Any:
    """封装 pystray.Icon 构造，避免闭包内类型 narrowing 问题。"""
    return pystray.Icon("comfyui_gateway", icon, title, menu)  # type: ignore[union-attr]


class SystemTrayController:
    """Windows 系统托盘控制器，提供暂停/恢复、重启下游、查看队列数量与退出等操作。

    在独立线程中运行 pystray 消息循环，通过 loop.call_soon_threadsafe
    将业务操作安全地调度回主事件循环执行。
    """

    def __init__(
        self,
        app_facade: AppFacade,
        queue_reader: JobQueueReader,
        downstream_service: DownstreamClient,
        loop: Any,
        exit_event: Any,
    ):
        if not _tray_available:
            raise ImportError("系统托盘需要安装额外依赖：pip install .[tray]")
        _ensure_icons()

        self._app_facade = app_facade
        self._queue_reader = queue_reader
        self._downstream_service = downstream_service
        self._loop = loop
        self._exit_event = exit_event

        self._paused = app_facade.get_state.handle()
        self._queue_count = self._queue_reader.count(
            JobFilters([JobStatus.PENDING, JobStatus.RUNNING])
        )
        self._estimated_time_ms: Optional[int] = None  # 预估时间（毫秒）
        self._restart_pending = False  # 是否正在等待暂停后重启
        self._icon: Optional[Any] = None
        self._thread: Optional[threading.Thread] = None
        self._refresh_timer: Optional[threading.Timer] = None  # 自动恢复定时器
        self._refresh_lock = threading.Lock()  # 防抖锁

    def start(self) -> None:
        """在独立后台线程中启动系统托盘图标。"""

        def _run() -> None:
            self._icon = _create_pystray_icon(
                self._get_current_icon(),
                self._get_tooltip(),
                self._create_menu(),
            )
            # 注入钩子解决分辨率变化或远程桌面切换导致的托盘图标消失 Bug
            self._setup_icon_hooks(self._icon)
            self._icon.run()  # type: ignore[union-attr]

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """停止并移除系统托盘图标。"""
        with self._refresh_lock:
            if self._refresh_timer is not None:
                self._refresh_timer.cancel()
                self._refresh_timer = None
        if self._icon is not None:
            self._icon.stop()

    def _setup_icon_hooks(self, icon: Any) -> None:
        """为 pystray.Icon 注入钩子，在分辨率变化或任务栏重建时自动重新加载图标句柄并刷新。

        同时劫持底层 _message 发送方法，在 NIM_ADD 失败时进行指数退避重试，以解决不稳定期托盘未就绪问题。
        """
        import ctypes
        import time
        from pystray._util import win32  # type: ignore[import-untyped]

        # 1. 劫持底层 _message 发送方法，在注册图标 (NIM_ADD) 失败时进行指数退避重试
        original_message = icon._message

        def patched_message(code: int, flags: int, **kwargs: Any) -> Any:
            if code == win32.NIM_ADD:
                max_retries = 5
                retry_delay = 0.05  # 初始重试间隔 50ms
                for attempt in range(max_retries):
                    res = win32.Shell_NotifyIcon(
                        code,
                        win32.NOTIFYICONDATAW(
                            cbSize=ctypes.sizeof(win32.NOTIFYICONDATAW),
                            hWnd=icon._hwnd,
                            hID=id(icon),
                            uFlags=flags,
                            **kwargs,
                        ),
                    )
                    if res:
                        if attempt > 0:
                            logger.info(
                                f"Tray icon NIM_ADD succeeded on attempt {attempt + 1}"
                            )
                        return res
                    logger.warning(
                        f"Tray icon NIM_ADD failed on attempt {attempt + 1}, retrying in {retry_delay}s..."
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2  # 指数退避
                logger.error("Tray icon NIM_ADD failed after maximum retries.")
                return False
            else:
                return original_message(code, flags, **kwargs)

        icon._message = patched_message

        # 2. 自动恢复逻辑与防抖
        def do_restore(original_handler: Any, wparam: Any, lparam: Any) -> None:
            try:
                if icon.visible:
                    # 强行释放旧图标句柄并清空缓存，迫使下一次 _show 重新调用 LoadImage
                    icon._release_icon()
                    icon._icon_valid = False

                    # 调用原始的事件处理函数（原始函数会同步调用 _hide / _show 或其他后续版本内部逻辑）
                    if original_handler is not None:
                        original_handler(wparam, lparam)
                    else:
                        # 兜底行为，如果原始回调不存在
                        icon._hide()
                        icon._show()

                    # 刷新提示文字与菜单内容
                    icon._update_title()
                    icon.update_menu()
                    logger.info(
                        "Successfully restored system tray icon after display change."
                    )
            except Exception as e:
                logger.error(f"Failed to restore system tray icon: {e}", exc_info=True)
            finally:
                with self._refresh_lock:
                    self._refresh_timer = None

        def trigger_restore(original_handler: Any, wparam: Any, lparam: Any) -> None:
            with self._refresh_lock:
                if self._refresh_timer is not None:
                    self._refresh_timer.cancel()
                # 防抖：50 毫秒内多次触发显示变更仅执行最后一次，传入原始处理函数及消息参数
                self._refresh_timer = threading.Timer(
                    0.05, do_restore, args=[original_handler, wparam, lparam]
                )
                self._refresh_timer.daemon = True
                self._refresh_timer.start()

        # 3. 拦截 pystray 的底层消息回调，将原始的回调函数保存并传入防抖逻辑中
        original_on_display_change = getattr(icon, "_on_display_change", None)
        original_on_taskbarcreated = getattr(icon, "_on_taskbarcreated", None)

        def patched_on_display_change(wparam: Any, lparam: Any) -> int:
            logger.info(
                "Display change message received, scheduling tray icon restore..."
            )
            trigger_restore(original_on_display_change, wparam, lparam)
            return 0

        def patched_on_taskbarcreated(wparam: Any, lparam: Any) -> int:
            logger.info(
                "Taskbar created message received, scheduling tray icon restore..."
            )
            trigger_restore(original_on_taskbarcreated, wparam, lparam)
            return 0

        if original_on_display_change is not None:
            icon._on_display_change = patched_on_display_change
        if original_on_taskbarcreated is not None:
            icon._on_taskbarcreated = patched_on_taskbarcreated

    def _is_stopping(self) -> bool:
        """是否处于"正在停止"过渡态：已暂停但仍有任务正在下游执行，等待其完成。

        注意：仅依据队列剩余数量不足以判定，因为暂停后 PENDING 任务不会再被派发，
        但仍会保留在队列中。真正的过渡态只由"是否仍有任务在下游执行"决定。
        """
        if not self._paused:
            return False
        return self._queue_reader.count(JobFilters([JobStatus.RUNNING]), limit=1) > 0

    def _get_current_icon(self) -> Any:
        if self._is_stopping():
            return _icon_stopping
        return _icon_paused if self._paused else _icon_running

    @staticmethod
    def _format_duration(ms: int) -> str:
        """将毫秒时间戳转换为可读格式（如 "5分30秒"）。"""
        seconds = ms // 1000
        minutes, secs = divmod(seconds, 60)
        hours, mins = divmod(minutes, 60)
        if hours > 0:
            return f"{hours}时{mins}分{secs}秒"
        elif minutes > 0:
            return f"{minutes}分{secs}秒"
        return f"{secs}秒"

    def _get_tooltip(self) -> str:
        if self._is_stopping():
            state = "正在停止"
        elif self._paused:
            state = "已暂停"
        else:
            state = "运行中"
        tooltip = f"ComfyUI Gateway - {state} (队列: {self._queue_count})"
        if self._estimated_time_ms is not None:
            tooltip += f" 预计: {self._format_duration(self._estimated_time_ms)}"
        return tooltip

    # ── 事件回调（可能从非主线程调用，需线程安全） ──

    def on_state_changed(self, event: StateChangedEvent) -> None:
        """响应领域层暂停状态变更事件，刷新托盘图标与菜单。"""
        self._paused = event.paused
        # 恢复时重置重启等待状态
        if not self._paused:
            self._restart_pending = False
        if self._icon is not None:
            self._icon.icon = self._get_current_icon()
            self._icon.title = self._get_tooltip()
            self._icon.update_menu()

    def on_status_changed(self, event: StatusChangedEvent) -> None:
        """响应队列数量变更事件，刷新托盘提示与菜单。"""
        self._queue_count = event.queue_remaining
        self._estimated_time_ms = event.estimated_time_ms
        if self._icon is not None:
            # 队列归零会令"正在停止"过渡为"已停止"，故需同步刷新图标
            self._icon.icon = self._get_current_icon()
            self._icon.title = self._get_tooltip()
            self._icon.update_menu()

    # ── 菜单动作回调（pystray 线程 → 主事件循环） ──

    def _on_pause_resume(self) -> None:
        """切换暂停/恢复状态。"""
        if self._paused:
            self._loop.call_soon_threadsafe(self._app_facade.resume_queue.handle)
            self._paused = False
            logger.info("⏸️▶ Queue Resumed via tray")
        else:
            self._loop.call_soon_threadsafe(self._app_facade.pause_queue.handle, False)
            self._paused = True
            logger.info("⏸️ Queue Paused via tray")
        if self._icon is not None:
            self._icon.icon = self._get_current_icon()
            self._icon.title = self._get_tooltip()
            self._icon.update_menu()

    def _on_restart(self) -> None:
        """重启下游 ComfyUI 服务。

        第一次点击：暂停队列（带 restart_after_idle 标志），等待当前任务完成后自动重启
        第二次点击（已处于等待状态）：立即重启下游服务
        """
        if self._restart_pending:
            # 已经处于等待状态，立即重启
            self._restart_pending = False
            self._loop.call_soon_threadsafe(self._downstream_service.restart)
            logger.info("🔄 Restarting downstream immediately via tray")
        else:
            # 第一次点击：暂停并标记等待重启
            self._restart_pending = True
            self._loop.call_soon_threadsafe(
                self._app_facade.pause_queue.handle, True
            )  # restart_after_idle=True
            logger.info("⏸️ Queue paused, waiting for idle to restart via tray")

        if self._icon is not None:
            self._icon.update_menu()

    def _on_exit(self) -> None:
        """触发网关优雅退出。"""
        self._loop.call_soon_threadsafe(self._exit_event.set)

    # ── 菜单构建 ──

    def _create_menu(self) -> Any:
        """构建托盘右键上下文菜单，使用动态文本以反映最新状态。"""
        assert pystray is not None

        # pystray 会在每次打开菜单时调用 callable，并传入 MenuItem 实例
        def pause_text(_: Any) -> str:
            return "恢复任务" if self._paused else "暂停任务"

        def restart_text(_: Any) -> str:
            return "立即重启" if self._restart_pending else "暂停后重启"

        def queue_text(_: Any) -> str:
            text = f"队列: {self._queue_count}"
            if self._estimated_time_ms is not None:
                text += f" 预计: {self._format_duration(self._estimated_time_ms)}"
            return text

        return pystray.Menu(
            pystray.MenuItem(queue_text, None, enabled=False),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem(pause_text, self._on_pause_resume),
            pystray.MenuItem(restart_text, self._on_restart),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("退出", self._on_exit),
        )
