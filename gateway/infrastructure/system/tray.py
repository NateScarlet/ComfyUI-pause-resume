import logging
import threading
from typing import Optional, Any

from gateway.application.facade import AppFacade
from gateway.shared.interfaces import DownstreamClient
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
_icon_paused: Optional[Any] = None


def _ensure_icons() -> None:
    """延迟创建图标单例，避免模块导入时即依赖 Pillow。"""
    global _icon_running, _icon_paused
    if _icon_running is None:
        _icon_running = _create_icon("#4CAF50")
    if _icon_paused is None:
        _icon_paused = _create_icon("#FF9800")


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
        queue_reader: Any,
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
        self._queue_count = self._queue_reader.get_task_count()
        self._restart_pending = False  # 是否正在等待暂停后重启
        self._icon: Optional[Any] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """在独立后台线程中启动系统托盘图标。"""

        def _run() -> None:
            self._icon = _create_pystray_icon(
                self._get_current_icon(),
                self._get_tooltip(),
                self._create_menu(),
            )
            self._icon.run()  # type: ignore[union-attr]

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """停止并移除系统托盘图标。"""
        if self._icon is not None:
            self._icon.stop()

    def _get_current_icon(self) -> Any:
        return _icon_paused if self._paused else _icon_running

    def _get_tooltip(self) -> str:
        state = "已暂停" if self._paused else "运行中"
        return f"ComfyUI Gateway - {state} (队列: {self._queue_count})"

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
        if self._icon is not None:
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
            return f"队列: {self._queue_count}"

        return pystray.Menu(
            pystray.MenuItem(queue_text, None, enabled=False),
            pystray.MenuItem(pause_text, self._on_pause_resume),
            pystray.MenuItem(restart_text, self._on_restart),
            pystray.MenuItem("退出", self._on_exit),
        )
