import unittest
from unittest.mock import MagicMock, patch

from gateway.infrastructure.system.tray import SystemTrayController
from gateway.shared.models import DownstreamState, GatewayState


class TestSystemTrayController(unittest.TestCase):
    """测试托盘菜单与提示文案（#16）。"""

    def _make_controller(
        self, paused: bool = False, running: int = 0, allow: bool = False
    ) -> SystemTrayController:
        app = MagicMock()
        app.get_state.handle.return_value = GatewayState(
            paused=paused,
            allow_idle_shutdown=allow,
            downstream_state=DownstreamState.READY,
        )
        reader = MagicMock()
        reader.count.return_value = running
        return SystemTrayController(
            app_facade=app,
            queue_reader=reader,
            loop=MagicMock(),
            exit_event=MagicMock(),
            fonts={},
        )

    def test_tooltip_uses_waiting_for_current_task_text(self):
        """暂停后仍有任务执行时，提示文案为「等待当前任务完成」（#16）。"""
        controller = self._make_controller(paused=True, running=1)
        with patch("gateway.infrastructure.system.tray._LC", "zh"):
            self.assertEqual(
                controller._get_tooltip(),
                "ComfyUI Gateway - 等待当前任务完成 (队列: 1)",
            )

    def test_menu_contains_idle_shutdown_checkbox_without_start_stop_items(self):
        """托盘只有一个空闲停机勾选项，不出现启动/停止下游菜单（#16）。"""
        controller = self._make_controller(allow=False)
        with patch("gateway.infrastructure.system.tray._LC", "zh"):
            labels = [item.text for item in controller._create_menu().items]

        self.assertIn("空闲时允许下游停机", labels)
        self.assertNotIn("启动下游", labels)
        self.assertNotIn("停止下游", labels)
        self.assertNotIn("正在停止", labels)

    def test_toggling_checkbox_updates_preference_via_facade(self):
        """勾选动作通过 Facade 命令更新偏好（#16）。"""
        controller = self._make_controller(allow=False)

        controller._on_toggle_idle_shutdown()

        self.assertTrue(controller._allow_idle_shutdown)
        controller._loop.call_soon_threadsafe.assert_called_once_with(
            controller._app_facade.set_allow_idle_shutdown.handle, True
        )


if __name__ == "__main__":
    unittest.main()
