import unittest
from typing import Any
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

    @staticmethod
    def _restart_item(controller: SystemTrayController) -> Any:
        """取出菜单中的重启项；文案在两种语言间切换，只能按已知文案集合匹配。"""
        restart_labels = {
            "立即重启下游",
            "Restart Downstream Now",
            "暂停后重启下游",
            "Pause then Restart Downstream",
        }
        items = controller._create_menu().items
        restart_items = [item for item in items if item.text in restart_labels]
        assert len(restart_items) == 1, [item.text for item in items]
        return restart_items[0]

    def test_restart_label_is_restart_now_when_paused_and_idle(self):
        """暂停且下游无任务执行时，重启项文案为「立即重启下游」（#17）。"""
        controller = self._make_controller(paused=True, running=0)
        with patch("gateway.infrastructure.system.tray._LC", "zh"):
            self.assertEqual(self._restart_item(controller).text, "立即重启下游")

    def test_clicking_restart_when_paused_and_idle_forces_restart(self):
        """暂停且下游空闲时单击重启项，调度的是强制重启而不是再次暂停（#17）。"""
        controller = self._make_controller(paused=True, running=0)

        self._restart_item(controller)(MagicMock())

        controller._loop.call_soon_threadsafe.assert_called_once_with(
            controller._app_facade.restart_downstream.handle
        )
        controller._app_facade.pause_queue.handle.assert_not_called()

    def test_restart_label_is_pause_then_restart_when_not_paused(self):
        """未暂停时重启项文案为「暂停后重启下游」（#17）。"""
        controller = self._make_controller(paused=False, running=0)
        with patch("gateway.infrastructure.system.tray._LC", "zh"):
            self.assertEqual(self._restart_item(controller).text, "暂停后重启下游")

    def test_first_click_when_not_paused_pauses_then_upgrades_to_restart_now(self):
        """未暂停时第一次单击只暂停并排程空闲后重启，文案随即升级（#17）。"""
        controller = self._make_controller(paused=False, running=0)
        item = self._restart_item(controller)

        item(MagicMock())

        controller._loop.call_soon_threadsafe.assert_called_once_with(
            controller._app_facade.pause_queue.handle, True
        )
        controller._app_facade.restart_downstream.handle.assert_not_called()

        with patch("gateway.infrastructure.system.tray._LC", "zh"):
            self.assertEqual(self._restart_item(controller).text, "立即重启下游")

    def test_second_click_when_not_paused_forces_restart(self):
        """未暂停时单击一次后再单击，执行强制重启（原有升级语义，#17）。"""
        controller = self._make_controller(paused=False, running=0)
        item = self._restart_item(controller)

        item(MagicMock())
        item(MagicMock())

        controller._loop.call_soon_threadsafe.assert_any_call(
            controller._app_facade.restart_downstream.handle
        )
        controller._loop.call_soon_threadsafe.assert_any_call(
            controller._app_facade.pause_queue.handle, True
        )

    def test_pending_restart_forces_restart_even_while_task_running(self):
        """单击一次进入等待重启后，即使下游仍有任务执行，再单击也强制重启（#17）。"""
        controller = self._make_controller(paused=False, running=1)
        item = self._restart_item(controller)

        item(MagicMock())
        item(MagicMock())

        controller._loop.call_soon_threadsafe.assert_any_call(
            controller._app_facade.restart_downstream.handle
        )

    def test_restart_label_stays_pause_then_restart_while_waiting_for_current_task(self):
        """暂停后仍有任务在下游执行时，重启项文案仍为「暂停后重启下游」（#17）。"""
        controller = self._make_controller(paused=True, running=1)
        with patch("gateway.infrastructure.system.tray._LC", "zh"):
            self.assertEqual(self._restart_item(controller).text, "暂停后重启下游")

    def test_pausing_via_tray_upgrades_restart_label_to_restart_now(self):
        """通过托盘暂停任务后，下游空闲时重启项立即变为「立即重启下游」（#17）。"""
        controller = self._make_controller(paused=False, running=0)

        controller._on_pause_resume()

        with patch("gateway.infrastructure.system.tray._LC", "zh"):
            self.assertEqual(self._restart_item(controller).text, "立即重启下游")

    def test_restart_label_in_english(self):
        """英文环境下两组重启文案都正确（#17）。"""
        with patch("gateway.infrastructure.system.tray._LC", "en"):
            idle_controller = self._make_controller(paused=True, running=0)
            self.assertEqual(
                self._restart_item(idle_controller).text, "Restart Downstream Now"
            )
            running_controller = self._make_controller(paused=False, running=0)
            self.assertEqual(
                self._restart_item(running_controller).text,
                "Pause then Restart Downstream",
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
