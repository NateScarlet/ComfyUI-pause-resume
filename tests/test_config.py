import os
import unittest
from unittest.mock import patch

from gateway.config import GatewayConfig


class GatewayConfigTest(unittest.TestCase):
    """验证环境变量到配置字段的映射。"""

    @patch("gateway.config.load_env", lambda path: None)  # 隔离仓库根目录的 .env
    def test_pause_program_from_env(self):
        """COMFYUI_PAUSE_PROGRAM 环境变量应映射到 pause_program 配置。"""
        with patch.dict(os.environ, {"COMFYUI_PAUSE_PROGRAM": "miner.cmd"}):
            config = GatewayConfig()
        self.assertEqual(config.pause_program, "miner.cmd")

    @patch("gateway.config.load_env", lambda path: None)
    def test_pause_program_default_empty(self):
        """未设置环境变量时，pause_program 应为空字符串。"""
        with patch.dict(os.environ, {}, clear=True):
            config = GatewayConfig()
        self.assertEqual(config.pause_program, "")

    @patch("gateway.config.load_env", lambda path: None)
    def test_queue_compression_default_auto(self):
        """未设置环境变量时，压缩档位默认 auto（自动探测可选依赖）。"""
        with patch.dict(os.environ, {}, clear=True):
            config = GatewayConfig()
        self.assertEqual(config.queue_compression, "auto")

    @patch("gateway.config.load_env", lambda path: None)
    def test_queue_compression_from_env(self):
        """COMFYUI_QUEUE_COMPRESSION 环境变量应映射到 queue_compression 配置。"""
        with patch.dict(os.environ, {"COMFYUI_QUEUE_COMPRESSION": "ZSTD"}):
            config = GatewayConfig()
        self.assertEqual(config.queue_compression, "zstd")

    @patch("gateway.config.load_env", lambda path: None)
    def test_queue_compression_invalid_value_rejected(self):
        """非法的压缩档位取值应在配置解析期直接报错。"""
        with patch.dict(os.environ, {"COMFYUI_QUEUE_COMPRESSION": "lzma"}), \
                self.assertRaises(ValueError):
            GatewayConfig()

    @patch("gateway.config.load_env", lambda path: None)
    def test_queue_dictionary_tuning_defaults(self):
        """未设置环境变量时，字典轮换三参数使用内置默认值。"""
        with patch.dict(os.environ, {}, clear=True):
            config = GatewayConfig()
        self.assertEqual(config.queue_rotation_interval, 512)
        self.assertEqual(config.queue_dict_sample_count, 300)
        self.assertEqual(config.queue_dict_target_size, 65536)

    @patch("gateway.config.load_env", lambda path: None)
    def test_queue_dictionary_tuning_from_env(self):
        """三个轮换调优参数应分别映射对应环境变量。"""
        env = {
            "COMFYUI_QUEUE_ROTATION_INTERVAL": "1024",
            "COMFYUI_QUEUE_DICT_SAMPLE_COUNT": "200",
            "COMFYUI_QUEUE_DICT_TARGET_SIZE": "32768",
        }
        with patch.dict(os.environ, env):
            config = GatewayConfig()
        self.assertEqual(config.queue_rotation_interval, 1024)
        self.assertEqual(config.queue_dict_sample_count, 200)
        self.assertEqual(config.queue_dict_target_size, 32768)

    @patch("gateway.config.load_env", lambda path: None)
    def test_queue_dictionary_tuning_non_positive_rejected(self):
        """非正整数的轮换调优参数应在配置解析期直接报错。"""
        with patch.dict(os.environ, {"COMFYUI_QUEUE_ROTATION_INTERVAL": "0"}), \
                self.assertRaises(ValueError):
            GatewayConfig()

    @patch("gateway.config.load_env", lambda path: None)
    def test_proxy_address_from_extra_args(self):
        """COMFYUI_EXTRA_ARGS 中的 --listen/--port 应覆盖网关实际监听地址与端口。"""
        with patch.dict(
            os.environ, {"COMFYUI_EXTRA_ARGS": "--listen 0.0.0.0 --port 9000"}
        ):
            config = GatewayConfig()
        self.assertEqual(config.proxy_host, "0.0.0.0")
        self.assertEqual(config.proxy_port, 9000)

    @patch("gateway.config.load_env", lambda path: None)
    def test_idle_shutdown_switches_default_off(self):
        """两个新开关默认关闭，行为与改动前一致。"""
        with patch.dict(os.environ, {}, clear=True):
            config = GatewayConfig()
        self.assertFalse(config.on_demand_start)
        self.assertFalse(config.allow_idle_shutdown)

    @patch("gateway.config.load_env", lambda path: None)
    def test_idle_shutdown_switches_from_env(self):
        """两个新开关从环境变量读取 true/false。"""
        env = {
            "COMFYUI_ON_DEMAND_START": "true",
            "COMFYUI_ALLOW_IDLE_SHUTDOWN": "TRUE",
        }
        with patch.dict(os.environ, env):
            config = GatewayConfig()
        self.assertTrue(config.on_demand_start)
        self.assertTrue(config.allow_idle_shutdown)

    @patch("gateway.config.load_env", lambda path: None)
    def test_idle_shutdown_switch_invalid_value_rejected(self):
        """非 true/false 的开关取值应在配置解析期直接报错。"""
        with patch.dict(os.environ, {"COMFYUI_ON_DEMAND_START": "yes"}), \
                self.assertRaises(ValueError):
            GatewayConfig()

    @patch("gateway.config.load_env", lambda path: None)
    def test_idle_shutdown_memory_threshold_default(self):
        """未设置环境变量时，空闲停机内存阈值默认 1024 MB。"""
        with patch.dict(os.environ, {}, clear=True):
            config = GatewayConfig()
        self.assertEqual(config.idle_shutdown_memory_threshold_mb, 1024)

    @patch("gateway.config.load_env", lambda path: None)
    def test_idle_shutdown_memory_threshold_from_env(self):
        """阈值环境变量应映射到配置字段。"""
        with patch.dict(
            os.environ, {"COMFYUI_IDLE_SHUTDOWN_MEMORY_THRESHOLD_MB": "2048"}
        ):
            config = GatewayConfig()
        self.assertEqual(config.idle_shutdown_memory_threshold_mb, 2048)

    @patch("gateway.config.load_env", lambda path: None)
    def test_idle_shutdown_memory_threshold_non_positive_rejected(self):
        """阈值为 0 会让任何可用内存都判定为紧张，必须在配置解析期直接报错。"""
        with patch.dict(
            os.environ, {"COMFYUI_IDLE_SHUTDOWN_MEMORY_THRESHOLD_MB": "0"}
        ), self.assertRaises(ValueError):
            GatewayConfig()

    @patch("gateway.config.load_env", lambda path: None)
    def test_stall_timeout_default(self):
        """未设置环境变量时，下游异常后的收尾超时默认 1800 秒。"""
        with patch.dict(os.environ, {}, clear=True):
            config = GatewayConfig()
        self.assertEqual(config.stall_timeout_sec, 1800)

    @patch("gateway.config.load_env", lambda path: None)
    def test_stall_timeout_from_env(self):
        """COMFYUI_STALL_TIMEOUT_SEC 环境变量应映射到 stall_timeout_sec 配置。"""
        with patch.dict(os.environ, {"COMFYUI_STALL_TIMEOUT_SEC": "300"}):
            config = GatewayConfig()
        self.assertEqual(config.stall_timeout_sec, 300)

    @patch("gateway.config.load_env", lambda path: None)
    def test_stall_timeout_non_positive_rejected(self):
        """收尾超时为 0 会把每次执行异常都误判为卡死，必须在配置解析期直接报错。"""
        with patch.dict(os.environ, {"COMFYUI_STALL_TIMEOUT_SEC": "0"}), \
                self.assertRaises(ValueError):
            GatewayConfig()


if __name__ == "__main__":
    unittest.main()
