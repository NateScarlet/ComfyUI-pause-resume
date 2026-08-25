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


if __name__ == "__main__":
    unittest.main()
