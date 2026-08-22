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


if __name__ == "__main__":
    unittest.main()
