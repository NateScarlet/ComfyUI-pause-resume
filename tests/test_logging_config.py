import logging
import os
import tempfile
import unittest
from logging import handlers as log_handlers

from gateway.infrastructure.logging_config import LOG_FILENAME, configure_logging


class TestConfigureLogging(unittest.TestCase):
    """日志必须落盘到数据目录，让事后诊断无需人工搬运终端回滚。"""

    def _close_handlers(self) -> None:
        root = logging.getLogger()
        for handler in list(root.handlers):
            handler.close()
            root.removeHandler(handler)
        root.setLevel(logging.WARNING)

    def test_creates_log_file_in_data_dir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            try:
                path = configure_logging(tmp)
                self.assertEqual(path, os.path.join(tmp, LOG_FILENAME))
                self.assertTrue(os.path.isfile(path))
            finally:
                self._close_handlers()

    def test_info_log_is_written_to_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            try:
                path = configure_logging(tmp)
                logging.getLogger("test.logging").info("persisted message")
                for handler in logging.getLogger().handlers:
                    handler.flush()
                with open(path, encoding="utf-8") as f:
                    content = f.read()
                self.assertIn("persisted message", content)
                self.assertIn("[GATEWAY]", content)
            finally:
                self._close_handlers()

    def test_retains_stderr_handler(self) -> None:
        """落盘不能取代终端输出：原有 stderr 行为必须保留。"""
        with tempfile.TemporaryDirectory() as tmp:
            try:
                configure_logging(tmp)
                root_handlers = logging.getLogger().handlers
                self.assertTrue(any(type(h) is logging.StreamHandler for h in root_handlers))
                self.assertTrue(
                    any(
                        isinstance(h, log_handlers.RotatingFileHandler)
                        for h in root_handlers
                    )
                )
            finally:
                self._close_handlers()

    def test_debug_flag_selects_level(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            try:
                configure_logging(tmp, debug=True)
                self.assertEqual(logging.getLogger().level, logging.DEBUG)
            finally:
                self._close_handlers()


if __name__ == "__main__":
    unittest.main()
