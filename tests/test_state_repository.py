import os
import tempfile
import unittest

from gateway.infrastructure.sqlite.state import SQLiteStateRepository


class TestSQLiteStateRepositoryIdleShutdownPreference(unittest.TestCase):
    """测试空闲停机授权勾选的持久化（#16）。"""

    def test_allow_idle_shutdown_preference_persists_across_repository_instances(
        self,
    ):
        """勾选状态写入数据库后，新的仓储实例仍能读到（模拟网关重启）。"""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "state.db")

            repo = SQLiteStateRepository(db_path)
            self.assertIsNone(repo.get_allow_idle_shutdown())
            repo.set_allow_idle_shutdown(True)
            repo.close()

            reopened = SQLiteStateRepository(db_path)
            try:
                self.assertTrue(reopened.get_allow_idle_shutdown())
                reopened.set_allow_idle_shutdown(False)
                self.assertFalse(reopened.get_allow_idle_shutdown())
            finally:
                reopened.close()


if __name__ == "__main__":
    unittest.main()
