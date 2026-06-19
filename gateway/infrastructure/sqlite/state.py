import sqlite3
import threading
from gateway.shared.interfaces import StateRepository


class SQLiteStateRepository(StateRepository):
    """基于 SQLite3 数据库实现的网关运行时状态（例如暂停/恢复）持久化仓储。"""

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            self._db_path, check_same_thread=False, timeout=30.0
        )
        self._init_db()

    def _init_db(self) -> None:
        """初始化持久化状态表结构，并启用高效的 WAL 日志模式。"""
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA synchronous=NORMAL;")

            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS state (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            self._conn.commit()

    def get_paused(self) -> bool:
        """查询网关当前的暂停设置状态。"""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("SELECT value FROM state WHERE key = 'paused'")
            row = cursor.fetchone()
            if row is not None:
                return row[0] == "true"
            return False

    def set_paused(self, paused: bool) -> None:
        """持久化保存网关的暂停设置状态。"""
        with self._lock:
            val = "true" if paused else "false"
            self._conn.execute(
                "INSERT OR REPLACE INTO state (key, value) VALUES ('paused', ?)", (val,)
            )
            self._conn.commit()

    def close(self) -> None:
        """关闭数据库持久化连接。"""
        with self._lock:
            self._conn.close()
