import json
import sqlite3
import threading
from typing import Optional
from gateway.shared.interfaces import StateRepository
from gateway.shared.models import EstimationState, TimeBucket


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

    def get_estimation_state(self) -> Optional[EstimationState]:
        """获取预估时间状态，返回 None 表示无数据。"""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("SELECT value FROM state WHERE key = 'estimation'")
            row = cursor.fetchone()
            if row is None:
                return None
            try:
                data = json.loads(row[0])
                return EstimationState(
                    active=TimeBucket(
                        avg_ms=data["active"]["avgMs"], count=data["active"]["count"]
                    ),
                    staging=TimeBucket(
                        avg_ms=data["staging"]["avgMs"], count=data["staging"]["count"]
                    ),
                )
            except (json.JSONDecodeError, KeyError):
                return None

    def save_estimation_state(self, state: EstimationState) -> None:
        """保存预估时间状态。"""
        with self._lock:
            data = {
                "active": {
                    "avgMs": state.active.avg_ms,
                    "count": state.active.count,
                },
                "staging": {
                    "avgMs": state.staging.avg_ms,
                    "count": state.staging.count,
                },
            }
            self._conn.execute(
                "INSERT OR REPLACE INTO state (key, value) VALUES ('estimation', ?)",
                (json.dumps(data),),
            )
            self._conn.commit()

    def close(self) -> None:
        """关闭数据库持久化连接。"""
        with self._lock:
            self._conn.close()
