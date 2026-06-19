import sqlite3
import threading
import logging
import time
import json
from typing import List, Optional, Any

from gateway.shared.interfaces import TaskQueueReader, TaskQueueWriter
from gateway.shared.models import Task, RawJSON

logger = logging.getLogger(__name__)


class SQLiteQueue(TaskQueueReader, TaskQueueWriter):
    """基于 SQLite3 数据库实现的任务队列，支持高并发读写，并集成了 WAL 日志模式。"""

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            self._db_path, check_same_thread=False, timeout=30.0
        )
        self._init_db()

    def _init_db(self) -> None:
        """初始化数据库表结构，处理从低版本的单向平滑迁移。"""
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA synchronous=NORMAL;")

            cursor = self._conn.cursor()
            cursor.execute("PRAGMA user_version")
            db_version = cursor.fetchone()[0]

            SUPPORTED_VERSION = 2
            if db_version > SUPPORTED_VERSION:
                raise RuntimeError(
                    f"Database version error: The database version is {db_version}, "
                    f"but the current code only supports up to version {SUPPORTED_VERSION}."
                )

            if db_version < 2:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS tasks_v2 (
                        id TEXT PRIMARY KEY,
                        number INTEGER,
                        prompt TEXT,
                        extra_data TEXT,
                        outputs_to_execute TEXT,
                        status TEXT,
                        create_time INTEGER
                    )
                """)

                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='tasks'"
                )
                old_tasks_exists = cursor.fetchone()

                if old_tasks_exists:
                    logger.info(
                        "🔧 Migrating SQLite schema: Migrating data from 'tasks' to 'tasks_v2'..."
                    )
                    select_cursor = self._conn.cursor()
                    insert_cursor = self._conn.cursor()
                    select_cursor.execute(
                        "SELECT id, number, prompt, extra_data, outputs_to_execute, status FROM tasks"
                    )
                    for row in select_cursor:
                        (
                            task_id,
                            number,
                            prompt_str,
                            extra_data_str,
                            outputs_str,
                            status,
                        ) = row
                        create_time = -1
                        insert_cursor.execute(
                            "INSERT INTO tasks_v2 (id, number, prompt, extra_data, outputs_to_execute, status, create_time) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (
                                task_id,
                                number,
                                prompt_str,
                                extra_data_str,
                                outputs_str,
                                status,
                                create_time,
                            ),
                        )
                    cursor.execute("DROP TABLE tasks")
                    logger.info(
                        "✅ SQLite schema migration to version 2 completed successfully."
                    )

                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS metadata (
                        key TEXT PRIMARY KEY,
                        value TEXT
                    )
                """)

                cursor.execute("PRAGMA user_version = 2")
                self._conn.commit()

    @staticmethod
    def _row_to_task(row: Any) -> Task:
        """将 DB 查询出的行数据转化为不可变的 Task 实体。"""
        return Task(
            number=row[0],
            prompt_id=row[1],
            prompt=RawJSON(row[2]),
            extra_data=RawJSON(row[3]),
            outputs_to_execute=json.loads(row[4]),
            create_time=row[5],
        )

    def get_pending(self) -> List[Task]:
        """获取所有待处理任务（按 number 升序排序）。"""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(
                "SELECT number, id, prompt, extra_data, outputs_to_execute, create_time FROM tasks_v2 WHERE status = 'pending' ORDER BY number ASC"
            )
            tasks: List[Task] = []
            for row in cursor.fetchall():
                try:
                    tasks.append(self._row_to_task(row))
                except Exception as e:
                    logger.error(f"Failed to decode task from DB: {e}")
            return tasks

    def get_running(self) -> List[Task]:
        """获取所有正在运行任务。"""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(
                "SELECT number, id, prompt, extra_data, outputs_to_execute, create_time FROM tasks_v2 WHERE status = 'running'"
            )
            tasks: List[Task] = []
            for row in cursor.fetchall():
                try:
                    tasks.append(self._row_to_task(row))
                except Exception as e:
                    logger.error(f"Failed to decode running task from DB: {e}")
            return tasks

    def get_pending_count(self, limit: Optional[int] = None) -> int:
        """获取待处理任务数量，支持 limit 以优化扫描性能。"""
        t_start = time.perf_counter()
        with self._lock:
            cursor = self._conn.cursor()
            if limit is not None:
                cursor.execute(
                    "SELECT 1 FROM tasks_v2 WHERE status = 'pending' LIMIT ?", (limit,)
                )
                result = len(cursor.fetchall())
            else:
                cursor.execute("SELECT COUNT(*) FROM tasks_v2 WHERE status = 'pending'")
                row = cursor.fetchone()
                result = row[0] if row else 0
        t_total = (time.perf_counter() - t_start) * 1000
        if t_total > 10:
            logger.debug(
                f"SQLite get_pending_count(limit={limit}) = {result} took {t_total:.1f}ms"
            )
        return result

    def get_running_count(self, limit: Optional[int] = None) -> int:
        """获取正在运行任务数量，支持 limit 以优化扫描性能。"""
        t_start = time.perf_counter()
        with self._lock:
            cursor = self._conn.cursor()
            if limit is not None:
                cursor.execute(
                    "SELECT 1 FROM tasks_v2 WHERE status = 'running' LIMIT ?", (limit,)
                )
                result = len(cursor.fetchall())
            else:
                cursor.execute("SELECT COUNT(*) FROM tasks_v2 WHERE status = 'running'")
                row = cursor.fetchone()
                result = row[0] if row else 0
        t_total = (time.perf_counter() - t_start) * 1000
        if t_total > 10:
            logger.debug(
                f"SQLite get_running_count(limit={limit}) = {result} took {t_total:.1f}ms"
            )
        return result

    def new_task_number(self) -> int:
        """分配生成一个新的唯一任务编号。"""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("SELECT value FROM metadata WHERE key = 'next_task_number'")
            row = cursor.fetchone()
            number = int(row[0]) if row else 1
            cursor.execute(
                "INSERT OR REPLACE INTO metadata (key, value) VALUES ('next_task_number', ?)",
                (str(number + 1),),
            )
            self._conn.commit()
            return number

    def add_task(self, task: Task) -> None:
        """添加新任务到待处理队列中。"""
        t_start = time.perf_counter()
        with self._lock:
            cursor = self._conn.cursor()
            t_serialize_start = time.perf_counter()
            outputs_str = json.dumps(task.outputs_to_execute, ensure_ascii=False)
            t_serialize = (time.perf_counter() - t_serialize_start) * 1000

            cursor.execute(
                "INSERT INTO tasks_v2 (id, number, prompt, extra_data, outputs_to_execute, status, create_time) VALUES (?, ?, ?, ?, ?, 'pending', ?)",
                (
                    task.prompt_id,
                    task.number,
                    task.prompt,
                    task.extra_data,
                    outputs_str,
                    task.create_time,
                ),
            )
            t_commit_start = time.perf_counter()
            self._conn.commit()
            t_commit = (time.perf_counter() - t_commit_start) * 1000
        t_total = (time.perf_counter() - t_start) * 1000
        logger.debug(
            f"SQLite add_task {task.prompt_id}: "
            f"serialize={t_serialize:.1f}ms commit={t_commit:.1f}ms total={t_total:.1f}ms"
        )

    def pop_task(self, skip: int = 0) -> Optional[Task]:
        """弹出指定偏移量的待处理任务，并将其更新标记为正在运行。"""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(
                "SELECT number, id, prompt, extra_data, outputs_to_execute, create_time FROM tasks_v2 WHERE status = 'pending' ORDER BY number ASC LIMIT 1 OFFSET ?",
                (skip,),
            )
            row = cursor.fetchone()
            if row:
                target_id = row[1]
                cursor.execute(
                    "UPDATE tasks_v2 SET status = 'running' WHERE id = ?", (target_id,)
                )
                self._conn.commit()

                try:
                    return self._row_to_task(row)
                except Exception as e:
                    logger.error(f"Failed to decode popped task: {e}")
            return None

    def requeue_running(self) -> None:
        """将正在运行的任务放回待处理队列（原位置不变）。"""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(
                "UPDATE tasks_v2 SET status = 'pending' WHERE status = 'running'"
            )
            self._conn.commit()

    def clear_running(self) -> None:
        """清空所有正在运行的任务。"""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("DELETE FROM tasks_v2 WHERE status = 'running'")
            self._conn.commit()

    def clear_pending(self) -> None:
        """清空所有排队待处理的任务。"""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("DELETE FROM tasks_v2 WHERE status = 'pending'")
            self._conn.commit()

    def delete_pending(self, prompt_ids: List[str]) -> None:
        """删除指定的待处理任务。"""
        if not prompt_ids:
            return
        with self._lock:
            cursor = self._conn.cursor()
            placeholders = ",".join("?" for _ in prompt_ids)
            cursor.execute(
                f"DELETE FROM tasks_v2 WHERE status = 'pending' AND id IN ({placeholders})",
                tuple(prompt_ids),
            )
            self._conn.commit()

    def close(self) -> None:
        """关闭 SQLite 数据库连接。"""
        with self._lock:
            self._conn.close()
