import sqlite3
import threading
import logging
import time
import json
from typing import List, Optional, Tuple, Any

from gateway.shared.interfaces import TaskQueueReader, TaskQueueWriter
from gateway.shared.models import Task, RawJSON, TaskStatus, TaskFilters

logger = logging.getLogger(__name__)


class SQLiteQueue(TaskQueueReader, TaskQueueWriter):
    """基于 SQLite3 数据库实现的任务队列，支持高并发读写，并集成了 WAL 日志模式。"""

    def __init__(self, db_path: str, history_retention_days: int = 90):
        self._db_path = db_path
        self._history_retention_days = history_retention_days
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

    def get_tasks(
        self,
        filter_by: Optional[TaskFilters] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        desc: bool = False,
    ) -> List[Tuple[TaskStatus, Task]]:
        """获取符合条件的任务列表，支持分页限制和排序方向。"""
        with self._lock:
            cursor = self._conn.cursor()
            where_parts: List[str] = []
            params: List[Any] = []

            if filter_by is not None:
                if filter_by.statuses is not None:
                    placeholders = ",".join("?" for _ in filter_by.statuses)
                    where_parts.append(f"status IN ({placeholders})")
                    params.extend(s.value for s in filter_by.statuses)

                if filter_by.workflow_id is not None:
                    # 数据库层粗筛：通过 LIKE 排除绝大部分数据，只反序列化可能匹配的行
                    where_parts.append("extra_data LIKE ?")
                    params.append(f"%{filter_by.workflow_id}%")

            where_clause = " WHERE " + " AND ".join(where_parts) if where_parts else ""
            order_dir = "DESC" if desc else "ASC"

            # 如果存在内存细筛（如通过 workflow_id），我们必须加载所有粗筛结果到内存，
            # 过滤后再在 Python 内存里执行 LIMIT/OFFSET 切片。
            # 如果没有内存细筛，直接将分页下限推给 SQL 引擎以保障极致性能。
            has_memory_filter = (
                filter_by is not None and filter_by.workflow_id is not None
            )

            limit_offset_clause = ""
            db_params = list(params)
            if not has_memory_filter:
                if limit is not None:
                    limit_offset_clause += " LIMIT ?"
                    db_params.append(limit)
                    if offset is not None:
                        limit_offset_clause += " OFFSET ?"
                        db_params.append(offset)
                elif offset is not None:
                    limit_offset_clause += " LIMIT -1 OFFSET ?"
                    db_params.append(offset)

            cursor.execute(
                "SELECT number, id, prompt, extra_data, outputs_to_execute, create_time, status "
                f"FROM tasks_v2 {where_clause} ORDER BY number {order_dir}{limit_offset_clause}",
                db_params,
            )
            result: List[Tuple[TaskStatus, Task]] = []
            if not has_memory_filter:
                for row in cursor.fetchall():
                    task = self._row_to_task(row)
                    task_status = TaskStatus(row[6])
                    result.append((task_status, task))
            else:
                skipped = 0
                for row in cursor:
                    task = self._row_to_task(row)
                    task_status = TaskStatus(row[6])
                    # 内存细筛
                    if filter_by is not None and not filter_by.matches(
                        task_status, task
                    ):
                        continue

                    if offset is not None and skipped < offset:
                        skipped += 1
                        continue

                    result.append((task_status, task))
                    if limit is not None and len(result) >= limit:
                        break

            return result

    def get_task_count(
        self,
        filter_by: Optional[TaskFilters] = None,
        limit: Optional[int] = None,
    ) -> int:
        """获取符合条件的任务数量。"""
        t_start = time.perf_counter()
        has_memory_filter = filter_by is not None and filter_by.workflow_id is not None

        if not has_memory_filter:
            with self._lock:
                cursor = self._conn.cursor()
                where_parts: List[str] = []
                params: List[Any] = []

                if filter_by is not None:
                    if filter_by.statuses is not None:
                        placeholders = ",".join("?" for _ in filter_by.statuses)
                        where_parts.append(f"status IN ({placeholders})")
                        params.extend(s.value for s in filter_by.statuses)

                where_clause = (
                    " WHERE " + " AND ".join(where_parts) if where_parts else ""
                )

                if limit is not None:
                    cursor.execute(
                        f"SELECT 1 FROM tasks_v2 {where_clause} LIMIT ?",
                        params + [limit],
                    )
                    result = len(cursor.fetchall())
                else:
                    cursor.execute(
                        f"SELECT COUNT(*) FROM tasks_v2 {where_clause}", params
                    )
                    row = cursor.fetchone()
                    result = row[0] if row else 0
        else:
            # 存在内存细筛时，通过 get_tasks 获取精细化过滤的任务并统计其数量
            tasks = self.get_tasks(filter_by=filter_by)
            result = len(tasks)
            if limit is not None:
                result = min(result, limit)

        t_total = (time.perf_counter() - t_start) * 1000
        if t_total > 10:
            logger.debug(
                f"SQLite get_task_count(filter_by={filter_by}, limit={limit}) = {result} took {t_total:.1f}ms"
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
                return self._row_to_task(row)
            return None

    def requeue_running(self) -> None:
        """将正在运行的任务放回待处理队列（原位置不变）。"""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(
                "UPDATE tasks_v2 SET status = 'pending' WHERE status = 'running'"
            )
            self._conn.commit()

    def requeue_running_if_exists(self) -> bool:
        """原子地将正在运行的任务放回队列，返回是否确实存在任务并成功放回。"""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(
                "UPDATE tasks_v2 SET status = 'pending' WHERE status = 'running'"
            )
            changed = cursor.rowcount > 0
            self._conn.commit()
            return changed

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

    def update_task_status(
        self,
        new_status: TaskStatus,
        prompt_id: Optional[str] = None,
        filter_status: Optional[TaskStatus] = None,
    ) -> bool:
        """更新指定任务的状态。"""
        with self._lock:
            cursor = self._conn.cursor()
            where_parts: List[str] = []
            params: List[Any] = []
            if prompt_id is not None:
                where_parts.append("id = ?")
                params.append(prompt_id)
            if filter_status is not None:
                where_parts.append("status = ?")
                params.append(filter_status.value)

            where_clause = " WHERE " + " AND ".join(where_parts) if where_parts else ""

            cursor.execute(
                f"UPDATE tasks_v2 SET status = ?{where_clause}",
                [new_status.value] + params,
            )
            changed = cursor.rowcount > 0
            self._conn.commit()
            if changed and new_status in (
                TaskStatus.COMPLETED,
                TaskStatus.FAILED,
                TaskStatus.CANCELLED,
            ):
                self._cleanup_expired_history()
            return changed

    def _cleanup_expired_history(self) -> None:
        """清理已过期（如超过 retention_days）的非活动任务历史记录。"""
        expire_time = int(time.time() * 1000) - (
            self._history_retention_days * 24 * 3600 * 1000
        )
        cursor = self._conn.cursor()
        cursor.execute(
            "DELETE FROM tasks_v2 WHERE status IN ('completed', 'failed', 'cancelled') AND create_time < ?",
            (expire_time,),
        )
        self._conn.commit()

    def close(self) -> None:
        """释放数据库连接资源。"""
        with self._lock:
            self._conn.close()

    def get_task_by_id(self, prompt_id: str) -> Optional[Tuple[TaskStatus, Task]]:
        """根据 ID 获取任务及其当前状态。"""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(
                "SELECT number, id, prompt, extra_data, outputs_to_execute, create_time, status "
                "FROM tasks_v2 WHERE id = ?",
                (prompt_id,),
            )
            row = cursor.fetchone()
            if row:
                task = self._row_to_task(row)
                task_status = TaskStatus(row[6])
                return task_status, task
            return None
