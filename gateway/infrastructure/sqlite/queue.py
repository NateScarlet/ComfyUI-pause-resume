import sqlite3
import threading
import logging
import time
import json
from typing import List, Optional, Tuple, Any

from gateway.shared.interfaces import TaskQueueReader, TaskQueueWriter
from gateway.shared.models import Task, RawJSON, TaskStatus, TaskFilters, TaskSummary

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

            SUPPORTED_VERSION = 5
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
                db_version = 2

            if db_version < 3:
                logger.info(
                    "🔧 Migrating SQLite schema: Creating indexes on 'tasks_v2'..."
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_tasks_status_number ON tasks_v2 (status, number)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_tasks_number ON tasks_v2 (number)"
                )
                cursor.execute("PRAGMA user_version = 3")
                self._conn.commit()
                logger.info(
                    "✅ SQLite schema migration to version 3 completed successfully."
                )
                db_version = 3

            if db_version < 4:
                logger.info(
                    "🔧 Migrating SQLite schema to version 4: Creating 'jobs' table and migrating data..."
                )
                # 1. 创建全新的 jobs 表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS jobs (
                        id TEXT PRIMARY KEY,
                        number INTEGER,
                        prompt TEXT,
                        extra_data TEXT,
                        workflow_id TEXT,
                        outputs_to_execute TEXT,
                        status TEXT,
                        create_time INTEGER
                    )
                """)

                # 2. 创建以 job 为命名的全新索引
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_jobs_status_number ON jobs (status, number)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_jobs_number ON jobs (number)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_jobs_workflow_id ON jobs (workflow_id)"
                )

                # 3. 检查旧表 tasks_v2 是否存在并搬迁数据
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='tasks_v2'"
                )
                old_table_exists = cursor.fetchone()

                if old_table_exists:
                    logger.info(
                        "📦 Found legacy 'tasks_v2' table. Migrating data to 'jobs'..."
                    )
                    select_cursor = self._conn.cursor()
                    insert_cursor = self._conn.cursor()
                    select_cursor.execute(
                        "SELECT id, number, prompt, extra_data, outputs_to_execute, status, create_time FROM tasks_v2"
                    )
                    for row in select_cursor:
                        (
                            task_id,
                            number,
                            prompt_str,
                            extra_data_str,
                            outputs_str,
                            status,
                            create_time,
                        ) = row

                        # 提取 workflow_id
                        w_id = None
                        if extra_data_str:
                            try:
                                extra_data = json.loads(extra_data_str)
                                w_id = (
                                    extra_data.get("extra_pnginfo", {})
                                    .get("workflow", {})
                                    .get("id")
                                )
                            except Exception:
                                pass

                        insert_cursor.execute(
                            "INSERT OR REPLACE INTO jobs (id, number, prompt, extra_data, workflow_id, outputs_to_execute, status, create_time) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            (
                                task_id,
                                number,
                                prompt_str,
                                extra_data_str,
                                w_id,
                                outputs_str,
                                status,
                                create_time,
                            ),
                        )

                    # 4. 删除旧的 tasks_v2 表
                    cursor.execute("DROP TABLE tasks_v2")
                    logger.info("✅ Data migration to 'jobs' table completed.")

                cursor.execute("PRAGMA user_version = 4")
                self._conn.commit()
                logger.info(
                    "✅ SQLite schema migration to version 4 completed successfully."
                )
                db_version = 4
            if db_version < 5:
                logger.info(
                    "🔧 Migrating SQLite schema to version 5: Adding execution & outputs columns..."
                )
                columns_to_add = [
                    ("outputs", "TEXT"),
                    ("preview_output", "TEXT"),
                    ("execution_start_time", "REAL"),
                    ("execution_end_time", "REAL"),
                    ("execution_error", "TEXT"),
                ]
                for col_name, col_type in columns_to_add:
                    try:
                        cursor.execute(
                            f"ALTER TABLE jobs ADD COLUMN {col_name} {col_type}"
                        )
                    except sqlite3.OperationalError as e:
                        if "duplicate column" not in str(e).lower():
                            raise
                cursor.execute("PRAGMA user_version = 5")
                self._conn.commit()
                logger.info(
                    "✅ SQLite schema migration to version 5 completed successfully."
                )
                db_version = 5

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
            outputs=RawJSON(row[6]) if row[6] else None,
            preview_output=RawJSON(row[7]) if row[7] else None,
            execution_start_time=row[8],
            execution_end_time=row[9],
            execution_error=RawJSON(row[10]) if row[10] else None,
        )

    @staticmethod
    def _row_to_task_summary(row: Any) -> TaskSummary:
        """将 DB 查询出的行数据转化为不可变的 TaskSummary 实体。"""
        return TaskSummary(
            number=row[0],
            prompt_id=row[1],
            status=TaskStatus(row[2]),
            workflow_id=row[3],
            create_time=row[4],
            extra_data=RawJSON(row[5]) if row[5] else None,
            outputs=RawJSON(row[6]) if row[6] else None,
            preview_output=RawJSON(row[7]) if row[7] else None,
            execution_start_time=row[8],
            execution_end_time=row[9],
            execution_error=RawJSON(row[10]) if row[10] else None,
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
                    # 直接通过 SQL 进行精确查找，完美利用索引
                    where_parts.append("workflow_id = ?")
                    params.append(filter_by.workflow_id)

            where_clause = " WHERE " + " AND ".join(where_parts) if where_parts else ""
            order_dir = "DESC" if desc else "ASC"

            limit_offset_clause = ""
            db_params = list(params)
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
                "SELECT number, id, prompt, extra_data, outputs_to_execute, create_time, outputs, preview_output, execution_start_time, execution_end_time, execution_error, status "
                f"FROM jobs {where_clause} ORDER BY number {order_dir}{limit_offset_clause}",
                db_params,
            )
            result: List[Tuple[TaskStatus, Task]] = []
            for row in cursor.fetchall():
                task = self._row_to_task(row)
                task_status = TaskStatus(row[11])
                result.append((task_status, task))

            return result

    def get_task_summaries(
        self,
        filter_by: Optional[TaskFilters] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        desc: bool = False,
    ) -> List[TaskSummary]:
        """获取符合条件的任务摘要列表，支持分页限制和排序方向。"""
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
                    # 直接通过 SQL 进行精确查找，完美利用索引
                    where_parts.append("workflow_id = ?")
                    params.append(filter_by.workflow_id)

            where_clause = " WHERE " + " AND ".join(where_parts) if where_parts else ""
            order_dir = "DESC" if desc else "ASC"

            limit_offset_clause = ""
            db_params = list(params)
            if limit is not None:
                limit_offset_clause += " LIMIT ?"
                db_params.append(limit)
                if offset is not None:
                    limit_offset_clause += " OFFSET ?"
                    db_params.append(offset)
            elif offset is not None:
                limit_offset_clause += " LIMIT -1 OFFSET ?"
                db_params.append(offset)

            # 查询中彻底省去了 prompt 和 outputs_to_execute 列，完全避免了大 JSON 文本的 I/O 损耗
            cursor.execute(
                "SELECT number, id, status, workflow_id, create_time, extra_data, outputs, preview_output, execution_start_time, execution_end_time, execution_error "
                f"FROM jobs {where_clause} ORDER BY number {order_dir}{limit_offset_clause}",
                db_params,
            )
            result: List[TaskSummary] = []
            for row in cursor.fetchall():
                result.append(self._row_to_task_summary(row))

            return result

    def get_task_count(
        self,
        filter_by: Optional[TaskFilters] = None,
        limit: Optional[int] = None,
    ) -> int:
        """获取符合条件的任务数量。"""
        t_start = time.perf_counter()
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
                    where_parts.append("workflow_id = ?")
                    params.append(filter_by.workflow_id)

            where_clause = " WHERE " + " AND ".join(where_parts) if where_parts else ""

            if limit is not None:
                cursor.execute(
                    f"SELECT 1 FROM jobs {where_clause} LIMIT ?",
                    params + [limit],
                )
                result = len(cursor.fetchall())
            else:
                cursor.execute(f"SELECT COUNT(*) FROM jobs {where_clause}", params)
                row = cursor.fetchone()
                result = row[0] if row else 0

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

        # 提取 workflow_id 以便写入数据库物理列
        w_id = None
        if task.extra_data:
            try:
                extra_data = json.loads(task.extra_data)
                w_id = extra_data.get("extra_pnginfo", {}).get("workflow", {}).get("id")
            except Exception:
                pass

        with self._lock:
            cursor = self._conn.cursor()
            t_serialize_start = time.perf_counter()
            outputs_str = json.dumps(task.outputs_to_execute, ensure_ascii=False)
            t_serialize = (time.perf_counter() - t_serialize_start) * 1000

            cursor.execute(
                "INSERT INTO jobs (id, number, prompt, extra_data, workflow_id, outputs_to_execute, status, create_time, outputs, preview_output, execution_start_time, execution_end_time, execution_error) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, NULL, NULL, NULL, NULL, NULL)",
                (
                    task.prompt_id,
                    task.number,
                    task.prompt,
                    task.extra_data,
                    w_id,
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

    def save_task(self, task: Task) -> bool:
        """保存任务数据实体（如果已存在则更新，不存在则插入）。"""
        w_id = None
        if task.extra_data:
            try:
                extra_data = json.loads(task.extra_data)
                w_id = extra_data.get("extra_pnginfo", {}).get("workflow", {}).get("id")
            except Exception:
                pass

        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("SELECT 1 FROM jobs WHERE id = ?", (task.prompt_id,))
            exists = cursor.fetchone() is not None

            outputs_str = json.dumps(task.outputs_to_execute, ensure_ascii=False)

            if exists:
                cursor.execute(
                    "UPDATE jobs SET number = ?, prompt = ?, extra_data = ?, workflow_id = ?, outputs_to_execute = ?, create_time = ?, outputs = ?, preview_output = ?, execution_start_time = ?, execution_end_time = ?, execution_error = ? WHERE id = ?",
                    (
                        task.number,
                        task.prompt,
                        task.extra_data,
                        w_id,
                        outputs_str,
                        task.create_time,
                        str(task.outputs) if task.outputs else None,
                        str(task.preview_output) if task.preview_output else None,
                        task.execution_start_time,
                        task.execution_end_time,
                        str(task.execution_error) if task.execution_error else None,
                        task.prompt_id,
                    ),
                )
                changed = cursor.rowcount > 0
            else:
                cursor.execute(
                    "INSERT INTO jobs (id, number, prompt, extra_data, workflow_id, outputs_to_execute, status, create_time, outputs, preview_output, execution_start_time, execution_end_time, execution_error) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?)",
                    (
                        task.prompt_id,
                        task.number,
                        task.prompt,
                        task.extra_data,
                        w_id,
                        outputs_str,
                        task.create_time,
                        str(task.outputs) if task.outputs else None,
                        str(task.preview_output) if task.preview_output else None,
                        task.execution_start_time,
                        task.execution_end_time,
                        str(task.execution_error) if task.execution_error else None,
                    ),
                )
                changed = True

            self._conn.commit()
            return changed

    def pop_task(self, skip: int = 0) -> Optional[Task]:
        """弹出指定偏移量的待处理任务，并将其更新标记为正在运行。"""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(
                "SELECT number, id, prompt, extra_data, outputs_to_execute, create_time, outputs, preview_output, execution_start_time, execution_end_time, execution_error FROM jobs WHERE status = 'pending' ORDER BY number ASC LIMIT 1 OFFSET ?",
                (skip,),
            )
            row = cursor.fetchone()
            if row:
                target_id = row[1]
                cursor.execute(
                    "UPDATE jobs SET status = 'running' WHERE id = ?", (target_id,)
                )
                self._conn.commit()
                return self._row_to_task(row)
            return None

    def requeue_running(self) -> None:
        """将正在运行的任务放回待处理队列（原位置不变）。"""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(
                "UPDATE jobs SET status = 'pending' WHERE status = 'running'"
            )
            self._conn.commit()

    def requeue_running_if_exists(self) -> bool:
        """原子地将正在运行的任务放回队列，返回是否确实存在任务并成功放回。"""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(
                "UPDATE jobs SET status = 'pending' WHERE status = 'running'"
            )
            changed = cursor.rowcount > 0
            self._conn.commit()
            return changed

    def clear_running(self) -> None:
        """清空所有正在运行的任务。"""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("DELETE FROM jobs WHERE status = 'running'")
            self._conn.commit()

    def clear_pending(self) -> None:
        """清空所有排队待处理的任务。"""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("DELETE FROM jobs WHERE status = 'pending'")
            self._conn.commit()

    def delete_pending(self, prompt_ids: List[str]) -> None:
        """删除指定的待处理任务。"""
        if not prompt_ids:
            return
        with self._lock:
            cursor = self._conn.cursor()
            placeholders = ",".join("?" for _ in prompt_ids)
            cursor.execute(
                f"DELETE FROM jobs WHERE status = 'pending' AND id IN ({placeholders})",
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
                f"UPDATE jobs SET status = ?{where_clause}",
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
            "DELETE FROM jobs WHERE status IN ('completed', 'failed', 'cancelled') AND create_time < ?",
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
                "SELECT number, id, prompt, extra_data, outputs_to_execute, create_time, outputs, preview_output, execution_start_time, execution_end_time, execution_error, status "
                "FROM jobs WHERE id = ?",
                (prompt_id,),
            )
            row = cursor.fetchone()
            if row:
                task = self._row_to_task(row)
                task_status = TaskStatus(row[11])
                return task_status, task
            return None
