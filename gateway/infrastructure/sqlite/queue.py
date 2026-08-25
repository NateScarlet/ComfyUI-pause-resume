import sqlite3
import threading
import logging
import time
import json
from typing import List, Optional, Any, Tuple

from gateway.shared.interfaces import JobQueueReader, JobQueueWriter
from gateway.shared.models import Job, RawJSON, JobStatus, JobFilters, JobSummary
from gateway.shared.outputs_parser import extract_workflow_id
from gateway.infrastructure.sqlite.compression import CompressionBackend
from gateway.infrastructure.sqlite.dictionary import DictionaryStore

logger = logging.getLogger(__name__)

# 单批迁移的行数：控制单个事务规模，并在每批完成后输出进度日志
_MIGRATION_CHUNK_SIZE = 500

# WAL 高水位上限：checkpoint 完成且无人使用时自动截断到该值以内，
# 防止一次全库重建/VACUUM 留下上百 MB 的常驻残留文件
_WAL_SIZE_LIMIT_BYTES = 16 * 1024 * 1024


class SQLiteQueue(JobQueueReader, JobQueueWriter):
    """基于 SQLite3 数据库实现的任务队列，支持高并发读写，并集成了 WAL 日志模式。

    大文本列（prompt / extra_data / outputs_to_execute）以自描述压缩 blob 落库，
    压缩算法由外部注入的 CompressionBackend 决定；字典的注册表与轮换策略
    收敛在 DictionaryStore（dictionary.py），队列本体只做编排。
    """

    def __init__(
        self,
        db_path: str,
        *,
        backend: CompressionBackend,
        history_retention_days: int = 90,
        rotation_interval: int = 512,
        dict_sample_count: int = 300,
        dict_target_size: int = 65536,
    ):
        self._db_path = db_path
        self._backend = backend
        self._history_retention_days = history_retention_days
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            self._db_path, check_same_thread=False, timeout=30.0
        )
        # 字典轮换节奏（仿 EstimationService 的双桶模式：计数满即训练下一代并切换）
        self._dictionary = DictionaryStore(
            self._conn,
            backend,
            rotation_interval=rotation_interval,
            sample_count=dict_sample_count,
            target_size=dict_target_size,
        )
        self._init_db()

    # region 初始化与版本迁移
    def _init_db(self) -> None:
        """初始化数据库表结构，处理从低版本的单向平滑迁移。"""
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA synchronous=NORMAL;")
            # 外键按连接生效：让 jobs.dict_ref -> dictionaries 的引用完整性由数据库兜底
            self._conn.execute("PRAGMA foreign_keys=ON;")
            self._conn.execute(f"PRAGMA journal_size_limit={_WAL_SIZE_LIMIT_BYTES};")

            cursor = self._conn.cursor()
            cursor.execute("PRAGMA user_version")
            db_version = cursor.fetchone()[0]

            SUPPORTED_VERSION = 6
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

            if db_version < 6:
                self._migrate_v6_compress_large_columns(cursor)
                db_version = 6

    def _migrate_v6_compress_large_columns(self, cursor: sqlite3.Cursor) -> None:
        """v6：大文本列改存自描述压缩 blob，迁移完成后 VACUUM 收缩文件。"""
        logger.info(
            "🔧 Migrating SQLite schema to version 6: compressing large text columns..."
        )
        # 字典实体表：指纹即主键（内容寻址、不可变），先于 jobs 的外键列创建
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dictionaries (
                dict_ref INTEGER PRIMARY KEY,
                content  BLOB NOT NULL
            )
        """)
        # 不可变红线由数据库约束兜底：字典内容一经写入永不覆盖（DELETE 不受限，GC 使用）
        cursor.execute("""
            CREATE TRIGGER IF NOT EXISTS trg_dictionaries_immutable
            BEFORE UPDATE ON dictionaries
            BEGIN
                SELECT RAISE(ABORT, 'dictionaries are immutable');
            END
        """)
        try:
            cursor.execute(
                "ALTER TABLE jobs ADD COLUMN dict_ref INTEGER REFERENCES dictionaries(dict_ref)"
            )
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e).lower():
                raise

        # 用存量 TEXT 行的近期样本引导第一代字典（基础档后端会自然跳过）；
        # 注册表按需加载，断电续迁时自动识别已落库字典并走"同内容复用"分支
        cursor.execute(
            "SELECT extra_data FROM jobs "
            "WHERE typeof(extra_data) = 'text' AND length(extra_data) > 0 "
            "ORDER BY number DESC LIMIT ?",
            (self._dictionary.sample_count,),
        )
        samples = [str(row[0]).encode("utf-8") for row in cursor.fetchall()]
        self._dictionary.train_and_activate(samples)
        active = self._dictionary.active_ref
        if active is not None:
            logger.info(f"🧠 Bootstrapped compression dictionary {active:08x}.")
        else:
            logger.info(
                "🧠 Not enough samples to bootstrap a dictionary; starting at base tier."
            )

        # 只取仍含 TEXT 列的行（与引导查询同用 typeof 判据）：
        # 已迁移行由 SQL 过滤掉，中断后重跑幂等，续跑时也不再向应用传输无用行
        def encode_if_text(cell: Any) -> Any:
            # 单行 UPDATE 原子生效，正常不会出现行内混合态；
            # 非 TEXT 格（BLOB/NULL）原样透传，防御未来未知的过渡形态
            return self._dictionary.encode_cell(cell) if isinstance(cell, str) else cell

        migrated = 0
        last_id: str = ""
        while True:
            cursor.execute(
                "SELECT id, prompt, extra_data, outputs_to_execute FROM jobs "
                "WHERE id > ? AND ("
                "typeof(prompt) = 'text' OR typeof(extra_data) = 'text' "
                "OR typeof(outputs_to_execute) = 'text'"
                ") ORDER BY id LIMIT ?",
                (last_id, _MIGRATION_CHUNK_SIZE),
            )
            rows = cursor.fetchall()
            if not rows:
                break
            # 每行：(压缩后 prompt, 压缩后 extra_data, 压缩后 outputs, dict_ref, 行id)
            # dict_ref 为活跃指纹，无字典时写 NULL
            updates: List[
                Tuple[
                    Optional[bytes],
                    Optional[bytes],
                    Optional[bytes],
                    Optional[int],
                    str,
                ]
            ] = []
            for row_id, prompt, extra_data, outputs in rows:
                last_id = row_id
                updates.append(
                    (
                        encode_if_text(prompt),
                        encode_if_text(extra_data),
                        encode_if_text(outputs),
                        self._dictionary.active_ref,
                        row_id,
                    )
                )
            if updates:
                cursor.executemany(
                    "UPDATE jobs SET prompt = ?, extra_data = ?, outputs_to_execute = ?, dict_ref = ? "
                    "WHERE id = ?",
                    updates,
                )
            migrated += len(updates)
            self._conn.commit()
            logger.info(f"📦 v6 migration progress: {migrated} rows compressed...")

        # 字典回收的存在性检查走该覆盖索引，避免轮换期对大表做全表扫描
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_dict_ref ON jobs (dict_ref)"
        )

        cursor.execute("PRAGMA user_version = 6")
        self._conn.commit()
        logger.info(
            f"✅ SQLite schema migration to version 6 completed successfully "
            f"({migrated} rows compressed)."
        )
        # 数据已全部提交，VACUUM 将删除后的空闲页真正归还操作系统
        cursor.execute("VACUUM")

    # endregion

    # region 行映射与查询（Queries）
    def _row_to_job(self, row: Any) -> Job:
        """将 DB 查询出的行数据转化为不可变的 Task 实体（大文本列按行头解压）。"""
        outputs_text = self._dictionary.decode_cell(row[4])
        if outputs_text is None:
            # outputs_to_execute 是任务的必组成部分，缺失即数据损坏
            raise RuntimeError(f"任务 {row[1]} 缺失 outputs_to_execute，数据可能已损坏")
        return Job(
            number=row[0],
            prompt_id=row[1],
            prompt=RawJSON(self._dictionary.decode_cell(row[2])),
            extra_data=RawJSON(self._dictionary.decode_cell(row[3])),
            outputs_to_execute=json.loads(outputs_text),
            create_time=row[5],
            status=JobStatus(row[11]),
            outputs=RawJSON(self._dictionary.decode_cell(row[6])) if row[6] else None,
            preview_output=(
                RawJSON(self._dictionary.decode_cell(row[7])) if row[7] else None
            ),
            execution_start_time=row[8],
            execution_end_time=row[9],
            execution_error=(
                RawJSON(self._dictionary.decode_cell(row[10])) if row[10] else None
            ),
        )

    def _row_to_task_summary(self, row: Any) -> JobSummary:
        """将 DB 查询出的行数据转化为不可变的 TaskSummary 实体（大文本列按行头解压）。"""
        return JobSummary(
            number=row[0],
            prompt_id=row[1],
            status=JobStatus(row[2]),
            workflow_id=row[3],
            create_time=row[4],
            outputs=RawJSON(self._dictionary.decode_cell(row[5])) if row[5] else None,
            preview_output=(
                RawJSON(self._dictionary.decode_cell(row[6])) if row[6] else None
            ),
            execution_start_time=row[7],
            execution_end_time=row[8],
            execution_error=(
                RawJSON(self._dictionary.decode_cell(row[9])) if row[9] else None
            ),
        )

    def list(
        self,
        filter_by: Optional[JobFilters] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        desc: bool = False,
    ) -> List[Job]:
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
            result: List[Job] = []
            for row in cursor.fetchall():
                result.append(self._row_to_job(row))

            return result

    def get_summaries(
        self,
        filter_by: Optional[JobFilters] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        desc: bool = False,
    ) -> List[JobSummary]:
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

            # 摘要路径不拉取 prompt / extra_data / outputs_to_execute 大列，
            # 完全避免大 JSON 文本的读取与解压开销
            cursor.execute(
                "SELECT number, id, status, workflow_id, create_time, outputs, preview_output, execution_start_time, execution_end_time, execution_error "
                f"FROM jobs {where_clause} ORDER BY number {order_dir}{limit_offset_clause}",
                db_params,
            )
            result: List[JobSummary] = []
            for row in cursor.fetchall():
                result.append(self._row_to_task_summary(row))

            return result

    def count(
        self,
        filter_by: Optional[JobFilters] = None,
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
                f"SQLite count(filter_by={filter_by}, limit={limit}) = {result} took {t_total:.1f}ms"
            )
        return result

    def get(self, prompt_id: str) -> Optional[Job]:
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
                return self._row_to_job(row)
            return None

    # endregion

    # region 写入与状态变更（Commands）
    def new_number(self) -> int:
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

    def add(self, job: Job) -> None:
        """添加新任务到待处理队列中。"""
        t_start = time.perf_counter()

        # 提取 workflow_id 以便写入数据库物理列
        w_id = extract_workflow_id(job.extra_data)

        with self._lock:
            cursor = self._conn.cursor()
            t_serialize_start = time.perf_counter()
            outputs_str = json.dumps(job.outputs_to_execute, ensure_ascii=False)
            t_serialize = (time.perf_counter() - t_serialize_start) * 1000

            cursor.execute(
                "INSERT INTO jobs (id, number, prompt, extra_data, workflow_id, outputs_to_execute, status, create_time, dict_ref, outputs, preview_output, execution_start_time, execution_end_time, execution_error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL)",
                (
                    job.prompt_id,
                    job.number,
                    self._dictionary.encode_cell(job.prompt),
                    self._dictionary.encode_cell(job.extra_data),
                    w_id,
                    self._dictionary.encode_cell(outputs_str),
                    job.status.value,
                    job.create_time,
                    self._dictionary.active_ref,
                ),
            )
            t_commit_start = time.perf_counter()
            self._conn.commit()
            t_commit = (time.perf_counter() - t_commit_start) * 1000
        t_total = (time.perf_counter() - t_start) * 1000
        logger.debug(
            f"SQLite add_task {job.prompt_id}: "
            f"serialize={t_serialize:.1f}ms commit={t_commit:.1f}ms total={t_total:.1f}ms"
        )

    def save(self, job: Job) -> bool:
        """保存任务数据实体（如果已存在则更新，不存在则插入）。"""
        w_id = extract_workflow_id(job.extra_data)

        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("SELECT 1 FROM jobs WHERE id = ?", (job.prompt_id,))
            exists = cursor.fetchone() is not None

            outputs_str = json.dumps(job.outputs_to_execute, ensure_ascii=False)

            if exists:
                cursor.execute(
                    "UPDATE jobs SET number = ?, prompt = ?, extra_data = ?, workflow_id = ?, outputs_to_execute = ?, status = ?, create_time = ?, dict_ref = ?, outputs = ?, preview_output = ?, execution_start_time = ?, execution_end_time = ?, execution_error = ? WHERE id = ?",
                    (
                        job.number,
                        self._dictionary.encode_cell(job.prompt),
                        self._dictionary.encode_cell(job.extra_data),
                        w_id,
                        self._dictionary.encode_cell(outputs_str),
                        job.status.value,
                        job.create_time,
                        self._dictionary.active_ref,
                        str(job.outputs) if job.outputs else None,
                        str(job.preview_output) if job.preview_output else None,
                        job.execution_start_time,
                        job.execution_end_time,
                        str(job.execution_error) if job.execution_error else None,
                        job.prompt_id,
                    ),
                )
                changed = cursor.rowcount > 0
            else:
                cursor.execute(
                    "INSERT INTO jobs (id, number, prompt, extra_data, workflow_id, outputs_to_execute, status, create_time, dict_ref, outputs, preview_output, execution_start_time, execution_end_time, execution_error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        job.prompt_id,
                        job.number,
                        self._dictionary.encode_cell(job.prompt),
                        self._dictionary.encode_cell(job.extra_data),
                        w_id,
                        self._dictionary.encode_cell(outputs_str),
                        job.status.value,
                        job.create_time,
                        self._dictionary.active_ref,
                        str(job.outputs) if job.outputs else None,
                        str(job.preview_output) if job.preview_output else None,
                        job.execution_start_time,
                        job.execution_end_time,
                        str(job.execution_error) if job.execution_error else None,
                    ),
                )
                changed = True

            self._conn.commit()
        return changed

    def pop(self, skip: int = 0) -> Optional[Job]:
        """弹出指定偏移量的待处理任务，并将其更新标记为正在运行。"""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute(
                "SELECT number, id, prompt, extra_data, outputs_to_execute, create_time, outputs, preview_output, execution_start_time, execution_end_time, execution_error, status FROM jobs WHERE status = 'pending' ORDER BY number ASC LIMIT 1 OFFSET ?",
                (skip,),
            )
            row = cursor.fetchone()
            if row:
                target_id = row[1]
                cursor.execute(
                    "UPDATE jobs SET status = 'running' WHERE id = ?", (target_id,)
                )
                self._conn.commit()
                return self._row_to_job(row)
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

    def update_status(
        self,
        new_status: JobStatus,
        prompt_id: Optional[str] = None,
        filter_status: Optional[JobStatus] = None,
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
                JobStatus.COMPLETED,
                JobStatus.FAILED,
                JobStatus.CANCELLED,
            ):
                self._cleanup_expired_history()
                # 完成事件即字典轮换检查点：攒满一个周期才真正训练
                self._dictionary.rotate_if_due()
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

    # endregion

    # region 资源收尾
    def close(self) -> None:
        """释放数据库连接资源。"""
        with self._lock:
            self._conn.close()
