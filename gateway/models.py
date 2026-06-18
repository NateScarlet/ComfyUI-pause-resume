import os
import json
import random
import sqlite3
import time
import threading
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Any, Optional, Sequence, Set, Dict, cast, Generator
from .config import BASE_DIR, GatewayConfig

logger = logging.getLogger(__name__)


class RawJSON(str):
    """标记一段已经是合法 JSON 的字符串，序列化时直接嵌入不做转义。

    类似 Go 语言的 json.RawMessage，配合 _RawJSONEncoder 使用。
    """

    pass


class RawJSONEncoder(json.JSONEncoder):
    """能将 RawJSON 对象作为原始 JSON 嵌入输出的自定义编码器

    配合 json.dump 使用：json.dump(data, f, cls=RawJSONEncoder)。
    配合 web.json_response 使用 raw_json_dumps 函数作为 dumps 参数。
    """

    def iterencode(self, o: Any, _one_shot: bool = False) -> Generator[str, None, None]:
        if isinstance(o, RawJSON):
            yield o
            return
        elif isinstance(o, dict):
            yield "{"
            first = True
            for key, value in cast(Dict[str, Any], o).items():
                if not first:
                    yield ", "
                first = False
                yield from self.iterencode(key)
                yield ": "
                yield from self.iterencode(value)
            yield "}"
        elif isinstance(o, list):
            yield "["
            first = True
            for item in cast(List[Any], o):
                if not first:
                    yield ", "
                first = False
                yield from self.iterencode(item)
            yield "]"
        else:
            yield from super().iterencode(o, _one_shot)


def raw_json_dumps(obj: Any, **kwargs: Any) -> str:
    """json.dumps 替代，将 RawJSON 对象作为原始 JSON 嵌入输出而非转义字符串"""
    return "".join(RawJSONEncoder(**kwargs).iterencode(obj))


@dataclass(frozen=True)
class Task:
    """队列中的一个任务，不可变。

    prompt 和 extra_data 以 JSON 字符串形式存储，避免在队列读写时做无意义的
    解析/序列化往返。仅在需要访问内部字段或通过 to_list() 对外输出时才按需解析。
    """

    number: float
    prompt_id: str
    prompt: RawJSON
    extra_data: RawJSON
    outputs_to_execute: Sequence[str]
    create_time: int

    def to_list(self) -> List[Any]:
        """转换为 ComfyUI /queue 接口期望的 5 项列表格式

        prompt 和 extra_data 是 _RawJSON 类型，配合 _raw_json_dumps
        或 _RawJSONEncoder 序列化时直接嵌入原始 JSON，避免解析再序列化。
        """
        return [
            self.number,
            self.prompt_id,
            self.prompt,
            self.extra_data,
            list(self.outputs_to_execute),
        ]


class TaskQueue(ABC):
    @abstractmethod
    def get_pending(self) -> List[Task]:
        """获取所有待处理任务"""
        pass

    @abstractmethod
    def get_running(self) -> List[Task]:
        """获取所有正在运行任务"""
        pass

    @abstractmethod
    def get_pending_count(self, limit: Optional[int] = None) -> int:
        """获取待处理任务数量，支持通过 limit 限制扫描深度以提升性能"""
        pass

    @abstractmethod
    def get_running_count(self, limit: Optional[int] = None) -> int:
        """获取正在运行任务数量，支持通过 limit 限制扫描深度以提升性能"""
        pass

    @abstractmethod
    def new_task_number(self) -> int:
        """分配一个新的唯一任务编号，每次调用保证返回不同的值"""
        pass

    @abstractmethod
    def add_task(self, task: Task) -> None:
        """添加新任务到待处理队列"""
        pass

    @abstractmethod
    def pop_task(self, skip: int = 0) -> Optional[Task]:
        """跳过前 skip 个待处理任务，将下一个移入运行队列并返回"""
        pass

    @abstractmethod
    def requeue_running(self) -> None:
        """将正在运行的任务放回待处理队列（按 number 恢复原位），并清空运行队列"""
        pass

    @abstractmethod
    def clear_running(self) -> None:
        """清空运行队列"""
        pass

    @abstractmethod
    def clear_pending(self) -> None:
        """清空待处理队列"""
        pass

    @abstractmethod
    def delete_pending(self, prompt_ids: List[str]) -> None:
        """删除指定的待处理任务"""
        pass

    @abstractmethod
    def close(self) -> None:
        """关闭队列资源"""
        pass


class JSONFileQueue(TaskQueue):
    def __init__(self, queue_file: str):
        self._queue_file = queue_file
        self._lock = threading.Lock()
        self._pending_queue: List[Task] = []
        self._queue_running: List[Task] = []
        self._next_task_number = 1
        self._load()

    @staticmethod
    def _parse_task_from_list(q: List[Any]) -> Optional[Task]:
        """从 JSON 反序列化的原始列表构造 Task，兼容缺少 create_time 的旧格式"""
        if len(q) < 5:
            return None
        create_time = int(q[5]) if len(q) > 5 else -1
        return Task(
            number=q[0],
            prompt_id=str(q[1]),
            prompt=RawJSON(json.dumps(q[2], ensure_ascii=False)),
            extra_data=RawJSON(json.dumps(q[3], ensure_ascii=False)),
            outputs_to_execute=q[4],
            create_time=create_time,
        )

    def _load(self) -> None:
        if os.path.exists(self._queue_file):
            try:
                with open(self._queue_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                raw_pending: list[list[Any]] = data.get("queue_pending", [])
                raw_running: list[list[Any]] = data.get("queue_running", [])

                # 去重并滤除异常结构
                seen_ids: Set[str] = set()
                tasks: List[Task] = []
                for q in raw_running + raw_pending:
                    task = self._parse_task_from_list(q)
                    if task is None:
                        continue
                    if task.prompt_id not in seen_ids:
                        seen_ids.add(task.prompt_id)
                        tasks.append(task)

                # 按 number 升序排序
                tasks.sort(key=lambda t: t.number)
                self._pending_queue = tasks
                self._queue_running = []

                if tasks:
                    # number 可能是正数、负数或浮点数，取绝对值最大值作为自增计数器基础
                    self._next_task_number = (
                        max(int(abs(float(t.number))) for t in tasks) + 1
                    )
            except Exception as e:
                logger.error(f"Failed to load queue.json: {e}")

    def _save(self) -> None:
        try:
            data = {
                "queue_running": [t.to_list() for t in self._queue_running],
                "queue_pending": [t.to_list() for t in self._pending_queue],
            }
            temp_file = self._queue_file + ".tmp"
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, cls=RawJSONEncoder)
            os.replace(temp_file, self._queue_file)
        except Exception as e:
            logger.error(f"Failed to save queue: {e}")

    def get_pending(self) -> List[Task]:
        with self._lock:
            return list(self._pending_queue)

    def get_running(self) -> List[Task]:
        with self._lock:
            return list(self._queue_running)

    def get_pending_count(self, limit: Optional[int] = None) -> int:
        with self._lock:
            cnt = len(self._pending_queue)
            if limit is not None:
                return min(cnt, limit)
            return cnt

    def get_running_count(self, limit: Optional[int] = None) -> int:
        with self._lock:
            cnt = len(self._queue_running)
            if limit is not None:
                return min(cnt, limit)
            return cnt

    def new_task_number(self) -> int:
        with self._lock:
            number = self._next_task_number
            self._next_task_number += 1
            return number

    def add_task(self, task: Task) -> None:
        with self._lock:
            self._pending_queue.append(task)
            self._pending_queue.sort(key=lambda t: t.number)
            self._save()

    def pop_task(self, skip: int = 0) -> Optional[Task]:
        with self._lock:
            if 0 <= skip < len(self._pending_queue):
                task = self._pending_queue.pop(skip)
                self._queue_running = [task]
                self._save()
                return task
            return None

    def requeue_running(self) -> None:
        with self._lock:
            if self._queue_running:
                task = self._queue_running[0]
                # frozen dataclass 字段完整，直接放回
                self._pending_queue.append(task)
                self._queue_running.clear()
                self._pending_queue.sort(key=lambda t: t.number)
                self._save()

    def clear_running(self) -> None:
        with self._lock:
            self._queue_running.clear()
            self._save()

    def clear_pending(self) -> None:
        with self._lock:
            self._pending_queue.clear()
            self._save()

    def delete_pending(self, prompt_ids: List[str]) -> None:
        with self._lock:
            to_delete = set(prompt_ids)
            self._pending_queue = [
                t for t in self._pending_queue if t.prompt_id not in to_delete
            ]
            self._save()

    def close(self) -> None:
        pass


class SQLiteQueue(TaskQueue):
    def __init__(self, db_path: str):
        self._db_path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            self._db_path, check_same_thread=False, timeout=30.0
        )
        self._init_db()

    def _init_db(self) -> None:
        with self._lock:
            # 开启 WAL 模式提高读写并发性能
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA synchronous=NORMAL;")

            cursor = self._conn.cursor()
            cursor.execute("PRAGMA user_version")
            db_version = cursor.fetchone()[0]

            SUPPORTED_VERSION = 2
            if db_version > SUPPORTED_VERSION:
                raise RuntimeError(
                    f"Database version error: The database version is {db_version}, "
                    f"but the current code only supports up to version {SUPPORTED_VERSION}. "
                    f"Please upgrade your application."
                )

            # 单向步进升级
            if db_version < 2:
                # 无论如何先创建 V2 结构表
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

                # 检查是否存在遗留的 v1 tasks 表以做平滑迁移
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
                    # 删除旧 tasks 表
                    cursor.execute("DROP TABLE tasks")
                    logger.info(
                        "✅ SQLite schema migration to version 2 completed successfully."
                    )

                # 4. 创建 metadata 元数据表
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS metadata (
                        key TEXT PRIMARY KEY,
                        value TEXT
                    )
                """)

                # 标记版本为 2
                cursor.execute("PRAGMA user_version = 2")
                self._conn.commit()

    @staticmethod
    def _row_to_task(row: Any) -> Task:
        """将 DB 行 (number, id, prompt, extra_data, outputs_to_execute, create_time) 转为 Task"""
        return Task(
            number=row[0],
            prompt_id=row[1],
            prompt=RawJSON(row[2]),
            extra_data=RawJSON(row[3]),
            outputs_to_execute=json.loads(row[4]),
            create_time=row[5],
        )

    def get_pending(self) -> List[Task]:
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
        _t_start = time.perf_counter()
        with self._lock:
            cursor = self._conn.cursor()
            if limit is not None:
                # 使用限制行数的局部扫描判断是否存在或是否满足数量
                cursor.execute(
                    "SELECT 1 FROM tasks_v2 WHERE status = 'pending' LIMIT ?", (limit,)
                )
                result = len(cursor.fetchall())
            else:
                cursor.execute("SELECT COUNT(*) FROM tasks_v2 WHERE status = 'pending'")
                row = cursor.fetchone()
                result = row[0] if row else 0
        _t_total = (time.perf_counter() - _t_start) * 1000
        if _t_total > 10:  # 超过 10ms 才记录，避免日志噪音
            logger.debug(
                f"SQLite get_pending_count(limit={limit}) = {result} took {_t_total:.1f}ms"
            )
        return result

    def get_running_count(self, limit: Optional[int] = None) -> int:
        _t_start = time.perf_counter()
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
        _t_total = (time.perf_counter() - _t_start) * 1000
        if _t_total > 10:
            logger.debug(
                f"SQLite get_running_count(limit={limit}) = {result} took {_t_total:.1f}ms"
            )
        return result

    def new_task_number(self) -> int:
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
        _t_start = time.perf_counter()
        with self._lock:
            cursor = self._conn.cursor()
            _t_serialize_start = time.perf_counter()
            outputs_str = json.dumps(task.outputs_to_execute, ensure_ascii=False)
            _t_serialize = (time.perf_counter() - _t_serialize_start) * 1000

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
            _t_commit_start = time.perf_counter()
            self._conn.commit()
            _t_commit = (time.perf_counter() - _t_commit_start) * 1000
        _t_total = (time.perf_counter() - _t_start) * 1000
        logger.debug(
            f"SQLite add_task {task.prompt_id}: "
            f"serialize={_t_serialize:.1f}ms commit={_t_commit:.1f}ms total={_t_total:.1f}ms"
        )

    def pop_task(self, skip: int = 0) -> Optional[Task]:
        with self._lock:
            cursor = self._conn.cursor()
            # 跳过前 skip 个待处理任务，取下一个
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
        with self._lock:
            cursor = self._conn.cursor()
            # number 未变，只需将状态改回 pending 即可恢复原位
            cursor.execute(
                "UPDATE tasks_v2 SET status = 'pending' WHERE status = 'running'"
            )
            self._conn.commit()

    def clear_running(self) -> None:
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("DELETE FROM tasks_v2 WHERE status = 'running'")
            self._conn.commit()

    def clear_pending(self) -> None:
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("DELETE FROM tasks_v2 WHERE status = 'pending'")
            self._conn.commit()

    def delete_pending(self, prompt_ids: List[str]) -> None:
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
        with self._lock:
            self._conn.close()


class GatewayStateManager:
    """持久化保存网关状态属性（如暂停/恢复状态）"""

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            self._db_path, check_same_thread=False, timeout=30.0
        )
        self._init_db()

    def _init_db(self) -> None:
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
        """获取网关暂停状态"""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("SELECT value FROM state WHERE key = 'paused'")
            row = cursor.fetchone()
            if row is not None:
                return row[0] == "true"
            return False

    def set_paused(self, paused: bool) -> None:
        """持久化保存网关的暂停状态"""
        with self._lock:
            val = "true" if paused else "false"
            self._conn.execute(
                "INSERT OR REPLACE INTO state (key, value) VALUES ('paused', ?)", (val,)
            )
            self._conn.commit()

    def close(self) -> None:
        """关闭数据库连接"""
        with self._lock:
            self._conn.close()


def transfer_tasks(source: TaskQueue, dest: TaskQueue) -> None:
    """将源队列中的所有任务转移到目标队列的待处理中（迁移时下游未启动，所有任务均视为待处理）"""
    pending = source.get_pending()
    running = source.get_running()

    for task in running:
        dest.add_task(task)
    for task in pending:
        dest.add_task(task)


def init_queue(config: GatewayConfig) -> TaskQueue:
    """根据配置初始化并实例化 TaskQueue，支持从旧 BASE_DIR 下 queue.json 数据迁移"""
    os.makedirs(config.data_dir, exist_ok=True)

    if config.queue_type == "json":
        logger.info("💾 Using JSONFileQueue.")
        queue: TaskQueue = JSONFileQueue(os.path.join(config.data_dir, "queue.json"))
    else:
        if config.queue_type != "sqlite":
            logger.warning(
                f"⚠️ Unknown queue type '{config.queue_type}'. Defaulting to 'sqlite'."
            )
        db_path = os.path.join(config.data_dir, "queue.db")
        logger.info(f"🗃️ Using SQLiteQueue. DB path: {db_path}")
        queue = SQLiteQueue(db_path)

    # 从旧位置迁移遗留的 JSON 队列数据
    old_json_path = os.path.join(BASE_DIR, "queue.json")
    if os.path.exists(old_json_path):
        logger.info(f"📦 Found legacy queue file {old_json_path}. Migrating...")
        try:
            legacy_queue = JSONFileQueue(old_json_path)
            transfer_tasks(legacy_queue, queue)
            legacy_queue.close()

            # 使用随机后缀避免覆盖已有备份
            suffix = "".join(random.choices("0123456789abcdef", k=8))
            bak_path = f"{old_json_path}~{suffix}"
            os.rename(old_json_path, bak_path)
            logger.info(
                f"✅ Migration successful! Legacy queue file renamed to {bak_path}"
            )
        except Exception as e:
            logger.error(f"❌ Migration failed: {e}")

    return queue
