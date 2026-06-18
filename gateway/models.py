import os
import json
import sqlite3
import threading
import logging
from abc import ABC, abstractmethod
from typing import List, Any, Optional, Set
from .config import BASE_DIR, GatewayConfig

logger = logging.getLogger(__name__)

class TaskQueue(ABC):
    @abstractmethod
    def get_pending(self) -> List[List[Any]]:
        """获取所有待处理任务"""
        pass

    @abstractmethod
    def get_running(self) -> List[List[Any]]:
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
    def add_task(self, prompt_id: str, prompt: Any, extra_data: Any) -> int:
        """添加新任务到待处理队列，并返回分配的任务编号 number"""
        pass

    @abstractmethod
    def pop_task(self, idx: int) -> Optional[List[Any]]:
        """将指定索引的待处理任务移入运行队列，并返回该任务"""
        pass

    @abstractmethod
    def requeue_running(self, idx: int) -> None:
        """将正在运行的任务放回待处理队列的指定位置，并清空运行队列"""
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
        self.queue_file = queue_file
        self.lock = threading.Lock()
        self.pending_queue: List[List[Any]] = []
        self.queue_running: List[List[Any]] = []
        self.global_number = 1
        self._load()

    def _load(self) -> None:
        if os.path.exists(self.queue_file):
            try:
                with open(self.queue_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                pending = data.get("queue_pending", [])
                running = data.get("queue_running", [])
                
                self.pending_queue = running + pending
                self.queue_running = []
                
                # 去重并滤除异常结构
                seen_ids: Set[str] = set()
                new_q: List[List[Any]] = []
                for q in self.pending_queue:
                    if len(q) < 2:
                        continue
                    _id = str(q[1])
                    if _id not in seen_ids:
                        seen_ids.add(_id)
                        new_q.append(q)
                self.pending_queue = new_q
                
                if new_q:
                    self.global_number = max(int(q[0]) for q in new_q) + 1
            except Exception as e:
                logger.error(f"Failed to load queue.json: {e}")

    def _save(self) -> None:
        try:
            data = {
                "queue_running": self.queue_running,
                "queue_pending": self.pending_queue
            }
            temp_file = self.queue_file + ".tmp"
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
            os.replace(temp_file, self.queue_file)
        except Exception as e:
            logger.error(f"Failed to save queue: {e}")

    def get_pending(self) -> List[List[Any]]:
        with self.lock:
            return list(self.pending_queue)

    def get_running(self) -> List[List[Any]]:
        with self.lock:
            return list(self.queue_running)

    def get_pending_count(self, limit: Optional[int] = None) -> int:
        with self.lock:
            cnt = len(self.pending_queue)
            if limit is not None:
                return min(cnt, limit)
            return cnt

    def get_running_count(self, limit: Optional[int] = None) -> int:
        with self.lock:
            cnt = len(self.queue_running)
            if limit is not None:
                return min(cnt, limit)
            return cnt

    def add_task(self, prompt_id: str, prompt: Any, extra_data: Any) -> int:
        with self.lock:
            number = self.global_number
            self.global_number += 1
            item: List[Any] = [number, prompt_id, prompt, extra_data, []]
            self.pending_queue.append(item)
            self._save()
            return number

    def pop_task(self, idx: int) -> Optional[List[Any]]:
        with self.lock:
            if 0 <= idx < len(self.pending_queue):
                task = self.pending_queue.pop(idx)
                self.queue_running = [task]
                self._save()
                return task
            return None

    def requeue_running(self, idx: int) -> None:
        with self.lock:
            if self.queue_running:
                task = self.queue_running[0]
                insert_idx = min(idx, len(self.pending_queue))
                self.pending_queue.insert(insert_idx, task)
                self.queue_running.clear()
                self._save()

    def clear_running(self) -> None:
        with self.lock:
            self.queue_running.clear()
            self._save()

    def clear_pending(self) -> None:
        with self.lock:
            self.pending_queue.clear()
            self._save()

    def delete_pending(self, prompt_ids: List[str]) -> None:
        with self.lock:
            to_delete = set(prompt_ids)
            self.pending_queue = [q for q in self.pending_queue if q[1] not in to_delete]
            self._save()

    def close(self) -> None:
        pass


class SQLiteQueue(TaskQueue):
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30.0)
        self._init_db()

    def _init_db(self) -> None:
        with self.lock:
            # 开启 WAL 模式提高读写并发性能
            self.conn.execute("PRAGMA journal_mode=WAL;")
            self.conn.execute("PRAGMA synchronous=NORMAL;")
            
            # 创建 tasks 表
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    number INTEGER,
                    prompt TEXT,
                    extra_data TEXT,
                    outputs_to_execute TEXT,
                    status TEXT,
                    position REAL
                )
            """)
            # 创建元数据表（存储全局自增序列号等）
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            self.conn.commit()

    def get_pending(self) -> List[List[Any]]:
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT number, id, prompt, extra_data, outputs_to_execute FROM tasks WHERE status = 'pending' ORDER BY position ASC"
            )
            rows = cursor.fetchall()
            tasks: List[List[Any]] = []
            for row in rows:
                try:
                    prompt = json.loads(row[2])
                    extra_data = json.loads(row[3])
                    outputs = json.loads(row[4])
                    tasks.append([row[0], row[1], prompt, extra_data, outputs])
                except Exception as e:
                    logger.error(f"Failed to decode task from DB: {e}")
            return tasks

    def get_running(self) -> List[List[Any]]:
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT number, id, prompt, extra_data, outputs_to_execute FROM tasks WHERE status = 'running'"
            )
            rows = cursor.fetchall()
            tasks: List[List[Any]] = []
            for row in rows:
                try:
                    prompt = json.loads(row[2])
                    extra_data = json.loads(row[3])
                    outputs = json.loads(row[4])
                    tasks.append([row[0], row[1], prompt, extra_data, outputs])
                except Exception as e:
                    logger.error(f"Failed to decode running task from DB: {e}")
            return tasks

    def get_pending_count(self, limit: Optional[int] = None) -> int:
        with self.lock:
            cursor = self.conn.cursor()
            if limit is not None:
                # 使用限制行数的局部扫描判断是否存在或是否满足数量
                cursor.execute("SELECT 1 FROM tasks WHERE status = 'pending' LIMIT ?", (limit,))
                return len(cursor.fetchall())
            else:
                cursor.execute("SELECT COUNT(*) FROM tasks WHERE status = 'pending'")
                row = cursor.fetchone()
                return row[0] if row else 0

    def get_running_count(self, limit: Optional[int] = None) -> int:
        with self.lock:
            cursor = self.conn.cursor()
            if limit is not None:
                cursor.execute("SELECT 1 FROM tasks WHERE status = 'running' LIMIT ?", (limit,))
                return len(cursor.fetchall())
            else:
                cursor.execute("SELECT COUNT(*) FROM tasks WHERE status = 'running'")
                row = cursor.fetchone()
                return row[0] if row else 0

    def _get_next_global_number(self) -> int:
        cursor = self.conn.cursor()
        cursor.execute("SELECT value FROM metadata WHERE key = 'global_number'")
        row = cursor.fetchone()
        if row:
            return int(row[0])
        return 1

    def add_task(self, prompt_id: str, prompt: Any, extra_data: Any) -> int:
        with self.lock:
            cursor = self.conn.cursor()
            number = self._get_next_global_number()
            
            prompt_str = json.dumps(prompt, ensure_ascii=False)
            extra_data_str = json.dumps(extra_data, ensure_ascii=False)
            outputs_str = json.dumps([], ensure_ascii=False)
            
            # 获取当前最大 position，以保证新任务排在末尾
            cursor.execute("SELECT MAX(position) FROM tasks WHERE status = 'pending'")
            row = cursor.fetchone()
            max_pos = row[0] if row and row[0] is not None else 0.0
            new_pos = max_pos + 1.0
            
            cursor.execute(
                "INSERT INTO tasks (id, number, prompt, extra_data, outputs_to_execute, status, position) VALUES (?, ?, ?, ?, ?, 'pending', ?)",
                (prompt_id, number, prompt_str, extra_data_str, outputs_str, new_pos)
            )
            cursor.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES ('global_number', ?)", (str(number + 1),))
            self.conn.commit()
            return number

    def pop_task(self, idx: int) -> Optional[List[Any]]:
        with self.lock:
            cursor = self.conn.cursor()
            # 仅在数据库中定位当前偏移量的单个任务，避免拉取所有任务的大字段与反序列化开销
            cursor.execute(
                "SELECT id, number, prompt, extra_data, outputs_to_execute FROM tasks WHERE status = 'pending' ORDER BY position ASC LIMIT 1 OFFSET ?",
                (idx,)
            )
            row = cursor.fetchone()
            if row:
                target_id = row[0]
                cursor.execute("UPDATE tasks SET status = 'running' WHERE id = ?", (target_id,))
                self.conn.commit()
                
                try:
                    prompt = json.loads(row[2])
                    extra_data = json.loads(row[3])
                    outputs = json.loads(row[4])
                    return [row[1], row[0], prompt, extra_data, outputs]
                except Exception as e:
                    logger.error(f"Failed to decode popped task: {e}")
            return None

    def requeue_running(self, idx: int) -> None:
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT id FROM tasks WHERE status = 'running'")
            running_rows = cursor.fetchall()
            if not running_rows:
                return
                
            running_id = running_rows[0][0]
            
            # 获取当前所有 pending 任务的 ID 和 position
            cursor.execute("SELECT id, position FROM tasks WHERE status = 'pending' ORDER BY position ASC")
            pending_rows = cursor.fetchall()
            
            # 重新计算并更新 position
            if not pending_rows:
                new_pos = 1.0
            elif idx <= 0:
                new_pos = pending_rows[0][1] - 1.0
            elif idx >= len(pending_rows):
                new_pos = pending_rows[-1][1] + 1.0
            else:
                new_pos = (pending_rows[idx-1][1] + pending_rows[idx][1]) / 2.0
                
            cursor.execute("UPDATE tasks SET status = 'pending', position = ? WHERE id = ?", (new_pos, running_id))
            self.conn.commit()

    def clear_running(self) -> None:
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM tasks WHERE status = 'running'")
            self.conn.commit()

    def clear_pending(self) -> None:
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM tasks WHERE status = 'pending'")
            self.conn.commit()

    def delete_pending(self, prompt_ids: List[str]) -> None:
        if not prompt_ids:
            return
        with self.lock:
            cursor = self.conn.cursor()
            placeholders = ",".join("?" for _ in prompt_ids)
            cursor.execute(
                f"DELETE FROM tasks WHERE status = 'pending' AND id IN ({placeholders})",
                tuple(prompt_ids)
            )
            self.conn.commit()

    def close(self) -> None:
        with self.lock:
            self.conn.close()


class GatewayStateManager:
    """持久化保存网关状态属性（如暂停/恢复状态）"""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30.0)
        self._init_db()

    def _init_db(self) -> None:
        with self.lock:
            self.conn.execute("PRAGMA journal_mode=WAL;")
            self.conn.execute("PRAGMA synchronous=NORMAL;")
            
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS state (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            self.conn.commit()

    def get_paused(self) -> bool:
        """获取网关暂停状态"""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT value FROM state WHERE key = 'paused'")
            row = cursor.fetchone()
            if row is not None:
                return row[0] == 'true'
            return False

    def set_paused(self, paused: bool) -> None:
        """持久化保存网关的暂停状态"""
        with self.lock:
            val = 'true' if paused else 'false'
            self.conn.execute("INSERT OR REPLACE INTO state (key, value) VALUES ('paused', ?)", (val,))
            self.conn.commit()

    def close(self) -> None:
        """关闭数据库连接"""
        with self.lock:
            self.conn.close()


def migrate_json_to_sqlite(json_path: str, sqlite_queue: SQLiteQueue) -> None:
    """自动将 JSON 队列中的遗留数据导入 SQLite 中，并对原文件改名备份"""
    if not os.path.exists(json_path):
        return

    logger.info(f"📦 Found legacy queue file {json_path}. Migrating to SQLite...")
    try:
        legacy_queue = JSONFileQueue(json_path)
        pending = legacy_queue.get_pending()
        running = legacy_queue.get_running()

        with sqlite_queue.lock:
            cursor = sqlite_queue.conn.cursor()
            cursor.execute("DELETE FROM tasks")
            
            # 导入 running 任务
            for i, task in enumerate(running):
                number, prompt_id, prompt, extra_data, outputs = task
                prompt_str = json.dumps(prompt, ensure_ascii=False)
                extra_data_str = json.dumps(extra_data, ensure_ascii=False)
                outputs_str = json.dumps(outputs, ensure_ascii=False)
                pos = float(i + 1)
                cursor.execute(
                    "INSERT INTO tasks (id, number, prompt, extra_data, outputs_to_execute, status, position) VALUES (?, ?, ?, ?, ?, 'running', ?)",
                    (prompt_id, number, prompt_str, extra_data_str, outputs_str, pos)
                )

            # 导入 pending 任务
            for i, task in enumerate(pending):
                number, prompt_id, prompt, extra_data, outputs = task
                prompt_str = json.dumps(prompt, ensure_ascii=False)
                extra_data_str = json.dumps(extra_data, ensure_ascii=False)
                outputs_str = json.dumps(outputs, ensure_ascii=False)
                pos = float(i + 1)
                cursor.execute(
                    "INSERT INTO tasks (id, number, prompt, extra_data, outputs_to_execute, status, position) VALUES (?, ?, ?, ?, ?, 'pending', ?)",
                    (prompt_id, number, prompt_str, extra_data_str, outputs_str, pos)
                )

            # 记录最大的任务全局序号
            all_numbers = [t[0] for t in running + pending]
            max_num = max(all_numbers) if all_numbers else 0
            cursor.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES ('global_number', ?)", (str(max_num + 1),))
            sqlite_queue.conn.commit()

        # 重命名原 JSON 队列文件，防止再次触发迁移
        bak_path = json_path + ".bak"
        if os.path.exists(bak_path):
            os.remove(bak_path)
        os.rename(json_path, bak_path)
        logger.info(f"✅ Migration successful! Legacy queue file renamed to {bak_path}")
    except Exception as e:
        logger.error(f"❌ Migration failed: {e}")


def init_queue(config: GatewayConfig) -> TaskQueue:
    """根据配置初始化并实例化 TaskQueue，提供从旧当前目录中 queue.json 数据迁移的支持"""
    os.makedirs(config.data_dir, exist_ok=True)
    
    new_json_path = os.path.join(config.data_dir, "queue.json")
    old_json_path = os.path.join(BASE_DIR, "queue.json")
    
    if config.queue_type == "json":
        logger.info("💾 Using JSONFileQueue.")
        # 如果新路径没有，但旧路径有，迁移至新数据目录
        if not os.path.exists(new_json_path) and os.path.exists(old_json_path):
            logger.info(f"🚚 Moving legacy JSON queue from {old_json_path} to {new_json_path}...")
            try:
                with open(old_json_path, 'r', encoding='utf-8') as f:
                    data = f.read()
                with open(new_json_path, 'w', encoding='utf-8') as f:
                    f.write(data)
                
                # 在旧位置生成 .bak 备份
                bak_path = old_json_path + ".bak"
                if os.path.exists(bak_path):
                    os.remove(bak_path)
                os.rename(old_json_path, bak_path)
            except Exception as e:
                logger.error(f"Failed to migrate legacy JSON queue: {e}")
        return JSONFileQueue(new_json_path)
    else:
        if config.queue_type != "sqlite":
            logger.warning(f"⚠️ Unknown queue type '{config.queue_type}'. Defaulting to 'sqlite'.")
        db_path = os.path.join(config.data_dir, "queue.db")
        logger.info(f"🗃️ Using SQLiteQueue. DB path: {db_path}")
        sqlite_queue = SQLiteQueue(db_path)
        
        # 尝试从新目录的遗留 json 迁移
        if os.path.exists(new_json_path):
            migrate_json_to_sqlite(new_json_path, sqlite_queue)
            
        # 尝试从老根目录下的 json 迁移
        if os.path.exists(old_json_path):
            migrate_json_to_sqlite(old_json_path, sqlite_queue)
            
        return sqlite_queue
