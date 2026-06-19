import os
import json
import threading
import logging
from typing import List, Optional, Set, Any

from gateway.shared.interfaces import TaskQueueReader, TaskQueueWriter
from gateway.shared.models import Task, RawJSON
from gateway.shared.utils import RawJSONEncoder

logger = logging.getLogger(__name__)


class JSONFileQueue(TaskQueueReader, TaskQueueWriter):
    """基于本地 JSON 文件持久化实现的任务队列。"""

    def __init__(self, queue_file: str):
        self._queue_file = queue_file
        self._lock = threading.Lock()
        self._pending_queue: List[Task] = []
        self._queue_running: List[Task] = []
        self._next_task_number = 1
        self._load()

    @staticmethod
    def _parse_task_from_list(q: List[Any]) -> Optional[Task]:
        """从 JSON 反序列化的原生列表中恢复 Task 结构，兼容未记录 create_time 的旧历史数据。"""
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
        """从磁盘中加载并反序列化 JSON 队列。"""
        if os.path.exists(self._queue_file):
            try:
                with open(self._queue_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                raw_pending: list[list[Any]] = data.get("queue_pending", [])
                raw_running: list[list[Any]] = data.get("queue_running", [])

                seen_ids: Set[str] = set()
                tasks: List[Task] = []
                for q in raw_running + raw_pending:
                    task = self._parse_task_from_list(q)
                    if task is None:
                        continue
                    if task.prompt_id not in seen_ids:
                        seen_ids.add(task.prompt_id)
                        tasks.append(task)

                tasks.sort(key=lambda t: t.number)
                self._pending_queue = tasks
                self._queue_running = []

                if tasks:
                    self._next_task_number = (
                        max(int(abs(float(t.number))) for t in tasks) + 1
                    )
            except Exception as e:
                logger.error(f"Failed to load queue.json: {e}")

    def _save(self) -> None:
        """以原子覆盖的形式将当前的内存队列状态写入 JSON 文件中。"""
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
        """获取所有待处理任务列表。"""
        with self._lock:
            return list(self._pending_queue)

    def get_running(self) -> List[Task]:
        """获取所有正在运行任务列表。"""
        with self._lock:
            return list(self._queue_running)

    def get_pending_count(self, limit: Optional[int] = None) -> int:
        """获取待处理任务数量，支持通过 limit 限制扫描深度以提升性能。"""
        with self._lock:
            cnt = len(self._pending_queue)
            if limit is not None:
                return min(cnt, limit)
            return cnt

    def get_running_count(self, limit: Optional[int] = None) -> int:
        """获取正在运行任务数量，支持通过 limit 限制扫描深度以提升性能。"""
        with self._lock:
            cnt = len(self._queue_running)
            if limit is not None:
                return min(cnt, limit)
            return cnt

    def new_task_number(self) -> int:
        """分配生成一个新的唯一任务编号，每次调用保证返回不同的值。"""
        with self._lock:
            number = self._next_task_number
            self._next_task_number += 1
            return number

    def add_task(self, task: Task) -> None:
        """添加新任务到待处理队列。"""
        with self._lock:
            self._pending_queue.append(task)
            self._pending_queue.sort(key=lambda t: t.number)
            self._save()

    def pop_task(self, skip: int = 0) -> Optional[Task]:
        """弹出指定偏移量的待处理任务，并将其更新标记为正在运行。"""
        with self._lock:
            if 0 <= skip < len(self._pending_queue):
                task = self._pending_queue.pop(skip)
                self._queue_running = [task]
                self._save()
                return task
            return None

    def requeue_running(self) -> None:
        """将正在运行的任务放回待处理队列（恢复其原序号位置），并清空运行状态。"""
        with self._lock:
            if self._queue_running:
                task = self._queue_running[0]
                self._pending_queue.append(task)
                self._queue_running.clear()
                self._pending_queue.sort(key=lambda t: t.number)
                self._save()

    def clear_running(self) -> None:
        """物理清除所有正在运行状态的任务。"""
        with self._lock:
            self._queue_running.clear()
            self._save()

    def clear_pending(self) -> None:
        """物理清除所有排队待处理的任务。"""
        with self._lock:
            self._pending_queue.clear()
            self._save()

    def delete_pending(self, prompt_ids: List[str]) -> None:
        """按 ID 物理删除队列中的指定待处理任务。"""
        with self._lock:
            to_delete = set(prompt_ids)
            self._pending_queue = [
                t for t in self._pending_queue if t.prompt_id not in to_delete
            ]
            self._save()

    def close(self) -> None:
        """关闭队列资源。"""
        pass
