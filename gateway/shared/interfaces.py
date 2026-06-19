from abc import ABC, abstractmethod
from typing import List, Optional, Tuple
from .models import Task


class TaskQueueReader(ABC):
    """网关任务队列的只读查询接口，隔离了写操作，符合读写分离与最小接口原则。"""

    @abstractmethod
    def get_pending(self) -> List[Task]:
        """获取当前队列中所有等待处理的任务列表。"""
        pass

    @abstractmethod
    def get_running(self) -> List[Task]:
        """获取当前正在下游执行的任务列表。"""
        pass

    @abstractmethod
    def get_queue_snapshot(self) -> Tuple[List[Task], List[Task]]:
        """原子地同时获取 (running, pending) 任务列表快照，保证两者来自同一时刻的一致性视图。"""
        pass

    @abstractmethod
    def get_pending_count(self, limit: Optional[int] = None) -> int:
        """获取等待处理的任务数量，支持指定 limit 以防止在大队列下全扫描。"""
        pass

    @abstractmethod
    def get_running_count(self, limit: Optional[int] = None) -> int:
        """获取正在执行的任务数量。"""
        pass


class TaskQueueWriter(ABC):
    """网关任务队列的写操作接口，负责队列数据的增删改和生命周期管理。"""

    @abstractmethod
    def new_task_number(self) -> int:
        """分配生成一个新的唯一自增任务序号。"""
        pass

    @abstractmethod
    def add_task(self, task: Task) -> None:
        """添加新的待处理任务至任务队列。"""
        pass

    @abstractmethod
    def pop_task(self, skip: int = 0) -> Optional[Task]:
        """弹出指定偏移量的待处理任务，并将其更新标记为正在运行。"""
        pass

    @abstractmethod
    def requeue_running(self) -> None:
        """将当前正在运行的任务退回到待处理队列（恢复其原序号位置），并清空运行状态。"""
        pass

    @abstractmethod
    def requeue_running_if_exists(self) -> bool:
        """原子地将正在运行的任务放回队列。返回是否确实存在正在运行的任务并成功放回。"""
        pass

    @abstractmethod
    def clear_running(self) -> None:
        """物理清除所有正在运行状态的任务。"""
        pass

    @abstractmethod
    def clear_pending(self) -> None:
        """物理清除所有排队待处理的任务。"""
        pass

    @abstractmethod
    def delete_pending(self, prompt_ids: List[str]) -> None:
        """按 ID 物理删除队列中的指定待处理任务。"""
        pass

    @abstractmethod
    def close(self) -> None:
        """释放队列所占用的物理存储及连接资源。"""
        pass


class StateRepository(ABC):
    """负责网关运行时持久化状态（例如暂停/恢复状态）读写的仓储接口。"""

    @abstractmethod
    def get_paused(self) -> bool:
        """查询网关当前的暂停设置状态。"""
        pass

    @abstractmethod
    def set_paused(self, paused: bool) -> None:
        """持久化保存网关的暂停设置状态。"""
        pass

    @abstractmethod
    def close(self) -> None:
        """关闭仓储的物理连接资源。"""
        pass


class ProcessManager(ABC):
    """负责调度和管理网关在繁忙与空闲状态下需执行的外挂辅助程序（例如监控或挖矿进程）。"""

    @abstractmethod
    def update_state(self, is_busy: bool, ever_active: bool) -> None:
        """根据网关当前的业务状态，自动调度启停对应的外部进程。"""
        pass

    @abstractmethod
    def is_running(self) -> bool:
        """检测当前是否有正在运行的受控外部辅助进程。"""
        pass

    @abstractmethod
    def cleanup(self) -> None:
        """强制清理并终止所有由该管理器启动的外部子进程。"""
        pass
