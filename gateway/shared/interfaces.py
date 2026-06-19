from abc import ABC, abstractmethod
from typing import List, Optional, Tuple, Callable, Dict, Any, TypeVar, Type
from .models import Task

T = TypeVar("T")


class EventBus(ABC):
    """网关内部的事件发布与订阅总线接口。"""

    @abstractmethod
    def subscribe(
        self, event_class: Type[T], callback: Callable[[T], Any]
    ) -> Callable[[], None]:
        """订阅指定类型的事件类，返回一个无参取消订阅的闭包函数。"""
        pass

    @abstractmethod
    def publish(self, event: object) -> None:
        """发布指定事件的实例，触发所有订阅了该事件类类型的回调。"""
        pass


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


class DownstreamClient(ABC):
    """代表下游 ComfyUI 服务生命周期的抽象控制客户端。"""

    @property
    @abstractmethod
    def downstream_ready(self) -> bool:
        """下游服务是否已就绪。"""
        pass

    @property
    @abstractmethod
    def downstream_port(self) -> int:
        """下游服务监听的物理端口。"""
        pass

    @abstractmethod
    def restart(self) -> None:
        """重启下游服务。"""
        pass

    @abstractmethod
    def on_sleep_prevention_changed(self, preventing: bool) -> None:
        """通知下游客户端阻止系统休眠的状态发生变化（以便管理超时重启等运行时状态）。"""
        pass

    @abstractmethod
    async def send_prompt(self, prompt_id: str, body: Dict[str, Any]) -> None:
        """向下游发送任务数据。如果发送失败，可能抛出 DownstreamError。"""
        pass

    @abstractmethod
    async def get_jobs(self, query_params: Dict[str, str]) -> List[Dict[str, Any]]:
        """从下游 ComfyUI 原生 API 获取历史作业列表。"""
        pass


class StateBroadcaster(ABC):
    """负责向客户端广播网关状态变更事件的抽象接口。"""

    @abstractmethod
    def register_ws_callback(self, callback: Callable[[], None]) -> None:
        """注册前端 WebSocket 状态广播回调。"""
        pass

    @abstractmethod
    def register_sse_callback(self, callback: Callable[[bool], None]) -> None:
        """注册 SSE 状态广播回调。"""
        pass

    @abstractmethod
    def notify_state_changed(self, paused: bool) -> None:
        """广播暂停/恢复状态变更事件。"""
        pass

    @abstractmethod
    def notify_status_changed(self) -> None:
        """广播队列任务数量等状态变更事件。"""
        pass


class TaskDispatcher(ABC):
    """负责向下一代发任务的抽象调度器接口。"""

    @abstractmethod
    def try_dispatch(self) -> None:
        """尝试向下一代发任务。"""
        pass


class SystemPowerController(ABC):
    """负责控制系统电源状态（例如休眠阻止）的抽象接口。"""

    @abstractmethod
    def prevent_sleep(self) -> None:
        """阻止系统进入休眠状态。"""
        pass

    @abstractmethod
    def allow_sleep(self) -> None:
        """恢复系统默认的休眠行为。"""
        pass


class Timer(ABC):
    """用于处理定时延时触发动作的抽象定时器接口。"""

    @abstractmethod
    def start_timeout(
        self, seconds: float, callback: Callable[[], None]
    ) -> Callable[[], None]:
        """在指定秒数后异步执行回调。

        返回一个无参取消函数（Callable[[], None]），调用该函数可取消此特定定时任务。
        """
        pass
