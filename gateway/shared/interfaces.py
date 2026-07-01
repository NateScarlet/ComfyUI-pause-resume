from abc import ABC, abstractmethod
from typing import List, Optional, Tuple, Callable, Dict, Any, TypeVar, Type
from .models import Job, JobStatus, EstimationState, JobFilters, JobSummary

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


class JobQueueReader(ABC):
    """任务队列的只读查询接口，隔离了写操作，符合读写分离与最小接口原则。"""

    @abstractmethod
    def list(
        self,
        filter_by: Optional[JobFilters] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        desc: bool = False,
    ) -> List[Tuple[JobStatus, Job]]:
        """获取符合筛选条件的任务列表，支持分页限制和排序方向。"""
        pass

    @abstractmethod
    def get_summaries(
        self,
        filter_by: Optional[JobFilters] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        desc: bool = False,
    ) -> List[JobSummary]:
        """获取符合筛选条件的任务摘要列表，支持分页限制和排序方向。"""
        pass

    @abstractmethod
    def count(
        self,
        filter_by: Optional[JobFilters] = None,
        limit: Optional[int] = None,
    ) -> int:
        """获取符合筛选条件的任务数量；filter_by=None 时返回全部任务数量。"""
        pass

    @abstractmethod
    def get(self, prompt_id: str) -> Optional[Tuple[JobStatus, Job]]:
        """根据 ID 获取任务及其当前状态。如果不存在返回 None。"""
        pass


class JobQueueWriter(ABC):
    """任务队列的写操作接口，负责队列数据的增删改和生命周期管理。"""

    @abstractmethod
    def new_number(self) -> int:
        """分配生成一个新的唯一自增任务序号。"""
        pass

    @abstractmethod
    def add(self, job: Job) -> None:
        """添加新的待处理任务至任务队列。"""
        pass

    @abstractmethod
    def pop(self, skip: int = 0) -> Optional[Job]:
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
    def update_status(
        self,
        new_status: JobStatus,
        prompt_id: Optional[str] = None,
        filter_status: Optional[JobStatus] = None,
    ) -> bool:
        """更新符合条件的任务的状态。"""
        pass

    @abstractmethod
    def save(self, job: Job) -> bool:
        """保存任务的完整最新状态（支持新增或更新现有任务）。

        如果保存（或更新）成功返回 True，否则返回 False。
        """
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
    def get_estimation_state(self) -> Optional[EstimationState]:
        """获取预估时间状态，返回 None 表示无数据。"""
        pass

    @abstractmethod
    def save_estimation_state(self, state: EstimationState) -> None:
        """保存预估时间状态。"""
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
    async def send_prompt(self, prompt_id: str, body: Dict[str, Any]) -> None:
        """向下游发送任务数据。如果发送失败，可能抛出 DownstreamError。"""
        pass

    @abstractmethod
    async def get_jobs(self, query_params: Dict[str, str]) -> List[Dict[str, Any]]:
        """从下游 ComfyUI 原生 API 获取历史作业列表。"""
        pass

    @abstractmethod
    async def interrupt(self, prompt_id: Optional[str] = None) -> None:
        """向物理 ComfyUI 服务发送中断执行信号。"""
        pass

    @abstractmethod
    async def get_queue(self) -> Optional[Dict[str, Any]]:
        """从下游获取原生队列数据。"""
        pass

    @abstractmethod
    async def get_history(
        self, max_items: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """从下游获取原生历史数据。"""
        pass


class JobDispatcher(ABC):
    """负责向下一代发任务的抽象调度器接口。"""

    @abstractmethod
    def dispatch(self, skip: Optional[int]) -> None:
        """尝试向下一代发任务。"""
        pass

    @abstractmethod
    def handle_failed_job(self, job: Job, error_msg: str) -> None:
        """处理执行失败的坏任务（例如，备份至 failed_workflows 目录）。"""
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
