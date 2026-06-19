import asyncio
import threading
import logging
from typing import Dict, List, Callable, Any, Type, TypeVar
from gateway.shared.interfaces import EventBus

T = TypeVar("T")
logger = logging.getLogger(__name__)


class InMemoryEventBus(EventBus):
    """线程与协程安全的内存事件总线。

    基于事件类类型实现发布与订阅。
    支持在任意后台线程中调用 publish，其回调执行会被安全地调度到 asyncio 事件循环的主线程中运行。
    """

    def __init__(self) -> None:
        self._subscribers: Dict[Type[Any], List[Callable[[Any], Any]]] = {}
        self._lock = threading.Lock()

    def subscribe(
        self, event_class: Type[T], callback: Callable[[T], Any]
    ) -> Callable[[], None]:
        """线程安全地订阅指定类型的事件类。"""
        with self._lock:
            if event_class not in self._subscribers:
                self._subscribers[event_class] = []
            self._subscribers[event_class].append(callback)  # type: ignore[arg-type]

        def unsubscribe() -> None:
            with self._lock:
                if event_class in self._subscribers:
                    try:
                        self._subscribers[event_class].remove(callback)  # type: ignore[arg-type]
                    except ValueError:
                        pass

        return unsubscribe

    def publish(self, event: object) -> None:
        """线程安全地发布事件实例。

        如果当前 asyncio 事件循环正在运行，所有回调将在事件循环的线程中执行，
        以保护非线程安全的领域模型及表示层连接。
        """
        event_class = type(event)
        with self._lock:
            if event_class not in self._subscribers:
                return
            callbacks = list(self._subscribers[event_class])

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = None

        def run_callbacks() -> None:
            for callback in callbacks:
                try:
                    callback(event)
                except Exception as e:
                    logger.error(f"Error in event callback: {e}")

        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(run_callbacks)
        else:
            run_callbacks()
