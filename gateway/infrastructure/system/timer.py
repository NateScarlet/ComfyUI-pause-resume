import asyncio
from typing import Callable
from gateway.shared.interfaces import Timer


class AsyncioTimer(Timer):
    """基于 asyncio 的无状态定时器实现。"""

    def start_timeout(
        self, seconds: float, callback: Callable[[], None]
    ) -> Callable[[], None]:
        """开启一个定时任务，达到指定秒数后执行回调。返回取消函数。"""
        task = asyncio.create_task(self._wait_and_call(seconds, callback))

        def cancel() -> None:
            if not task.done():
                task.cancel()

        return cancel

    async def _wait_and_call(
        self, seconds: float, callback: Callable[[], None]
    ) -> None:
        try:
            await asyncio.sleep(seconds)
            callback()
        except asyncio.CancelledError:
            pass
