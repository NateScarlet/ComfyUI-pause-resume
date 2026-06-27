import os
import sys
import socket
import threading
import subprocess
import asyncio
import logging
import aiohttp
from typing import Optional, Any, List, Dict, cast

from gateway.config import BASE_DIR, GatewayConfig
from gateway.shared.utils import raw_json_dumps
from gateway.shared.interfaces import (
    DownstreamClient,
    EventBus,
    ProcessManager,
)
from gateway.shared.exceptions import DownstreamError, DownstreamStartupTimeout
from gateway.shared.events import (
    DownstreamExecutingChangedEvent,
    DownstreamReadyChangedEvent,
    DownstreamCrashedEvent,
)

logger = logging.getLogger(__name__)


class ComfyUIDownstreamClient(DownstreamClient):
    """下游 ComfyUI 进程管理器及网络请求基础设施客户端。

    实现 DownstreamClient 接口。
    负责拉起进程、监听日志、转发网络请求、上报事件到事件总线。
    """

    def __init__(
        self,
        config: GatewayConfig,
        event_bus: EventBus,
        process_manager: ProcessManager,
    ):
        self.config = config
        self.event_bus = event_bus
        self.process_manager = process_manager

        # 运行时状态变量
        self._downstream_port: int = 0
        self.exiting: bool = False
        self.loop: Optional[asyncio.AbstractEventLoop] = None

        self._process: Optional[subprocess.Popen[str]] = None
        self._is_restarting: bool = False
        self._downstream_ready: bool = False

    @property
    def downstream_ready(self) -> bool:
        """下游服务是否已就绪（接口实现）。"""
        return self._downstream_ready

    @property
    def downstream_port(self) -> int:
        """下游服务监听的物理端口（接口实现）。"""
        return self._downstream_port

    def restart(self) -> None:
        """重启下游服务（接口实现）。"""
        if self.loop is not None and self.loop.is_running():
            self.loop.create_task(self.restart_downstream())

    async def send_prompt(self, prompt_id: str, body: Dict[str, Any]) -> None:
        """向下游发送任务数据（接口实现）。如果发送失败，抛出 DownstreamError。"""
        url = f"http://127.0.0.1:{self._downstream_port}/prompt"
        body_str = raw_json_dumps(body)
        headers = {"Content-Type": "application/json"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, data=body_str, headers=headers) as resp:
                    if resp.status == 200:
                        return
                    else:
                        txt = await resp.text()
                        raise DownstreamError(resp.status, txt)
        except DownstreamError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            raise DownstreamError(500, f"Network error: {str(e)}")

    async def get_jobs(self, query_params: Dict[str, str]) -> List[Dict[str, Any]]:
        """从下游 ComfyUI 原生 API 获取历史作业列表（接口实现）。"""
        downstream_jobs_url = f"http://127.0.0.1:{self._downstream_port}/api/jobs"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    downstream_jobs_url, params=query_params
                ) as resp:
                    if resp.status == 200:
                        resp_json = await resp.json()
                        if isinstance(resp_json, dict):
                            resp_dict = cast(Dict[str, Any], resp_json)
                            raw_jobs = resp_dict.get("jobs", [])
                            if isinstance(raw_jobs, list):
                                return cast(List[Dict[str, Any]], raw_jobs)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.error(f"Error fetching jobs from downstream: {e}")
        return []

    async def interrupt(self, prompt_id: Optional[str] = None) -> None:
        """向物理 ComfyUI 服务发送中断执行信号（接口实现）。"""
        url = f"http://127.0.0.1:{self._downstream_port}/interrupt"
        body = {"prompt_id": prompt_id} if prompt_id else {}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=body) as resp:
                    if resp.status != 200:
                        logger.warning(
                            f"Failed to send interrupt to downstream: status={resp.status}"
                        )
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.error(f"Network error sending interrupt to downstream: {e}")

    def start_downstream(self) -> None:
        """在系统空闲端口上拉起下游进程。"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            self._downstream_port = s.getsockname()[1]

        python_exe = sys.executable
        cmd = [
            python_exe,
            "-s",
            "ComfyUI/main.py",
            "--port",
            str(self._downstream_port),
        ]
        if self.config.downstream_args:
            cmd.extend(self.config.downstream_args)

        logger.info(f"🚀 Starting downstream ComfyUI on port {self._downstream_port}")
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUTF8"] = "1"
        self._process = subprocess.Popen(
            cmd,
            cwd=BASE_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            encoding="utf-8",
            errors="replace",
            env=env,
        )

        def log_reader(pipe: Any, is_stderr: bool) -> None:
            for line in iter(pipe.readline, ""):
                line = line.rstrip()
                if not line:
                    continue
                if is_stderr:
                    print(f"[{self._downstream_port}] STDERR: {line}", file=sys.stderr)
                else:
                    print(f"[{self._downstream_port}] STDOUT: {line}")

                # 仅仅发布物理状态日志对应的强类型事件到 EventBus
                if "got prompt" in line:
                    logger.debug("Detected 'got prompt' in downstream log")
                    self.event_bus.publish(
                        DownstreamExecutingChangedEvent(executing=True)
                    )
                elif "Prompt executed in" in line:
                    logger.debug("Detected 'Prompt executed in' in downstream log")
                    self.event_bus.publish(
                        DownstreamExecutingChangedEvent(executing=False)
                    )

        threading.Thread(
            target=log_reader, args=(self._process.stdout, False), daemon=True
        ).start()
        threading.Thread(
            target=log_reader, args=(self._process.stderr, True), daemon=True
        ).start()

    async def wait_downstream_ready(self) -> None:
        """以轮询 GET 的形式等待下游进程就绪。

        每轮循环动态构建 URL，以应对重启期间 _downstream_port 变化：
        老 wait 会自动追随新端口，最终在新进程就绪后返回。
        """
        logger.info(
            f"⌛ Waiting for downstream service (port {self._downstream_port})..."
        )
        async with aiohttp.ClientSession() as session:
            for _ in range(300):
                if self.exiting:
                    return
                # 每次循环动态构建 URL，应对重启期间 port 变化
                url = f"http://127.0.0.1:{self._downstream_port}"
                try:
                    async with session.get(
                        url, timeout=aiohttp.ClientTimeout(total=3)
                    ) as resp:
                        if resp.status == 200:
                            logger.info("✅ Downstream service ready")
                            self._downstream_ready = True
                            self.event_bus.publish(
                                DownstreamReadyChangedEvent(ready=True)
                            )
                            return
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    pass
                await asyncio.sleep(1)
        logger.error("❌ Downstream wait timeout")
        raise DownstreamStartupTimeout("Downstream wait timeout")

    async def restart_downstream(self) -> None:
        """优雅关闭下游老进程并拉起新的下游。"""
        if self._is_restarting:
            return
        self._is_restarting = True
        self._downstream_ready = False
        self.event_bus.publish(DownstreamReadyChangedEvent(ready=False))
        try:
            if self._process:
                try:
                    self._process.kill()
                    for _ in range(100):
                        if self._process.poll() is not None:
                            break
                        await asyncio.sleep(0.1)
                except (ProcessLookupError, OSError):
                    pass
                self._process = None
            self.event_bus.publish(DownstreamExecutingChangedEvent(executing=False))
            self.start_downstream()
            await self.wait_downstream_ready()
        finally:
            self._is_restarting = False

    async def monitor_downstream(self) -> None:
        """下游非预期崩溃监控。"""
        while not self.exiting:
            if self._process and not self._is_restarting:
                await asyncio.get_event_loop().run_in_executor(None, self._process.wait)
                if self.exiting or self._is_restarting:
                    continue
                exit_code = self._process.poll()
                logger.error(
                    f"❌ Downstream process exited unexpectedly with code {exit_code}. "
                    f"Restarting in {self.config.restart_delay_sec} seconds..."
                )
                self.event_bus.publish(DownstreamCrashedEvent())
                await asyncio.sleep(self.config.restart_delay_sec)
                await self.restart_downstream()
            else:
                await asyncio.sleep(1)

    async def shutdown(self) -> None:
        """停止下游子程序。"""
        self.exiting = True
        if self._process:
            try:
                logger.info("Cleaning up downstream process...")
                self._process.kill()
            except (ProcessLookupError, OSError):
                pass
