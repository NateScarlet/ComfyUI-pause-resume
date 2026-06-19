import os
import sys
import socket
import time
import json
import uuid
import asyncio
import logging
import datetime
import threading
import subprocess
import aiohttp
from aiohttp import web
from typing import Optional, Set, Dict, Any, cast

from .config import BASE_DIR, GatewayConfig
from .models import Task, RawJSON, TaskQueue, GatewayStateManager, raw_json_dumps
from .utils import PowerManagement
from .program import ExternalProgramManager

logger = logging.getLogger(__name__)


class Gateway:
    """网关生命周期、下游服务监控及核心队列调度的管理容器"""

    def __init__(
        self,
        config: GatewayConfig,
        state_manager: GatewayStateManager,
        queue: TaskQueue,
    ):
        self.config = config
        self.state_manager = state_manager
        self.queue = queue

        # 运行时状态变量
        self.paused: bool = state_manager.get_paused()
        self._downstream_executing: bool = False
        self.downstream_port: int = 0
        self._preventing_sleep: bool = False
        self._idle_start_time: Optional[float] = None
        self._ever_active: bool = False
        self._restart_after_idle_on_pause: bool = False

        # 实例化外部空闲与繁忙程序管理器
        self._program_manager = ExternalProgramManager(
            config.idle_program, config.busy_program
        )
        self._process: Optional[subprocess.Popen[str]] = None
        self._attempt_count: int = 0
        self._is_restarting: bool = False
        self.downstream_ready: bool = False
        self.exiting: bool = False
        self.loop: Optional[asyncio.AbstractEventLoop] = None

        # 保存活跃的 SSE 和 WebSocket 客户端连接
        self.sse_clients: Set[asyncio.Queue[str]] = set()
        self.ws_clients: Set[web.WebSocketResponse] = set()
        self.queue_lock = threading.Lock()

        # WS 状态广播防抖：避免每次提交任务都触发一次广播
        self._broadcast_ws_scheduled: bool = False
        self._broadcast_ws_debounce_sec: float = 0.1

        # 事件驱动的任务派发（由业务方法在状态变更时触发）
        self._dispatching: bool = False
        self._idle_timeout_task: Optional[asyncio.Task[None]] = None

    def _update_sleep_and_programs(self) -> None:
        """根据当前的执行状态和暂停状态，更新外部程序管理器及 Windows 的休眠阻止状态"""
        was_idle = self._idle_start_time is not None and not self._preventing_sleep

        # 如果已被暂停，即使有待处理任务且下游未执行，也不视为繁忙，以便允许系统休眠
        # 使用 limit=1 优化，快速检测是否有待处理任务即可
        has_pending = self.queue.get_pending_count(limit=1) > 0
        is_busy = self._downstream_executing or (not self.paused and has_pending)
        if is_busy:
            self._ever_active = True
        self._program_manager.update_state(is_busy, self._ever_active)

        scripts_running = self._program_manager.is_running()
        should_prevent_sleep = is_busy or scripts_running

        if should_prevent_sleep:
            self._idle_start_time = None
            if not self._preventing_sleep:
                logger.info("☕ Preventing system sleep")
                PowerManagement.prevent_sleep()
                self._preventing_sleep = True
        else:
            if self._idle_start_time is None:
                self._idle_start_time = time.time()

            if self._preventing_sleep:
                logger.info("💤 Allowing system sleep")
                PowerManagement.allow_sleep()
                self._preventing_sleep = False

        is_idle = self._idle_start_time is not None and not self._preventing_sleep
        if not was_idle and is_idle:
            self._on_idle_entered()
        elif was_idle and not is_idle:
            self._on_idle_exited()

    def _on_idle_entered(self) -> None:
        """进入闲置状态时：检查暂停后重启、启动空闲超时计时"""
        if self._restart_after_idle_on_pause:
            logger.info(
                "🔄 Pause-and-restart: system idle, "
                "restarting downstream immediately..."
            )
            self._restart_after_idle_on_pause = False
            asyncio.create_task(self.restart_downstream())
            return

        if self._ever_active and self.config.idle_restart_timeout > 0:
            self._idle_timeout_task = asyncio.create_task(self._idle_timeout_wait())

    def _on_idle_exited(self) -> None:
        """退出闲置状态时：取消空闲超时计时"""
        if self._idle_timeout_task:
            self._idle_timeout_task.cancel()
            self._idle_timeout_task = None

    async def _idle_timeout_wait(self) -> None:
        """等待闲置超时，超时后若仍闲置则重启下游"""
        idle_start = self._idle_start_time
        timeout = self.config.idle_restart_timeout
        try:
            await asyncio.sleep(timeout)
        except asyncio.CancelledError:
            return
        if self._idle_start_time == idle_start and not self._preventing_sleep:
            logger.info(
                f"🕒 Idle timeout ({timeout}s), "
                f"restarting downstream to free resources..."
            )
            await self.restart_downstream()

    def _broadcast_state(self) -> None:
        """将最新的网关暂停/恢复状态广播给所有连接的 SSE 客户端"""
        data = json.dumps({"paused": self.paused})
        for q in list(self.sse_clients):
            q.put_nowait(data)

    # ─── 下游执行状态管理（状态源 → 状态变更 → 业务决策）──────────

    def _set_downstream_executing(self, value: bool) -> None:
        """设置下游执行状态（线程安全），状态变更时触发业务逻辑

        log_reader 等状态源通过此方法汇报状态变更，不直接调用业务方法。
        """
        if self._downstream_executing == value:
            return
        self._downstream_executing = value
        if value:
            self._on_downstream_busy()
        else:
            self._on_downstream_idle()

    def _on_downstream_busy(self) -> None:
        """下游开始繁忙"""
        self._update_sleep_and_programs()

    def _on_downstream_idle(self) -> None:
        """下游完成执行：清理运行队列、更新状态、广播、触发派发"""
        with self.queue_lock:
            self.queue.clear_running()
            self._attempt_count = 0
        self._update_sleep_and_programs()
        self._broadcast_ws_status()
        self._try_dispatch()

    # ─── 业务方法：供 server 层调用 ────────────────────────────────

    def is_idle(self) -> bool:
        """系统是否处于闲置状态（无任务执行、无待处理、允许休眠）"""
        return not self._preventing_sleep and self._idle_start_time is not None

    def pause(self, restart_after_idle: bool = False) -> None:
        """暂停队列，可选是否在闲置后立即重启下游 ComfyUI

        若已暂停且闲置时再次以 restart_after_idle=True 调用，
        则立即触发重启，不等待新的闲置状态。
        """
        if restart_after_idle and self.paused and self.is_idle():
            logger.info(
                "🔄 Pause-and-restart: already paused and idle, restarting now..."
            )
            self._restart_after_idle_on_pause = False
            asyncio.create_task(self.restart_downstream())
            self._broadcast_state()
            return

        self.paused = True
        self.state_manager.set_paused(True)
        self._restart_after_idle_on_pause = restart_after_idle
        if restart_after_idle:
            logger.info("⏸️ Queue Paused (will restart downstream when idle)")
        else:
            logger.info("⏸️ Queue Paused")
        self._update_sleep_and_programs()
        self._broadcast_state()

    def resume(self) -> None:
        """恢复队列"""
        self.paused = False
        self.state_manager.set_paused(False)
        self._restart_after_idle_on_pause = False
        logger.info("▶️ Queue Resumed")
        self._update_sleep_and_programs()
        self._broadcast_state()
        self._try_dispatch()

    def _on_task_submitted(self) -> None:
        """任务提交到队列后的统一入口"""
        self._update_sleep_and_programs()
        self._broadcast_ws_status()
        self._try_dispatch()

    def _on_queue_modified(self) -> None:
        """队列被清除/删除后的统一入口"""
        self._update_sleep_and_programs()
        self._broadcast_ws_status()
        self._try_dispatch()

    # ─── 领域方法：供接口层调用 ──────────────────────────────────

    def add_task(
        self,
        prompt: dict[str, Any],
        extra_data: Optional[dict[str, Any]] = None,
        prompt_id: Optional[str] = None,
        number: Optional[float] = None,
        front: bool = False,
    ) -> dict[str, Any]:
        """领域方法：添加任务到队列，返回 prompt_id 和 number"""
        if extra_data is None:
            extra_data = {}
        if prompt_id is None:
            prompt_id = str(uuid.uuid4())

        with self.queue_lock:
            if number is not None:
                task_number = number
            else:
                task_number = float(self.queue.new_task_number())
                if front:
                    task_number = -task_number

            create_time = int(time.time() * 1000)
            task = Task(
                number=task_number,
                prompt_id=prompt_id,
                prompt=RawJSON(json.dumps(prompt, ensure_ascii=False)),
                extra_data=RawJSON(json.dumps(extra_data, ensure_ascii=False)),
                outputs_to_execute=[],
                create_time=create_time,
            )
            self.queue.add_task(task)

        self._on_task_submitted()
        return {"prompt_id": prompt_id, "number": task_number}

    def modify_queue(
        self,
        clear: bool = False,
        delete_ids: Optional[list[str]] = None,
    ) -> None:
        """领域方法：清空/删除队列中的任务"""
        with self.queue_lock:
            if clear:
                self.queue.clear_pending()
            if delete_ids:
                self.queue.delete_pending(delete_ids)
        self._on_queue_modified()

    def _try_dispatch(self) -> None:
        """线程安全地触发一次任务派发尝试"""
        if self.loop is not None:
            self.loop.call_soon_threadsafe(
                lambda: asyncio.create_task(self._try_send_task())
            )

    def _broadcast_ws_status(self) -> None:
        """
        主动广播最新的队列剩余数状态给所有活跃的前端 WebSocket 连接（带防抖）。
        本方法可以在外部线程中被安全调用（如 STDOUT/STDERR 读取线程）。
        """
        if self.loop is None:
            return
        self.loop.call_soon_threadsafe(self._schedule_broadcast_ws_status)

    def _schedule_broadcast_ws_status(self) -> None:
        """在事件循环中调度防抖的 WS 状态广播，合并短时间内的多次调用"""
        if self._broadcast_ws_scheduled:
            return
        self._broadcast_ws_scheduled = True

        async def _do_broadcast():
            await asyncio.sleep(self._broadcast_ws_debounce_sec)
            self._broadcast_ws_scheduled = False
            try:
                remaining = (
                    self.queue.get_pending_count() + self.queue.get_running_count()
                )
            except Exception:
                remaining = 0

            msg = {
                "type": "status",
                "data": {"status": {"exec_info": {"queue_remaining": remaining}}},
            }
            msg_str = json.dumps(msg)

            for ws in list(self.ws_clients):
                try:
                    if not ws.closed:
                        await ws.send_str(msg_str)
                except Exception:
                    pass

        asyncio.create_task(_do_broadcast())

    def start_downstream(self) -> None:
        """在随机可用端口上异步启动下游 ComfyUI 进程，并启动日志监听线程"""
        # 获取一个系统当前闲置的端口
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            self.downstream_port = s.getsockname()[1]

        # 初始启动时将可能残留的运行中任务放回待处理队列
        self.queue.requeue_running()

        python_exe = sys.executable
        cmd = [python_exe, "-s", "ComfyUI/main.py", "--port", str(self.downstream_port)]
        if self.config.downstream_args:
            cmd.extend(self.config.downstream_args)

        logger.info(f"🚀 Starting downstream ComfyUI on port {self.downstream_port}")
        # 复制当前进程的环境变量，并强制设置 Python 编码为 UTF-8，以解决 Windows 环境下 tqdm 进度条等特殊字符输出为乱码的问题
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUTF8"] = "1"
        self._process = subprocess.Popen(
            cmd,
            cwd=BASE_DIR,  # 重写为 BASE_DIR，防止在 gateway/data 下找不到 main.py
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
                    print(f"[{self.downstream_port}] STDERR: {line}", file=sys.stderr)
                else:
                    print(f"[{self.downstream_port}] STDOUT: {line}")

                # 纯状态源：仅汇报下游执行状态变更，不直接调用业务方法
                if "got prompt" in line:
                    self._set_downstream_executing(True)
                elif "Prompt executed in" in line:
                    self._set_downstream_executing(False)

        threading.Thread(
            target=log_reader, args=(self._process.stdout, False), daemon=True
        ).start()
        threading.Thread(
            target=log_reader, args=(self._process.stderr, True), daemon=True
        ).start()

    async def wait_downstream_ready(self) -> None:
        """向启动的下游服务发送 GET 请求，以轮询等待其完全就绪"""
        url = f"http://127.0.0.1:{self.downstream_port}"
        logger.info(f"⌛ Waiting for downstream service ({url})...")
        async with aiohttp.ClientSession() as session:
            for _ in range(300):
                try:
                    async with session.get(
                        url, timeout=aiohttp.ClientTimeout(total=3)
                    ) as resp:
                        if resp.status == 200:
                            logger.info("✅ Downstream service ready")
                            self.downstream_ready = True
                            return
                except Exception:
                    pass
                await asyncio.sleep(1)
        logger.error("❌ Downstream wait timeout")
        sys.exit(1)

    async def restart_downstream(self) -> None:
        """异步重启下游服务进程，并安全释放相关资源

        内部自带 _is_restarting 守卫，调用者无需自行检查。
        多次并发调用会被自动合并为一次重启。
        """
        if self._is_restarting:
            return
        self._is_restarting = True
        self.downstream_ready = False
        try:
            if self._process:
                try:
                    self._process.kill()
                    # 等待老进程退出以释放独占资源和显存
                    for _ in range(100):
                        if self._process.poll() is not None:
                            break
                        await asyncio.sleep(0.1)
                except Exception:
                    pass
                self._process = None
            self._downstream_executing = False
            self.start_downstream()
            await self.wait_downstream_ready()
        finally:
            self._is_restarting = False

    async def monitor_downstream(self) -> None:
        """事件驱动监控下游进程（用 process.wait() 替代轮询 process.poll()）"""
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
                with self.queue_lock:
                    if self.queue.get_running_count(limit=1) > 0:
                        self.queue.requeue_running()
                        self._attempt_count += 1
                self._broadcast_ws_status()
                await asyncio.sleep(self.config.restart_delay_sec)
                await self.restart_downstream()
            else:
                await asyncio.sleep(1)  # 进程尚未启动时短暂等待

    async def _try_send_task(self) -> None:
        """尝试从待处理队列中取出一个任务并发送至下游

        由业务方法在状态变更时触发，内置并发保护防止多次同时派发。
        """
        if self._dispatching or self.exiting:
            return
        self._dispatching = True
        try:
            if self.paused or self._downstream_executing or not self.downstream_ready:
                return

            with self.queue_lock:
                pending_count = self.queue.get_pending_count()
                if pending_count > 0:
                    skip = self._attempt_count % pending_count
                    task = self.queue.pop_task(skip)
                else:
                    task = None
                    self._attempt_count = 0

            if task is None:
                return

            # 准备派发一个任务！
            extra_data = json.loads(task.extra_data)
            body: dict[str, Any] = {
                "prompt": task.prompt,
                "prompt_id": task.prompt_id,
                "extra_data": task.extra_data,
            }
            if extra_data.get("client_id"):
                body["client_id"] = extra_data["client_id"]

            url = f"http://127.0.0.1:{self.downstream_port}/prompt"
            try:
                body_str = raw_json_dumps(body)
                headers = {"Content-Type": "application/json"}
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        url, data=body_str, headers=headers
                    ) as resp:
                        if resp.status == 200:
                            logger.info(
                                f"📤 Sent workflow {task.prompt_id} to downstream"
                            )
                            self._broadcast_ws_status()
                        else:
                            txt = await resp.text()
                            logger.error(
                                f"Failed to send workflow {task.prompt_id}: {resp.status} - {txt}"
                            )
                            # 如果遇到非法的工作流（例如 400-500 状态码），直接丢弃该任务并将其数据存盘备份以防死循环
                            with self.queue_lock:
                                if 400 <= resp.status <= 500:
                                    self.queue.clear_running()
                                    try:
                                        date_str = datetime.date.today().isoformat()
                                        dir_name = (
                                            f"{date_str}-{resp.status}-{task.prompt_id}"
                                        )
                                        failed_dir = os.path.join(
                                            self.config.data_dir,
                                            "failed_workflows",
                                            dir_name,
                                        )
                                        os.makedirs(failed_dir, exist_ok=True)

                                        # 保存报错信息
                                        with open(
                                            os.path.join(failed_dir, "error.txt"),
                                            "w",
                                            encoding="utf-8",
                                        ) as f:
                                            f.write(txt)

                                        # 保存原始请求数据
                                        with open(
                                            os.path.join(failed_dir, "request.json"),
                                            "w",
                                            encoding="utf-8",
                                        ) as f:
                                            json.dump(
                                                body,
                                                f,
                                                ensure_ascii=False,
                                                indent=2,
                                            )

                                        # 获取并单独存盘 workflow JSON
                                        workflow = None
                                        extra_pnginfo = extra_data.get("extra_pnginfo")
                                        if isinstance(extra_pnginfo, dict):
                                            extra_pnginfo_dict = cast(
                                                Dict[str, Any], extra_pnginfo
                                            )
                                            workflow = extra_pnginfo_dict.get(
                                                "workflow"
                                            )
                                        if not workflow:
                                            workflow = extra_data.get("workflow")

                                        if workflow:
                                            with open(
                                                os.path.join(
                                                    failed_dir, "workflow.json"
                                                ),
                                                "w",
                                                encoding="utf-8",
                                            ) as f:
                                                json.dump(
                                                    workflow,
                                                    f,
                                                    ensure_ascii=False,
                                                    indent=2,
                                                )
                                        rel_failed_dir = os.path.relpath(
                                            failed_dir, BASE_DIR
                                        ).replace(os.sep, "/")
                                        logger.info(
                                            f"💾 Failed workflow {task.prompt_id} saved to {rel_failed_dir}"
                                        )
                                    except Exception as save_err:
                                        logger.error(
                                            f"Failed to save failed workflow details: {save_err}"
                                        )
                                else:
                                    # 暂时性服务错误，放回待处理队列并跳过重试
                                    self.queue.requeue_running()
                                    self._attempt_count += 1
                            self._broadcast_ws_status()
            except Exception as e:
                logger.error(f"Error sending workflow: {e}")
                with self.queue_lock:
                    self.queue.requeue_running()
                self._broadcast_ws_status()
        finally:
            self._dispatching = False

    async def shutdown(self, runner: web.AppRunner) -> None:
        """在网关结束或拦截退出信号时，关闭外部服务，退出客户端并优雅清理所有连接和资源"""
        self.exiting = True

        logger.info("Closing active SSE connections...")
        for q in list(self.sse_clients):
            try:
                q.put_nowait("shutdown")
            except Exception:
                pass

        logger.info("Closing active WebSocket connections...")

        async def close_ws(ws: web.WebSocketResponse) -> None:
            try:
                if not ws.closed:
                    await ws.close(code=1001, message=b"Server shutting down")
            except Exception:
                pass

        if self.ws_clients:
            await asyncio.gather(
                *(close_ws(ws) for ws in list(self.ws_clients)), return_exceptions=True
            )

        try:
            await runner.cleanup()
        except Exception as e:
            logger.error(f"Error during runner cleanup: {e}")

        # 释放自身持有的资源
        if self.state_manager:
            try:
                self.state_manager.close()
            except Exception:
                pass
        if self.queue:
            try:
                self.queue.close()
            except Exception:
                pass
        if self._process:
            try:
                logger.info("Cleaning up downstream process...")
                self._process.kill()
            except Exception:
                pass
        if self._program_manager:
            try:
                logger.info("Cleaning up external programs...")
                self._program_manager.cleanup()
            except Exception:
                pass
        PowerManagement.allow_sleep()
