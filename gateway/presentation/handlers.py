import json
import logging
import traceback
import asyncio
import time
from pathlib import Path
import aiohttp
from aiohttp import web
from typing import List, Set, Union, cast, Dict, Any

from gateway.shared.utils import raw_json_dumps
from gateway.shared.interfaces import (
    TaskQueueReader,
    DownstreamClient,
    EventBus,
)
from gateway.shared.models import TaskStatus, TaskFilters, TaskSummary
from gateway.shared.events import StatusChangedEvent, StateChangedEvent
from gateway.shared.exceptions import GatewayError, TaskNotFoundError
from gateway.application.facade import AppFacade

_STATIC_DIR = Path(__file__).parent / "static"
_LOADING_HTML = (_STATIC_DIR / "loading.html").read_text(encoding="utf-8")
_INJECT_JS = (_STATIC_DIR / "inject.js").read_text(encoding="utf-8")

logger = logging.getLogger(__name__)


class GatewayHandlers:
    """HTTP/SSE/WebSocket 反向代理请求处理器，承载了表示层与 web 框架的具体交互。"""

    def __init__(
        self,
        app: AppFacade,
        downstream_service: DownstreamClient,
        queue_reader: TaskQueueReader,
        event_bus: EventBus,
    ):
        self._app = app
        self._downstream_service = downstream_service
        self._queue_reader = queue_reader
        self._event_bus = event_bus

        # 维护活跃的连接句柄
        self.sse_clients: Set[asyncio.Queue[str]] = set()
        self.ws_clients: Set[web.WebSocketResponse] = set()

        # 通过订阅 EventBus 事件来处理 WS 和 SSE 广播
        self._unsub_status = self._event_bus.subscribe(
            StatusChangedEvent, self._on_ws_broadcast_triggered
        )
        self._unsub_state = self._event_bus.subscribe(
            StateChangedEvent, lambda ev: self._on_sse_broadcast_triggered(ev.paused)
        )

        # WS 广播防抖控制变量
        self._broadcast_ws_scheduled: bool = False
        self._broadcast_ws_debounce_sec: float = 0.1
        self._latest_queue_remaining: int = 0

    def _on_ws_broadcast_triggered(self, ev: StatusChangedEvent) -> None:
        """应用层通知：触发防抖的 WS 队列剩余数量状态广播。"""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.call_soon_threadsafe(
                    self._schedule_broadcast_ws_status, ev.queue_remaining
                )
        except RuntimeError:
            pass

    def _schedule_broadcast_ws_status(self, remaining: int) -> None:
        """在事件循环中调度防抖广播逻辑。"""
        self._latest_queue_remaining = remaining
        if self._broadcast_ws_scheduled:
            return
        self._broadcast_ws_scheduled = True

        async def do_broadcast() -> None:
            await asyncio.sleep(self._broadcast_ws_debounce_sec)
            self._broadcast_ws_scheduled = False

            msg = {
                "type": "status",
                "data": {
                    "status": {
                        "exec_info": {"queue_remaining": self._latest_queue_remaining}
                    }
                },
            }
            msg_str = json.dumps(msg)

            for ws in list(self.ws_clients):
                try:
                    if not ws.closed:
                        await ws.send_str(msg_str)
                except (ConnectionResetError, aiohttp.WebSocketError):
                    pass

        try:
            asyncio.create_task(do_broadcast())
        except RuntimeError:
            pass

    def _on_sse_broadcast_triggered(self, paused: bool) -> None:
        """应用层通知：广播暂停状态给 SSE 客户端。"""
        data = json.dumps({"paused": paused})
        for q in list(self.sse_clients):
            try:
                q.put_nowait(data)
            except asyncio.QueueFull:
                pass

    async def handle_pause(self, request: web.Request) -> web.Response:
        """暂停队列接口。"""
        restart_after_idle = False
        if request.body_exists:
            try:
                body = await request.json()
            except (json.JSONDecodeError, TypeError):
                return web.Response(status=400, text="Bad Request: Invalid JSON body")
            if not isinstance(body, dict):
                return web.Response(
                    status=400, text="Bad Request: Body must be a JSON object"
                )
            body_dict = cast(Dict[str, Any], body)
            val = body_dict.get("restart_after_idle")
            if val is not None:
                if not isinstance(val, bool):
                    return web.Response(
                        status=400,
                        text="Bad Request: 'restart_after_idle' must be a boolean",
                    )
                restart_after_idle = val

        self._app.pause_queue.handle(restart_after_idle=restart_after_idle)
        return web.json_response({"status": "ok", "paused": True})

    async def handle_resume(self, request: web.Request) -> web.Response:
        """恢复队列接口。"""
        self._app.resume_queue.handle()
        return web.json_response({"status": "ok", "paused": False})

    async def handle_state(self, request: web.Request) -> web.Response:
        """查询暂停状态接口。"""
        paused = self._app.get_state.handle()
        return web.json_response({"paused": paused})

    async def handle_sse(self, request: web.Request) -> web.StreamResponse:
        """建立 SSE 连接推送暂停状态。"""
        response = web.StreamResponse(
            status=200,
            reason="OK",
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            },
        )
        await response.prepare(request)

        q: asyncio.Queue[str] = asyncio.Queue()
        self.sse_clients.add(q)
        try:
            await response.write(b"retry: 3000\n\n")
            paused = self._app.get_state.handle()
            initial_data = json.dumps({"paused": paused})
            await response.write(f"data: {initial_data}\n\n".encode("utf-8"))

            while True:
                data = await q.get()
                if data == "shutdown":
                    q.task_done()
                    break
                await response.write(f"data: {data}\n\n".encode("utf-8"))
                q.task_done()
        except (asyncio.CancelledError, ConnectionResetError):
            pass
        finally:
            self.sse_clients.discard(q)
        return response

    async def proxy_handler(
        self, request: web.Request
    ) -> Union[web.Response, web.StreamResponse]:
        """核心反向代理和拦截器，转发网关与下游进程通信并拦截核心 API。"""
        path = request.path
        if path == "/io.github.natescarlet.pause-resume" or path.startswith(
            "/io.github.natescarlet.pause-resume/"
        ):
            return web.Response(status=404, text="Not Found")

        method = request.method
        accept = request.headers.get("Accept", "")

        # 检查是否为网关可以独立处理的本地接口，放行其 503 检查
        is_local_jobs_api = (
            (method == "GET" and path in ("/api/jobs", "/api/jobs/"))
            or (
                method == "GET"
                and path.startswith("/api/jobs/")
                and len(path.strip("/").split("/")) == 3
            )
            or (
                method == "POST"
                and path.startswith("/api/jobs/")
                and path.endswith("/cancel")
            )
        )

        if not self._downstream_service.downstream_ready and not is_local_jobs_api:
            if method == "GET" and (
                path == "/" or path == "/index.html" or "text/html" in accept
            ):
                return web.Response(
                    content_type="text/html",
                    text=_LOADING_HTML,
                )
            return web.Response(
                status=503, text="Service Unavailable: Downstream is booting up"
            )

        method = request.method
        headers = dict(request.headers)
        downstream_url = f"http://127.0.0.1:{self._downstream_service.downstream_port}{request.path_qs}"

        # 1. 拦截任务提交：POST /prompt
        if method == "POST" and path in ("/prompt", "/api/prompt"):
            t_start = time.perf_counter()
            try:
                t_json_start = time.perf_counter()
                body = await request.json()
                body_dict = cast(Dict[str, Any], body)
                t_json = (time.perf_counter() - t_json_start) * 1000

                prompt = cast(Dict[str, Any], body_dict.get("prompt", {}))
                extra_data_raw = body_dict.get("extra_data", {})
                extra_data = (
                    dict(cast(Dict[str, Any], extra_data_raw))
                    if isinstance(extra_data_raw, dict)
                    else {}
                )
                prompt_id = body_dict.get("prompt_id")
                if prompt_id is not None:
                    prompt_id = str(prompt_id)

                number = None
                if "number" in body_dict:
                    try:
                        number = float(body_dict["number"])
                    except (ValueError, TypeError):
                        pass

                front = bool(body_dict.get("front", False))

                result = self._app.add_task.handle(
                    prompt=prompt,
                    extra_data=extra_data,
                    prompt_id=prompt_id,
                    number=number,
                    front=front,
                )

                t_total = (time.perf_counter() - t_start) * 1000
                logger.info(
                    f"📥 Intercepted workflow {result['prompt_id']} "
                    f"(json={t_json:.1f}ms total={t_total:.1f}ms)"
                )

                return web.json_response(
                    {
                        "prompt_id": result["prompt_id"],
                        "number": result["number"],
                        "node_errors": {},
                    }
                )
            except (GatewayError, ValueError, TypeError) as e:
                logger.error(f"Error processing {path}: {e}")
                traceback.print_exc()
                return web.Response(status=400, text=str(e))

        # 2. 拦截队列状态查询：GET /queue
        if method == "GET" and path in ("/queue", "/api/queue"):
            tasks = self._app.get_queue.handle()
            res_data = {
                "queue_running": [
                    t.to_list() for status, t in tasks if status == TaskStatus.RUNNING
                ],
                "queue_pending": [
                    t.to_list() for status, t in tasks if status == TaskStatus.PENDING
                ],
            }
            return web.json_response(res_data, dumps=raw_json_dumps)

        # 3. 拦截合并 jobs 查询：GET /api/jobs
        if method == "GET" and path in ("/api/jobs", "/api/jobs/"):
            t_total_start = time.perf_counter()
            query_params = {k: v for k, v in request.rel_url.query.items()}
            try:
                # 3.1. 解析并验证 status 参数
                valid_statuses = {
                    "pending",
                    "in_progress",
                    "completed",
                    "failed",
                    "cancelled",
                }
                status_param = query_params.get("status")
                if status_param:
                    status_filter = [
                        s.strip().lower() for s in status_param.split(",") if s.strip()
                    ]
                    statuses: List[TaskStatus] = []
                    invalid_statuses: List[str] = []
                    for sf in status_filter:
                        if sf == "pending":
                            statuses.append(TaskStatus.PENDING)
                        elif sf == "in_progress":
                            statuses.append(TaskStatus.RUNNING)
                        elif sf == "completed":
                            statuses.append(TaskStatus.COMPLETED)
                        elif sf == "failed":
                            statuses.append(TaskStatus.FAILED)
                        elif sf == "cancelled":
                            statuses.append(TaskStatus.CANCELLED)
                        else:
                            invalid_statuses.append(sf)
                    if invalid_statuses:
                        raise ValueError(
                            f"Invalid status value(s): {', '.join(invalid_statuses)}. "
                            f"Valid values: {', '.join(sorted(valid_statuses))}"
                        )
                else:
                    statuses = [
                        TaskStatus.PENDING,
                        TaskStatus.RUNNING,
                        TaskStatus.COMPLETED,
                        TaskStatus.FAILED,
                        TaskStatus.CANCELLED,
                    ]

                # 3.2. 解析并验证 limit / offset
                limit_val = None
                if query_params.get("limit"):
                    try:
                        limit_val = int(query_params["limit"])
                    except ValueError:
                        pass
                offset_val = 0
                if query_params.get("offset"):
                    try:
                        offset_val = int(query_params["offset"])
                    except ValueError:
                        pass

                # 3.3. 解析排序与 workflow_id
                sort_order = query_params.get("sort_order", "desc").lower()
                reverse = sort_order == "desc"
                workflow_id_param = query_params.get("workflow_id")

                # 3.4. 构造 TaskFilters 条件并调用应用层
                filter_by = TaskFilters(
                    statuses=statuses, workflow_id=workflow_id_param
                )

                # 获取任务列表并计时
                t_get_jobs_start = time.perf_counter()
                tasks_page = await self._app.get_jobs.handle(
                    filter_by=filter_by,
                    limit=limit_val,
                    offset=offset_val,
                    desc=reverse,
                )
                t_get_jobs = (time.perf_counter() - t_get_jobs_start) * 1000

                # 获取任务总数并计时
                t_get_job_count_start = time.perf_counter()
                total = await self._app.get_job_count.handle(filter_by=filter_by)
                t_get_job_count = (time.perf_counter() - t_get_job_count_start) * 1000

                # 3.5. 在表现层将 Domain Model 转换为符合 API 规范的格式并计时
                t_format_start = time.perf_counter()

                def make_job_dict(
                    summary: TaskSummary, status_str: str
                ) -> Dict[str, Any]:
                    return {
                        "id": summary.prompt_id,
                        "status": status_str,
                        "priority": summary.number,
                        "create_time": summary.create_time,
                        "outputs_count": 0,
                        "workflow_id": summary.workflow_id,
                    }

                jobs_json: List[Dict[str, Any]] = []
                for summary in tasks_page:
                    status_str = (
                        "in_progress"
                        if summary.status == TaskStatus.RUNNING
                        else summary.status.value
                    )
                    jobs_json.append(make_job_dict(summary, status_str))

                has_more = (offset_val + len(jobs_json)) < total

                res_data = {
                    "jobs": jobs_json,
                    "pagination": {
                        "offset": offset_val,
                        "limit": limit_val,
                        "total": total,
                        "has_more": has_more,
                    },
                }
                t_format_data = (time.perf_counter() - t_format_start) * 1000

                # 3.6. 计算总响应耗时并组装 Server-Timing 头部
                t_total = (time.perf_counter() - t_total_start) * 1000
                server_timing = (
                    f"get_jobs;dur={t_get_jobs:.2f}, "
                    f"get_job_count;dur={t_get_job_count:.2f}, "
                    f"format_data;dur={t_format_data:.2f}, "
                    f"total;dur={t_total:.2f}"
                )
                headers = {"Server-Timing": server_timing}

                return web.json_response(res_data, headers=headers)
            except ValueError as e:
                return web.json_response({"error": str(e)}, status=400)

        # 3.5. 拦截具体 job 取消：POST /api/jobs/{job_id}/cancel
        if (
            method == "POST"
            and path.startswith("/api/jobs/")
            and path.endswith("/cancel")
        ):
            parts = path.strip("/").split("/")
            if len(parts) == 4 and parts[3] == "cancel":
                job_id = parts[2]
                try:
                    cancelled = await self._app.cancel_job.handle(job_id)
                    return web.json_response({"cancelled": cancelled})
                except TaskNotFoundError:
                    return web.Response(status=404, text="Job not found")

        # 4. 拦截具体 job 详情查询：GET /api/jobs/{job_id}
        if method == "GET" and path.startswith("/api/jobs/"):
            parts = path.strip("/").split("/")
            if len(parts) == 3:
                job_id = parts[2]
                res_data = await self._app.get_job_detail.handle(job_id)
                if res_data is not None:
                    status, task = res_data
                    status_str = (
                        "in_progress" if status == TaskStatus.RUNNING else status.value
                    )
                    extra_data = json.loads(task.extra_data)
                    extra_pnginfo = extra_data.get("extra_pnginfo", {})
                    workflow = extra_pnginfo.get("workflow", {})
                    workflow_id = workflow.get("id")

                    job_detail = {
                        "id": task.prompt_id,
                        "status": status_str,
                        "priority": task.number,
                        "create_time": task.create_time,
                        "outputs_count": 0,
                        "workflow_id": workflow_id,
                        "workflow": {
                            "prompt": task.prompt,
                            "extra_data": task.extra_data,
                        },
                    }
                    return web.json_response(job_detail, dumps=raw_json_dumps)

        # 5. 拦截清空/删除操作：POST /queue (带 clear 或 delete)
        if method == "POST" and path in ("/queue", "/api/queue"):
            try:
                raw_body = await request.json()
            except (json.JSONDecodeError, TypeError):
                return web.Response(status=400, text="Bad Request: Invalid JSON body")
            body_json: Dict[str, Any] = {}
            if isinstance(raw_body, dict):
                body_json = cast(Dict[str, Any], raw_body)

            clear = bool(body_json.get("clear"))
            raw_delete = body_json.get("delete")
            delete_ids = None
            if isinstance(raw_delete, list):
                delete_ids = [str(item) for item in cast(List[Any], raw_delete)]

            self._app.modify_queue.handle(clear=clear, delete_ids=delete_ids)
            return web.Response(status=200)

        # 6. WebSocket 代理连接并拦截 status 推送
        if request.headers.get("Upgrade", "").lower() == "websocket":
            ws_server = web.WebSocketResponse()
            await ws_server.prepare(request)

            self.ws_clients.add(ws_server)

            async with aiohttp.ClientSession() as session:
                try:
                    async with session.ws_connect(downstream_url) as ws_client:

                        async def ws_forward(
                            ws_from: Union[
                                web.WebSocketResponse, aiohttp.ClientWebSocketResponse
                            ],
                            ws_to: Union[
                                web.WebSocketResponse, aiohttp.ClientWebSocketResponse
                            ],
                        ) -> None:
                            async for msg in ws_from:
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    data_str = msg.data
                                    if ws_from == ws_client:
                                        try:
                                            data_json = json.loads(data_str)
                                            if isinstance(data_json, dict):
                                                data_dict = cast(
                                                    Dict[str, Any], data_json
                                                )
                                                if data_dict.get("type") == "status":
                                                    remaining = self._queue_reader.get_task_count(
                                                        TaskFilters(
                                                            [
                                                                TaskStatus.PENDING,
                                                                TaskStatus.RUNNING,
                                                            ]
                                                        )
                                                    )
                                                    data_payload = data_dict.get("data")
                                                    if isinstance(data_payload, dict):
                                                        data_payload_dict = cast(
                                                            Dict[str, Any], data_payload
                                                        )
                                                        status_info = (
                                                            data_payload_dict.get(
                                                                "status"
                                                            )
                                                        )
                                                        if isinstance(
                                                            status_info, dict
                                                        ):
                                                            status_info_dict = cast(
                                                                Dict[str, Any],
                                                                status_info,
                                                            )
                                                            exec_info = (
                                                                status_info_dict.get(
                                                                    "exec_info"
                                                                )
                                                            )
                                                            if isinstance(
                                                                exec_info, dict
                                                            ):
                                                                exec_info_dict = cast(
                                                                    Dict[str, Any],
                                                                    exec_info,
                                                                )
                                                                exec_info_dict[
                                                                    "queue_remaining"
                                                                ] = remaining
                                                                data_str = json.dumps(
                                                                    data_dict
                                                                )
                                        except (json.JSONDecodeError, TypeError):
                                            pass
                                    await ws_to.send_str(data_str)
                                elif msg.type == aiohttp.WSMsgType.BINARY:
                                    await ws_to.send_bytes(msg.data)
                                elif msg.type == aiohttp.WSMsgType.PING:
                                    await ws_to.ping()
                                elif msg.type == aiohttp.WSMsgType.PONG:
                                    await ws_to.pong()
                                elif msg.type == aiohttp.WSMsgType.CLOSE:
                                    await ws_to.close()
                                    break

                        t1 = asyncio.create_task(ws_forward(ws_server, ws_client))
                        t2 = asyncio.create_task(ws_forward(ws_client, ws_server))
                        await asyncio.gather(t1, t2)
                except ConnectionResetError:
                    # 客户端正常关闭页面时，传输层已关闭，写入失败属于正常行为
                    pass
                except (
                    aiohttp.ClientError,
                    ConnectionResetError,
                    asyncio.TimeoutError,
                ) as e:
                    logger.error(f"WebSocket Error: {e}")
                finally:
                    self.ws_clients.discard(ws_server)
            return ws_server

        # 7. 拦截主页加载以动态注入 JS 扩展按钮
        if method == "GET" and (path == "/" or path == "/index.html"):
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(downstream_url, headers=headers) as resp:
                        body = await resp.read()
                        html = body.decode("utf-8", errors="replace")

                        injection = (
                            '\n<script type="module">\n' + _INJECT_JS + "\n</script>\n"
                        )
                        if "</body>" in html:
                            html = html.replace("</body>", injection + "</body>")
                        else:
                            html += injection

                        resp_headers = {
                            k: v
                            for k, v in resp.headers.items()
                            if k.lower()
                            not in (
                                "content-type",
                                "content-length",
                                "content-encoding",
                            )
                        }
                        return web.Response(
                            body=html,
                            status=resp.status,
                            headers=resp_headers,
                            content_type=resp.content_type,
                        )
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    return web.Response(status=502, text=f"Bad Gateway: {e}")

        # 8. 默认普通代理
        try:
            if request.body_exists:
                body = await request.read()
            else:
                body = None

            async with aiohttp.ClientSession() as session:
                async with session.request(
                    method, downstream_url, headers=headers, data=body
                ) as resp:
                    proxy_resp = web.StreamResponse(
                        status=resp.status, headers=resp.headers
                    )
                    await proxy_resp.prepare(request)
                    async for chunk in resp.content.iter_any():
                        await proxy_resp.write(chunk)
                    await proxy_resp.write_eof()
                    return proxy_resp
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            return web.Response(status=502, text=f"Bad Gateway: {e}")

    def shutdown(self) -> None:
        """关闭所有订阅及 SSE 和 WS 连接。"""
        if hasattr(self, "_unsub_status"):
            self._unsub_status()
        if hasattr(self, "_unsub_state"):
            self._unsub_state()

        for q in list(self.sse_clients):
            try:
                q.put_nowait("shutdown")
            except asyncio.QueueFull:
                pass

        async def close_ws(ws: web.WebSocketResponse) -> None:
            try:
                if not ws.closed:
                    await ws.close(code=1001, message=b"Server shutting down")
            except (ConnectionResetError, RuntimeError):
                pass

        async def close_all_ws() -> None:
            if self.ws_clients:
                await asyncio.gather(
                    *(close_ws(ws) for ws in list(self.ws_clients)),
                    return_exceptions=True,
                )

        if self.ws_clients:
            try:
                asyncio.create_task(close_all_ws())
            except RuntimeError:
                pass
