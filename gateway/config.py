import os
import shlex
from typing import List

# 根目录 (ComfyUI-pause-resume 文件夹路径)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_env(path: str) -> None:
    """加载指定路径的 .env 环境变量配置文件"""
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            # 忽略空行和以 # 开头的注释行
            if line and not line.startswith("#"):
                parts = line.split("=", 1)
                if len(parts) == 2:
                    k, v = parts[0].strip(), parts[1].strip()
                    # 去除值两端的引号
                    if len(v) >= 2 and (
                        (v.startswith('"') and v.endswith('"'))
                        or (v.startswith("'") and v.endswith("'"))
                    ):
                        v = v[1:-1]
                    os.environ[k] = v


class GatewayConfig:
    def __init__(self) -> None:
        # 网关启动时加载根目录下的 .env 配置文件
        load_env(os.path.join(BASE_DIR, ".env"))

        # 网关数据存储目录，通过 COMFYUI_GATEWAY_DATA_DIR 环境变量指定，默认为启动脚本同目录下的 gateway_data
        default_data_dir = os.path.join(BASE_DIR, "gateway_data")
        self.data_dir = os.environ.get("COMFYUI_GATEWAY_DATA_DIR", default_data_dir)
        if not os.path.isabs(self.data_dir):
            self.data_dir = os.path.abspath(os.path.join(BASE_DIR, self.data_dir))

        self.comfyui_port = int(os.environ.get("COMFYUI_PORT", 8188))
        self.proxy_port = self.comfyui_port
        self.idle_program = os.environ.get("COMFYUI_IDLE_PROGRAM", "")
        self.busy_program = os.environ.get("COMFYUI_BUSY_PROGRAM", "")
        self.idle_restart_timeout = int(os.environ.get("COMFYUI_IDLE_RESTART_SEC", 600))
        self.restart_delay_sec = int(os.environ.get("COMFYUI_RESTART_DELAY_SEC", 10))
        self.comfyui_extra_args = os.environ.get("COMFYUI_EXTRA_ARGS", "")
        self.proxy_host = os.environ.get("COMFYUI_HOST", "127.0.0.1")
        self.queue_type = os.environ.get("COMFYUI_QUEUE_TYPE", "sqlite").lower()

        # 剥离了监听端口和地址后的，传递给下游 ComfyUI 进程的参数列表
        self.downstream_args: List[str] = []
        self._parse_extra_args()

    def _parse_extra_args(self) -> None:
        """
        解析 COMFYUI_EXTRA_ARGS 以过滤并获取代理网关本身的监听 host 和 port。
        将非端口、非监听地址的其它命令行参数传递给下游进程使用。
        """
        extra_args_list = shlex.split(self.comfyui_extra_args)
        i = 0
        while i < len(extra_args_list):
            arg = extra_args_list[i]
            if arg == "--listen":
                if i + 1 < len(extra_args_list) and not extra_args_list[
                    i + 1
                ].startswith("-"):
                    self.proxy_host = extra_args_list[i + 1]
                    i += 2
                else:
                    self.proxy_host = "0.0.0.0"
                    i += 1
            elif arg.startswith("--listen="):
                self.proxy_host = arg.split("=", 1)[1]
                i += 1
            elif arg == "--port":
                if i + 1 < len(extra_args_list) and not extra_args_list[
                    i + 1
                ].startswith("-"):
                    self.proxy_port = int(extra_args_list[i + 1])
                    i += 2
                else:
                    i += 1
            elif arg.startswith("--port="):
                self.proxy_port = int(arg.split("=", 1)[1])
                i += 1
            else:
                self.downstream_args.append(arg)
                i += 1
