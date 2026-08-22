[**English**](README.md)

# ComfyUI 可暂停队列代理网关

为 ComfyUI 添加暂停/恢复队列控制的前置代理网关，队列状态在重启后自动恢复。

## 功能特点

- **自动恢复**：启动时自动恢复上次暂停时保存的队列
- **队列备份**：实时备份队列，防止意外中断导致任务丢失
- **即时入列**：任务提交时不校验，立即入列返回 — 校验延迟到实际执行前才进行，提交瞬间完成
- **崩溃恢复**：自动重试可恢复的派发错误；崩溃后自动将运行中任务重新入队（单个任务最多重试 3 次后永久失败）
- **空闲重启**：可选配置，队列空闲超时后自动重启下游 ComfyUI 进程以释放资源
- **系统托盘**（需 `pip install .[tray]`）：动态图标显示队列数量与预估时间扇形进度；右键菜单暂停/恢复/重启/退出；显示变化时自动恢复图标
- **电源管理**：有任务运行或外部脚本执行时自动阻止系统休眠
- **外部程序挂钩**：队列繁忙/空闲时自动启动用户指定程序（如 GPU 监控、风扇控制、矿工程序），状态切换时自动停止
- **进程管理**：完善的进程启动、停止和监控机制
- **灵活配置**：支持 `.env` 文件和环境变量配置（如 `COMFYUI_PORT`, `COMFYUI_EXTRA_ARGS`）

## 安装

### 方式一：部署脚本

```powershell
.\scripts\deploy.ps1 -TargetDir "C:\ComfyUI"
```

脚本会将 `start.cmd` 和 `gateway/` 目录复制到目标目录，并校验 Python 运行时是否存在。

### 方式二：手动复制

将 `start.cmd` 和 `gateway/` 文件夹复制到便携版 ComfyUI 的根目录中（与 `python_embeded/` 同级）。

## 使用

```cmd
start.cmd
```

启动后界面上会出现暂停/恢复按钮。配置可通过 `.env` 文件或环境变量调整。

## 文件说明

- `start.cmd` - 启动脚本，直接运行 `gateway/__main__.py` 启动代理网关和 ComfyUI 本身，支持直接 UI 上控制队列暂停
- `gateway/` - 代理网关的核心业务包目录
- `gateway_data/` - 默认的数据存储目录（自动生成，可通过环境变量修改），其中包含：
  - `queue.db` - SQLite 队列数据库文件（启用 WAL 模式）
  - `state.db` - SQLite 网关状态数据库文件，用于持久化暂停/恢复等运行状态和任务预估时间数据
  - `queue.json` - 传统 JSON 格式的保存队列文件（禁用 SQLite 队列时自动生成）
  - `queue.json.tmp` - 队列保存时产生的临时文件
  - `failed_workflows/` - 保存提交失败（如 400-500 错误）的任务信息的目录
- `queue.json~<随机后缀>` - 旧版本 JSON 队列数据自动迁移到新数据目录后的备份文件（生成于根目录）

## 配置选项

### 环境变量与 .env

支持在脚本同目录下创建 `.env` 文件或直接设置环境变量：

- `COMFYUI_PORT`: 服务端口（默认 `8188`）
- `COMFYUI_EXTRA_ARGS`: 传递给 ComfyUI 的额外参数（例如 `--preview-method auto`）
- `COMFYUI_RESTART_DELAY_SEC`: 进程异常退出后重启延迟（秒，默认 `10`）
- `COMFYUI_STARTUP_WAIT_SEC`: 下游启动/重启期间，API 请求最多等待其就绪的秒数，超时才返回错误（默认 `600`）
- `COMFYUI_IDLE_RESTART_SEC`: 队列空闲后强制重启服务的超时时间（秒，默认 `600`，设置为 0 则禁用）
- `COMFYUI_IDLE_PROGRAM`: 空闲时启动的程序路径（例如矿工程序；仅在队列自然排空且未暂停时启动，有新任务或手动暂停时会自动停止）
- `COMFYUI_BUSY_PROGRAM`: 繁忙时启动的程序路径（例如 GPU 监控或风扇控制程序，在空闲或暂停时会自动停止）
- `COMFYUI_PAUSE_PROGRAM`: 手动暂停期间启动的程序路径（例如把资源让给游戏或其他占用 GPU 的程序，恢复队列后自动停止）。设置为与 `COMFYUI_IDLE_PROGRAM` 相同的命令即可让闲置程序在暂停期间继续运行
- `COMFYUI_QUEUE_TYPE`: 队列实现类型，支持 `sqlite`（默认值，启用 WAL 模式，推荐）或 `json`（传统 JSONFile 队列实现）
- `COMFYUI_GATEWAY_DATA_DIR`: 网关数据存储目录（默认值 `gateway_data`，支持绝对路径或相对路径，相对路径会相对于启动脚本所在根目录解析）
- `COMFYUI_HOST`: 网关监听地址（默认 `127.0.0.1`）
- `COMFYUI_HISTORY_RETENTION_DAYS`: 已完成/失败/已取消任务的记录保留天数（默认 `90`）
- `COMFYUI_LANG`: 强制指定界面语言（`zh` 或 `en`），默认自动检测系统区域
- `GATEWAY_DEBUG`: 设为 `true` 启用详细调试日志
- `COMFYUI_ESTIMATION_BUCKET_CAPACITY`: 预估时间桶容量（默认 `100`），控制双桶轮换算法中每个桶的任务记录数量上限

## 技术说明

此实现使用 HTTP API 进行队列的保存和恢复，相比 [yara](https://github.com/Satellile/yara) 的方案，能够完整保存工作流的 `extra_data` 信息，确保队列恢复的准确性。

## 注意事项

1. `start.cmd` 和 `gateway/` 需要部署到 ComfyUI 便携版根目录（使用 `scripts\deploy.ps1` 或手动复制）
2. 暂停操作会等待当前任务完成。如需放弃当前进度：可使用原生中断（正在执行的任务被丢弃，重启后不会重新执行），或 Ctrl+C / 托盘点退出（正在执行的任务将在下次启动后重新执行）
3. 如果进程异常退出，脚本会自动尝试重启
4. 提交新任务时会跳过校验总是返回成功，实际校验将延迟到执行前，有错误的工作流（例如遇到 400-500 错误时）会被保存至数据存储目录下的 `failed_workflows/` 目录下（包括错误信息、原始请求数据以及工作流 JSON）供排查，并从队列中丢弃。
5. GET /queue 会总是返回空的 outputs_to_execute， 因为现在收到任务时没有立即解析
6. 外挂辅助程序的运行不影响下游自动重启：即使 `COMFYUI_IDLE_PROGRAM` 等外挂程序正在运行，空闲超时重启（`COMFYUI_IDLE_RESTART_SEC`）与"暂停等待重启"仍会正常触发下游重启以释放显存
7. 手动暂停期间仅运行 `COMFYUI_PAUSE_PROGRAM` 指定的程序（未配置则不运行任何外挂程序）；将其配置成与 `COMFYUI_IDLE_PROGRAM` 相同的命令时，同一程序进程会在暂停/恢复等模式切换中原样保留继续运行，不会被重启
