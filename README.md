# ComfyUI 暂停和恢复脚本

将脚本放置在便携版 ComfyUI 的根目录中，使用 `start.cmd` 启动，`stop.cmd` 暂停（保存队列并结束进程），下次使用 `start.cmd` 时会自动恢复之前的队列。

启动配置可以在 `.env` 文件或环境变量中调整。

> **依赖要求**：需要 PowerShell 7 (pwsh7) 或更高版本。

## 功能特点

- **自动恢复**：启动时自动恢复上次暂停时保存的队列
- **队列备份**：运行过程中自动备份队列，防止意外中断导致任务丢失
- **进程管理**：完善的进程启动、停止和监控机制
- **灵活配置**：支持 `.env` 文件和环境变量配置（如 `COMFYUI_PORT`, `COMFYUI_EXTRA_ARGS`）

## 使用说明

### 启动 ComfyUI

```cmd
start.cmd
```

### 暂停 ComfyUI（保存队列并停止）

```cmd
stop.cmd
```

### 手动保存当前队列（不停止服务）

```cmd
save.cmd
```

## 文件说明

- `start.cmd` / `start.ps1` - 启动脚本，包含队列恢复功能
- `stop.cmd` / `stop.ps1` - 停止脚本，保存当前队列后结束进程
- `save.cmd` / `save.ps1` - 手动保存队列脚本
- `.process_info` - 进程信息文件（自动生成）
- `queue.json` - 保存的队列文件（自动生成）
- `queue.json~` - 队列备份文件（自动生成）

## 配置选项

### 环境变量与 .env

支持在脚本同目录下创建 `.env` 文件或直接设置环境变量：

- `COMFYUI_PORT`: 服务端口（默认 `8188`）
- `COMFYUI_EXTRA_ARGS`: 传递给 ComfyUI 的额外参数（例如 `--preview-method auto`）
- `COMFYUI_BACKUP_DEBOUNCE_SEC`: 队列备份防抖间隔（秒，默认 `30`）
- `COMFYUI_MAX_BACKUP_DELAY_SEC`: 最大备份延迟时间（秒，默认 `300`）
- `COMFYUI_RESTART_DELAY_SEC`: 进程异常退出后重启延迟（秒，默认 `10`）

## 技术说明

此实现使用 HTTP API 进行队列的保存和恢复，相比 [yara](https://github.com/Satellile/yara) 的方案，能够完整保存工作流的 `extra_data` 信息，确保队列恢复的准确性。

## 注意事项

1. 确保已安装 PowerShell 7 或更高版本
2. 脚本需要放置在 ComfyUI 便携版的根目录
3. 暂停操作会直接中断当前任务，后续启动再继续
4. 如果进程异常退出，脚本会自动尝试重启
