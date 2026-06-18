- **python:**: 编辑完代码后，使用 pyright 检查风格问题

应同时遵守 [本地规则](./AGENTS.local.md) @AGENTS.local.md

- **directory_structure:** 网关核心代码已拆分至 `gateway/` 包目录下，数据默认存储于 `gateway_data/` 目录下（可通过 `COMFYUI_GATEWAY_DATA_DIR` 环境变量进行自定义设置）。禁止向仓库根目录随意添加新的网关业务 python 文件。


