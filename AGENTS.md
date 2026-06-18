- **python:** 编辑完代码后，运行 `.\scripts\check.ps1` 脚本进行类型与风格检查。
- **python:** 所有非故意公开让外部访问的类成员，都应该用下划线前缀命名
- 
- **directory_structure:** 网关核心代码已拆分至 `gateway/` 包目录下，数据默认存储于 `gateway_data/` 目录下（可通过 `COMFYUI_GATEWAY_DATA_DIR` 环境变量进行自定义设置）。禁止向仓库根目录随意添加新的网关业务 python 文件。此外，保存提交失败任务信息的 `failed_workflows` 目录也必须保存在由 `COMFYUI_GATEWAY_DATA_DIR` 定义的网关数据存储目录下（即 `os.path.join(self.config.data_dir, "failed_workflows")`），禁止直接放置在项目根目录下。


