# ComfyUI 暂停和恢复

将脚本放在便携版的根目录，然后用 start.cmd 启动，stop.cmd 暂停（保存队列并结束），下次 start.cmd 会自动恢复队列

启动参数在 start.ps1 里面调

依赖 pwsh7。

[yara](https://github.com/Satellile/yara)　保存的工作流会缺少未使用的节点，所以我自己用 HTTP API 实现了
