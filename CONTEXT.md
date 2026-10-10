
# ComfyUI Pause-Resume Gateway

## Language

**Activity（活动）**:
表明用户正在使用网关、而非仅仅让网关后台常驻的信号。
_Avoid_: API 调用、请求、流量

**Idle（空闲）**:
网关没有任务在执行、用户也没有在使用的状态。
_Avoid_: 闲置、休眠、静默

**Idle Window（空闲窗口）**:
判定"期间无活动"的时间长度。
_Avoid_: 静默期、冷却期

**Completion（完成）**:
下游正常跑完一个任务并产出结果。
_Avoid_: 结束、finished

**Interruption（中断）**:
下游在执行任务途中被强制终止，任务未产出结果。
_Avoid_: 取消、cancel

**Forced Restart（强制重启）**:
用户主动终止下游进程并立即拉起新进程的操作。
_Avoid_: 立即重启

**Failure（失败）**:
下游执行任务时遇到错误而中止，未产出结果，但下游进程存活。
_Avoid_: 报错、error

**Stall（卡死）**:
下游进程存活但执行能力已丧失，任务既无法推进也无法被下游自行收尾的状态。
_Avoid_: 挂起、无响应、hang
