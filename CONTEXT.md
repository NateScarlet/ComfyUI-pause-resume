
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
