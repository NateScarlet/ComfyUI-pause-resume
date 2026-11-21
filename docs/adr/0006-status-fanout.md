# 客户端状态广播收敛到 presentation 层 StatusFanout

`queue_remaining` 等客户端可见状态目前有多个来源与消费者：领域聚合根发布 `StatusChangedEvent`，表现层分别订阅它维护 SSE 快照与 WS 防抖快照，`handle_sse` 建连时直接查询，WS 代理改写下游 status 帧时再次直接查询。我们决定在 presentation 层引入内聚的 `StatusFanout` 模块：由组合根构造并注入 `GatewayHandlers`，自主订阅领域事件、持有单一 `ClientStatus` 快照（`paused` / `queue_remaining` / `downstream`）并主动向 SSE/WS 广播；领域层仍是队列计数的唯一权威，不反向依赖 fanout。WS 转发 status 帧时 fanout 注入 `JobQueueReader` 读取实时计数，以保留「转发瞬间当前值」语义；防抖复用现有 `Timer` 抽象，便于测试注入 `FakeTimer`。理由是把客户端可见状态的生产、快照与广播集中到一处，消除多处同步与三份快照无守护点的问题，同时不让领域层承担协议与事件循环职责。

## Considered Options

- **放在 application 层并经 facade 暴露**：被否决。会让应用层感知 SSE/WS 帧格式与事件循环，违反依赖倒置，且需改动 facade 接口。
- **放在 domain 层作聚合根 seam，由 `_publish_status_changed` 直接调用**：被否决。领域层将承担传输与事件循环职责，违反 clean architecture。
- **fanout 只聚合状态、广播仍由 handlers 触发**：被否决。只搬状态不搬防抖与帧生成逻辑，收敛不彻底。
- **WS 改写改用最近事件快照值**：被否决。下游自带 `exec_info` 可能滞后，注入的值必须是权威实时计数；改用快照要求证明快照恒等于实时值，更难。
- **新建 fanout 专用时钟接口**：被否决。仓库已有 `Timer` 抽象与 `FakeTimer`，复用即可，不增加新抽象。

## Consequences

- `GatewayHandlers` 退化为路由与协议适配，不再维护 SSE/WS 两份计数快照与防抖逻辑。
- 托盘状态订阅与 SSE keepalive 帧维持现状，不属于本次收敛范围。
- `StatusFanout` 有独立生命周期（事件订阅与定时器），由组合根显式释放。
- 初始快照在构造或建连时经权威查询取得，此后靠事件增量更新；SSE 帧与 WS 值均从同一 `ClientStatus` 派生。
