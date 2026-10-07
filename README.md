# embedded-message-broker-java

进程内消息队列核心：用内存状态模拟消息中间件，提供投递、拉取、确认、位点提交、
死信查询与重放能力。全部行为可在本地复现验证，不依赖任何外部服务。

核心代码位于 `src/main/java/com/github/lechandonga/embeddedmessagebrokerjava/broker/`。

## 投递语义

- `publish(topic, partitionKey, payload, dedupKey)`：按 `(topic, partitionKey)` 分区，
  分区内 offset 严格递增、FIFO。
- `poll(group, consumerId, topic, max)`：以**租约**方式投递，同一时刻一条消息只会
  租约给一个 `(group, consumerId)`，租约期内对其他消费者不可见；并发投递/拉取下
  不丢消息、不重复投递（所有 public 方法内部串行化）。
- `dedupKey` 非空时在主题内去重：相同去重键重复投递抛 `DUPLICATE_MESSAGE`，只生效一次。
- 容量与参数边界明确拒绝而非静默丢弃：活跃消息数超过 `topicCapacity` 抛
  `CAPACITY_EXCEEDED`；空主题、null 负载、非正批量等抛 `INVALID_ARGUMENT`。

## 位点与确认规则

- `ack(group, consumerId, messageId)`：只有持有未过期租约的消费者才能确认，
  否则抛 `NOT_LEASED`；确认后消息才可能推进位点。
- `commitOffset(group, topic, partitionKey, offset)`：位点**单调向前**。
  - 目标位点低于已提交位点 → `OFFSET_REGRESSION`；
  - 目标位点跳过尚未确认的消息 → `OFFSET_GAP`；
  - 两者是**可区分**的错误码（见 `ErrorCode`）。
- 已确认且已提交的消息会被清理并释放容量。

## 超时、重试与死信

- 租约超过 `ackTimeout` 未确认，或消费者显式 `fail(...)`，消息进入重试：
  第 n 次重试退避 `baseBackoff * 2^(n-1)`，封顶 `maxBackoff`（指数退避，可解释）。
- 重投按分区内 offset 顺序进行，不打乱同一分区其他消息的顺序。
- 重试次数超过 `maxRetries` 转入死信，可通过 `deadLetters(topic)` 查询、
  `replay(deadLetterId)` 重放；重放沿用原去重键登记，**不绕过去重规则**
  （重放后相同 `dedupKey` 的投递仍被拒绝）。

## 关键配置项（`BrokerConfig`）

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `topicCapacity` | 10000 | 单主题最大活跃消息数，超限拒绝投递 |
| `maxRetries` | 3 | 最大重试次数，超过转死信 |
| `ackTimeout` | 30s | 确认租约超时，超时触发重投 |
| `baseBackoff` | 100ms | 退避基数 |
| `maxBackoff` | 10s | 退避封顶 |

时间相关行为全部通过注入 `java.time.Clock` 驱动，测试用 `MutableClock`
推进虚拟时间，不依赖真实等待。

## 验证方法

```bash
mvn test
```

测试覆盖（日志中以 `[业务覆盖]` 标明各业务、用例与结果）：

- 并发投递与拉取：8 生产者 x 50 条 / 4 消费者并发，校验不丢、不重、分区内 FIFO；
- 确认与位点提交：位点单调推进、回退拒绝（`OFFSET_REGRESSION`）、跳变拒绝（`OFFSET_GAP`）、租约校验；
- 超时重投与顺序保持：注入时钟推进租约超时，重投后分区内顺序不变；
- 重试退避与死信重放：指数退避封顶、重试耗尽转死信、死信查询/重放、重放不绕过去重；
- 去重与边界拒绝：重复去重键、容量超限、非法参数均明确拒绝。
