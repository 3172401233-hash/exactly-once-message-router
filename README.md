# exactly-once-message-router
JavaScript 消息路由核心，处理幂等键、分区顺序、租约消费和死信重投

## 快速开始

```bash
node --test
```

```js
const { Broker } = require('./src/broker');

const broker = new Broker({ dataDir: './data', leaseTtlMs: 30_000, maxRetries: 3 });

// 生产：epoch 围栏 + 幂等 sequence
broker.append({ topic: 'orders', partition: 0, producerId: 'p-1', epoch: 1, sequence: 0, payload: {...} });

// 消费：租约 + 位点提交（至少一次投递）
const lease = broker.acquireLease({ group: 'g', topic: 'orders', partition: 0, ownerId: 'c-1' });
const records = broker.poll({ group: 'g', topic: 'orders', partition: 0, lease });
broker.commitOffset({ group: 'g', topic: 'orders', partition: 0, offset: 1, lease });
```

## API

| 方法 | 说明 |
| --- | --- |
| `append({topic, partition, producerId, epoch, sequence, payload})` | 追加记录，返回 `{offset, duplicate}` |
| `acquireLease({group, topic, partition, ownerId, ttlMs?})` | 获取/抢占消费租约，返回带 fencing token 的租约 |
| `renewLease({group, topic, partition, lease, ttlMs?})` | 续期租约 |
| `poll({group, topic, partition, lease, maxRecords?})` | 从已提交位点拉取，不推进位点 |
| `commitOffset({group, topic, partition, offset, lease})` | 提交消费位点（下一条待消费 offset） |
| `nack({group, topic, partition, offset, lease, reason?})` | 报告处理失败，达到 `maxRetries` 进入死信队列 |
| `committedOffset / logEndOffset / deadLetters` | 只读视图 |

## 分区内语义

以下保证都以单个 `(topic, partition)` 为单位成立：

- **至少一次投递**：`poll` 不推进位点，只有 `commitOffset` 推进。消费者在提交前崩溃，未提交的消息会被下一个 owner 从已提交位点重放，因此可能重复消费，消费端处理逻辑需幂等。
- **单写者围栏**：分区维护单调递增的 producer epoch。任何低于分区当前 epoch 的写入抛 `ProducerFencedError`，僵尸写者（网络分区恢复后的旧 leader）无法污染日志。
- **幂等去重**：去重键是 `(producerId, epoch, sequence)`，只在单个 epoch 内有效。同 epoch 重复 sequence 幂等返回原 offset 不追加；sequence 必须严格连续（跳号抛 `OutOfOrderSequenceError`）；epoch 晋升后去重状态重置，相同 sequence 视为新消息。
- **租约消费**：每个 `(group, topic, partition)` 同一时刻只有一个 owner。`acquireLease` 总是成功并抢占旧 owner，fencing token 单调递增；旧 owner 的迟到 `poll`/`commitOffset`/`nack` 抛 `StaleLeaseError`，**消费位点不会因此回退**。
- **位点单调性**：提交位点只能前进。重复提交和乱序（低于当前位点）提交是幂等 no-op；超过日志末端抛 `OffsetOutOfRangeError`。
- **消费者超时**：租约带过期时间，过期后任何操作抛 `LeaseExpiredError`；新 owner 接管后从已提交位点重放。
- **死信队列**：`nack` 累计失败次数达到 `maxRetries` 后消息进入 DLQ；若毒消息挡住队头，位点自动越过它，分区不会被单条坏消息卡死。
- **崩溃恢复**：传入 `dataDir` 后，日志、位点、fencing token、生产者去重状态和 DLQ 在每次变更时原子落盘（tmp + rename），重启后全部恢复，重启前发放的租约依然失效。

## 跨分区限制

- **无全局顺序**：只保证单个分区内的 offset 顺序；同一 topic 不同分区之间的消息顺序不做任何保证。
- **无跨分区原子性**：`append`/`commitOffset` 都是单分区操作，不提供跨分区事务；一次写多个分区的生产者在部分失败时可能出现部分可见。
- **位点独立**：消费组在每个分区上的位点互相独立，一个分区的毒消息不影响其他分区进度。
- **单进程实现**：租约与围栏状态保存在单 broker 进程内（可选落盘），不提供多 broker 副本、leader 选举或跨进程一致性；跨进程部署需要外部分布式协调。
