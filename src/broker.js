'use strict';

const fs = require('node:fs');
const path = require('node:path');
const {
  ProducerFencedError,
  OutOfOrderSequenceError,
  StaleLeaseError,
  LeaseExpiredError,
  UnknownLeaseError,
  OffsetOutOfRangeError,
} = require('./errors');

const STATE_FILE = 'broker-state.json';

function partitionKey(topic, partition) {
  return `${topic}|${partition}`;
}

function groupKey(group, topic, partition) {
  return `${group}|${topic}|${partition}`;
}

/**
 * Broker 中间件核心。
 *
 * 语义（均为分区内保证）：
 * - 至少一次投递：poll 不推进位点，commit 才推进；消费者崩溃后未提交的消息会被重放。
 * - 生产者 fencing：每个 (topic, partition, producerId) 只接受最高 epoch 的写入。
 * - 幂等去重：(producerId, epoch) 维度内按 sequence 去重；epoch 晋升后去重状态重置。
 * - 租约消费：每个 (group, topic, partition) 同一时刻只有一个 owner，
 *   fencing token 单调递增，旧 owner 的迟到提交被拒绝，位点不会回退。
 */
class Broker {
  constructor({ dataDir = null, now = () => Date.now(), leaseTtlMs = 30_000, maxRetries = 3 } = {}) {
    this._dataDir = dataDir;
    this._now = now;
    this._leaseTtlMs = leaseTtlMs;
    this._maxRetries = maxRetries;
    this._state = { partitions: {}, groups: {}, dlq: [] };
    if (dataDir) this._load();
  }

  // ---------- 生产者路径 ----------

  /**
   * 追加一条记录。返回 { offset, duplicate }。
   * epoch 低于当前已见 epoch 时抛 ProducerFencedError；
   * sequence 在同一 epoch 内重复时幂等返回原 offset，不重复落盘。
   */
  append({ topic, partition, producerId, epoch, sequence, payload }) {
    const log = this._partition(topic, partition);

    // 分区级围栏：任何低于分区当前 epoch 的写入（僵尸写者）一律拒绝
    if (epoch < log.currentEpoch) {
      throw new ProducerFencedError(
        `producer ${producerId} fenced: epoch ${epoch} < partition epoch ${log.currentEpoch}`,
      );
    }
    if (epoch > log.currentEpoch) {
      log.currentEpoch = epoch;
    }

    let producer = log.producers[producerId];
    if (!producer || epoch > producer.epoch) {
      // 新 epoch：去重状态重置，sequence 从 0 重新开始
      producer = { epoch, lastSequence: -1 };
      log.producers[producerId] = producer;
    }

    if (sequence <= producer.lastSequence) {
      const existing = log.records.find(
        (r) => r.producerId === producerId && r.epoch === epoch && r.sequence === sequence,
      );
      return { offset: existing ? existing.offset : -1, duplicate: true };
    }
    if (sequence !== producer.lastSequence + 1) {
      throw new OutOfOrderSequenceError(
        `producer ${producerId} epoch ${epoch}: expected sequence ${producer.lastSequence + 1}, got ${sequence}`,
      );
    }

    const offset = log.nextOffset++;
    log.records.push({
      offset,
      producerId,
      epoch,
      sequence,
      payload,
      timestamp: this._now(),
    });
    producer.lastSequence = sequence;
    this._persist();
    return { offset, duplicate: false };
  }

  // ---------- 租约 ----------

  /**
   * 获取分区消费租约。总是成功：新 owner 抢占旧 owner，
   * fencing token 单调递增使旧 owner 的后续操作全部失效。
   */
  acquireLease({ group, topic, partition, ownerId, ttlMs = this._leaseTtlMs }) {
    const g = this._group(group, topic, partition);
    g.fencingToken += 1;
    g.lease = {
      ownerId,
      fencingToken: g.fencingToken,
      expiresAt: this._now() + ttlMs,
    };
    this._persist();
    return { ...g.lease };
  }

  renewLease({ group, topic, partition, lease, ttlMs = this._leaseTtlMs }) {
    const g = this._group(group, topic, partition);
    this._assertLease(g, lease);
    g.lease.expiresAt = this._now() + ttlMs;
    this._persist();
    return { ...g.lease };
  }

  // ---------- 消费者路径 ----------

  /**
   * 从已提交位点开始拉取消息。不推进位点（至少一次投递）。
   */
  poll({ group, topic, partition, lease, maxRecords = 100 }) {
    const g = this._group(group, topic, partition);
    this._assertLease(g, lease);
    const log = this._partition(topic, partition);
    return log.records
      .filter((r) => r.offset >= g.committed)
      .slice(0, maxRecords)
      .map((r) => ({ ...r }));
  }

  /**
   * 提交消费位点（语义：下一条待消费的 offset）。
   * - 旧 owner（fencing token 过期）提交抛 StaleLeaseError，位点不会回退；
   * - 重复或乱序（低于当前位点）的提交是幂等 no-op；
   * - 超过日志末端抛 OffsetOutOfRangeError。
   */
  commitOffset({ group, topic, partition, offset, lease }) {
    const g = this._group(group, topic, partition);
    this._assertLease(g, lease);
    const log = this._partition(topic, partition);

    if (offset > log.nextOffset) {
      throw new OffsetOutOfRangeError(
        `offset ${offset} beyond log end ${log.nextOffset}`,
      );
    }
    if (offset <= g.committed) {
      return { committed: g.committed, advanced: false };
    }
    g.committed = offset;
    this._persist();
    return { committed: g.committed, advanced: true };
  }

  /**
   * 报告一条消息处理失败。失败次数达到 maxRetries 后进入死信队列，
   * 若该消息正好挡住队头，则位点自动越过它，避免毒消息阻塞分区。
   */
  nack({ group, topic, partition, offset, lease, reason = 'processing failed' }) {
    const g = this._group(group, topic, partition);
    this._assertLease(g, lease);
    const log = this._partition(topic, partition);
    const record = log.records.find((r) => r.offset === offset);
    if (!record) {
      throw new OffsetOutOfRangeError(`offset ${offset} not in log`);
    }

    const attempts = (g.failures[offset] || 0) + 1;
    g.failures[offset] = attempts;
    let deadLettered = false;

    if (attempts >= this._maxRetries) {
      this._state.dlq.push({
        group,
        topic,
        partition,
        offset,
        payload: record.payload,
        reason,
        attempts,
        deadLetteredAt: this._now(),
      });
      delete g.failures[offset];
      deadLettered = true;
      if (offset === g.committed) {
        g.committed = offset + 1;
      }
    }
    this._persist();
    return { attempts, deadLettered };
  }

  // ---------- 只读视图 ----------

  committedOffset(group, topic, partition) {
    return this._group(group, topic, partition).committed;
  }

  logEndOffset(topic, partition) {
    return this._partition(topic, partition).nextOffset;
  }

  deadLetters({ group, topic, partition } = {}) {
    return this._state.dlq.filter(
      (d) =>
        (group === undefined || d.group === group) &&
        (topic === undefined || d.topic === topic) &&
        (partition === undefined || d.partition === partition),
    );
  }

  // ---------- 内部 ----------

  _partition(topic, partition) {
    const key = partitionKey(topic, partition);
    if (!this._state.partitions[key]) {
      this._state.partitions[key] = { nextOffset: 0, currentEpoch: 0, records: [], producers: {} };
    }
    return this._state.partitions[key];
  }

  _group(group, topic, partition) {
    const key = groupKey(group, topic, partition);
    if (!this._state.groups[key]) {
      this._state.groups[key] = { committed: 0, fencingToken: 0, lease: null, failures: {} };
    }
    return this._state.groups[key];
  }

  _assertLease(g, lease) {
    if (!lease || !g.lease) {
      throw new UnknownLeaseError('no active lease on this partition');
    }
    if (lease.ownerId !== g.lease.ownerId || lease.fencingToken !== g.lease.fencingToken) {
      throw new StaleLeaseError(
        `lease fencing token ${lease.fencingToken} is stale (current ${g.lease.fencingToken})`,
      );
    }
    if (g.lease.expiresAt <= this._now()) {
      throw new LeaseExpiredError(`lease for owner ${g.lease.ownerId} expired`);
    }
  }

  _load() {
    const file = path.join(this._dataDir, STATE_FILE);
    if (fs.existsSync(file)) {
      this._state = JSON.parse(fs.readFileSync(file, 'utf8'));
    }
  }

  _persist() {
    if (!this._dataDir) return;
    fs.mkdirSync(this._dataDir, { recursive: true });
    const file = path.join(this._dataDir, STATE_FILE);
    const tmp = `${file}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(this._state));
    fs.renameSync(tmp, file);
  }
}

module.exports = { Broker };
