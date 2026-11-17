'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const { Broker } = require('../src/broker');
const {
  ProducerFencedError,
  OutOfOrderSequenceError,
  StaleLeaseError,
  LeaseExpiredError,
  OffsetOutOfRangeError,
} = require('../src/errors');

const TOPIC = 'orders';
const GROUP = 'g-workers';

function makeBroker(opts = {}) {
  return new Broker(opts);
}

function appendBatch(broker, producerId, epoch, fromSequence, count) {
  const offsets = [];
  for (let i = 0; i < count; i += 1) {
    const { offset } = broker.append({
      topic: TOPIC,
      partition: 0,
      producerId,
      epoch,
      sequence: fromSequence + i,
      payload: `msg-${producerId}-${epoch}-${fromSequence + i}`,
    });
    offsets.push(offset);
  }
  return offsets;
}

test('同一分区单写者：epoch 更高的写者接管后，旧 epoch 写入被围栏拒绝', () => {
  const broker = makeBroker();
  appendBatch(broker, 'p-a', 1, 0, 3);

  // 新写者以更高 epoch 接管同一分区
  appendBatch(broker, 'p-b', 2, 0, 2);

  // 旧写者的迟到写入必须被拒绝
  assert.throws(
    () =>
      broker.append({
        topic: TOPIC,
        partition: 0,
        producerId: 'p-a',
        epoch: 1,
        sequence: 3,
        payload: 'stale',
      }),
    ProducerFencedError,
  );
  assert.equal(broker.logEndOffset(TOPIC, 0), 5);
});

test('同一生产者的旧 epoch 写入被拒绝', () => {
  const broker = makeBroker();
  appendBatch(broker, 'p-a', 1, 0, 2);
  appendBatch(broker, 'p-a', 2, 0, 1);

  assert.throws(
    () =>
      broker.append({
        topic: TOPIC,
        partition: 0,
        producerId: 'p-a',
        epoch: 1,
        sequence: 2,
        payload: 'zombie-write',
      }),
    ProducerFencedError,
  );
});

test('幂等 sequence 只在同一 epoch 内去重，epoch 晋升后重置', () => {
  const broker = makeBroker();
  appendBatch(broker, 'p-a', 1, 0, 2);

  // 同 epoch 重复 sequence：幂等返回原 offset，不追加
  const dup = broker.append({
    topic: TOPIC,
    partition: 0,
    producerId: 'p-a',
    epoch: 1,
    sequence: 1,
    payload: 'msg-p-a-1-1',
  });
  assert.equal(dup.duplicate, true);
  assert.equal(dup.offset, 1);
  assert.equal(broker.logEndOffset(TOPIC, 0), 2);

  // 新 epoch 后相同 sequence 是新消息，不去重
  const fresh = broker.append({
    topic: TOPIC,
    partition: 0,
    producerId: 'p-a',
    epoch: 2,
    sequence: 0,
    payload: 'new-epoch-msg',
  });
  assert.equal(fresh.duplicate, false);
  assert.equal(fresh.offset, 2);
});

test('sequence 跳号被拒绝', () => {
  const broker = makeBroker();
  appendBatch(broker, 'p-a', 1, 0, 1);
  assert.throws(
    () =>
      broker.append({
        topic: TOPIC,
        partition: 0,
        producerId: 'p-a',
        epoch: 1,
        sequence: 5,
        payload: 'gap',
      }),
    OutOfOrderSequenceError,
  );
});

test('租约抢占后旧 owner 的迟到提交被拒绝，位点不回退', () => {
  const broker = makeBroker();
  appendBatch(broker, 'p-a', 1, 0, 10);

  const leaseA = broker.acquireLease({ group: GROUP, topic: TOPIC, partition: 0, ownerId: 'c-1' });
  broker.commitOffset({ group: GROUP, topic: TOPIC, partition: 0, offset: 4, lease: leaseA });

  // c-1 崩溃，c-2 抢占租约并从已提交位点重放
  const leaseB = broker.acquireLease({ group: GROUP, topic: TOPIC, partition: 0, ownerId: 'c-2' });
  const replayed = broker.poll({ group: GROUP, topic: TOPIC, partition: 0, lease: leaseB });
  assert.deepEqual(replayed.map((r) => r.offset), [4, 5, 6, 7, 8, 9]);
  broker.commitOffset({ group: GROUP, topic: TOPIC, partition: 0, offset: 10, lease: leaseB });

  // 旧 owner 恢复后用过期租约提交更小的位点：必须拒绝，位点保持 10
  assert.throws(
    () => broker.commitOffset({ group: GROUP, topic: TOPIC, partition: 0, offset: 6, lease: leaseA }),
    StaleLeaseError,
  );
  assert.throws(
    () => broker.poll({ group: GROUP, topic: TOPIC, partition: 0, lease: leaseA }),
    StaleLeaseError,
  );
  assert.equal(broker.committedOffset(GROUP, TOPIC, 0), 10);
});

test('重复提交与乱序提交是幂等 no-op', () => {
  const broker = makeBroker();
  appendBatch(broker, 'p-a', 1, 0, 5);
  const lease = broker.acquireLease({ group: GROUP, topic: TOPIC, partition: 0, ownerId: 'c-1' });

  assert.deepEqual(
    broker.commitOffset({ group: GROUP, topic: TOPIC, partition: 0, offset: 3, lease }),
    { committed: 3, advanced: true },
  );
  // 重复提交相同位点
  assert.deepEqual(
    broker.commitOffset({ group: GROUP, topic: TOPIC, partition: 0, offset: 3, lease }),
    { committed: 3, advanced: false },
  );
  // 乱序：提交更小的位点，不回退
  assert.deepEqual(
    broker.commitOffset({ group: GROUP, topic: TOPIC, partition: 0, offset: 1, lease }),
    { committed: 3, advanced: false },
  );
  // 超过日志末端
  assert.throws(
    () => broker.commitOffset({ group: GROUP, topic: TOPIC, partition: 0, offset: 99, lease }),
    OffsetOutOfRangeError,
  );
  assert.equal(broker.committedOffset(GROUP, TOPIC, 0), 3);
});

test('消费者超时：过期租约无法提交，新 owner 重放未提交消息', () => {
  let now = 1_000;
  const broker = makeBroker({ now: () => now, leaseTtlMs: 500 });
  appendBatch(broker, 'p-a', 1, 0, 4);

  const leaseA = broker.acquireLease({ group: GROUP, topic: TOPIC, partition: 0, ownerId: 'c-1' });
  const first = broker.poll({ group: GROUP, topic: TOPIC, partition: 0, lease: leaseA });
  assert.equal(first.length, 4);
  // c-1 处理中但未来得及提交就超时
  now += 1_000;
  assert.throws(
    () => broker.commitOffset({ group: GROUP, topic: TOPIC, partition: 0, offset: 2, lease: leaseA }),
    LeaseExpiredError,
  );

  // c-2 接管，从位点 0 重放（至少一次投递）
  const leaseB = broker.acquireLease({ group: GROUP, topic: TOPIC, partition: 0, ownerId: 'c-2' });
  const replayed = broker.poll({ group: GROUP, topic: TOPIC, partition: 0, lease: leaseB });
  assert.deepEqual(replayed.map((r) => r.offset), [0, 1, 2, 3]);
  broker.commitOffset({ group: GROUP, topic: TOPIC, partition: 0, offset: 4, lease: leaseB });
  assert.equal(broker.committedOffset(GROUP, TOPIC, 0), 4);
});

test('租约可续期，续期后正常提交', () => {
  let now = 0;
  const broker = makeBroker({ now: () => now, leaseTtlMs: 500 });
  appendBatch(broker, 'p-a', 1, 0, 2);
  const lease = broker.acquireLease({ group: GROUP, topic: TOPIC, partition: 0, ownerId: 'c-1' });

  now += 400;
  const renewed = broker.renewLease({ group: GROUP, topic: TOPIC, partition: 0, lease });
  now += 400; // 原租约已过期，续期后的仍有效
  broker.commitOffset({ group: GROUP, topic: TOPIC, partition: 0, offset: 2, lease: renewed });
  assert.equal(broker.committedOffset(GROUP, TOPIC, 0), 2);
});

test('毒消息达到重试上限进入死信队列，位点越过它继续前进', () => {
  const broker = makeBroker({ maxRetries: 3 });
  appendBatch(broker, 'p-a', 1, 0, 3);
  const lease = broker.acquireLease({ group: GROUP, topic: TOPIC, partition: 0, ownerId: 'c-1' });

  assert.deepEqual(broker.nack({ group: GROUP, topic: TOPIC, partition: 0, offset: 0, lease }), {
    attempts: 1,
    deadLettered: false,
  });
  broker.nack({ group: GROUP, topic: TOPIC, partition: 0, offset: 0, lease });
  const third = broker.nack({ group: GROUP, topic: TOPIC, partition: 0, offset: 0, lease });
  assert.equal(third.deadLettered, true);

  const dlq = broker.deadLetters({ group: GROUP, topic: TOPIC, partition: 0 });
  assert.equal(dlq.length, 1);
  assert.equal(dlq[0].offset, 0);
  assert.equal(dlq[0].payload, 'msg-p-a-1-0');

  // 位点越过毒消息，后续消息可正常消费
  assert.equal(broker.committedOffset(GROUP, TOPIC, 0), 1);
  const rest = broker.poll({ group: GROUP, topic: TOPIC, partition: 0, lease });
  assert.deepEqual(rest.map((r) => r.offset), [1, 2]);
});

test('broker 重启后日志、位点与 fencing token 保持，过期租约仍被拒绝', () => {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'broker-'));
  try {
    const broker1 = makeBroker({ dataDir });
    appendBatch(broker1, 'p-a', 1, 0, 5);
    const leaseA = broker1.acquireLease({ group: GROUP, topic: TOPIC, partition: 0, ownerId: 'c-1' });
    broker1.commitOffset({ group: GROUP, topic: TOPIC, partition: 0, offset: 3, lease: leaseA });
    broker1.nack({ group: GROUP, topic: TOPIC, partition: 0, offset: 3, lease: leaseA });
    broker1.nack({ group: GROUP, topic: TOPIC, partition: 0, offset: 3, lease: leaseA });
    broker1.nack({ group: GROUP, topic: TOPIC, partition: 0, offset: 3, lease: leaseA });

    // 模拟重启：同一 dataDir 重新打开
    const broker2 = makeBroker({ dataDir });
    assert.equal(broker2.logEndOffset(TOPIC, 0), 5);
    assert.equal(broker2.committedOffset(GROUP, TOPIC, 0), 4); // 毒消息 offset 3 已入 DLQ
    assert.equal(broker2.deadLetters({ group: GROUP }).length, 1);

    // fencing token 单调递增：重启前的租约失效
    const leaseB = broker2.acquireLease({ group: GROUP, topic: TOPIC, partition: 0, ownerId: 'c-2' });
    assert.ok(leaseB.fencingToken > leaseA.fencingToken);
    assert.throws(
      () => broker2.commitOffset({ group: GROUP, topic: TOPIC, partition: 0, offset: 5, lease: leaseA }),
      StaleLeaseError,
    );

    // 生产者去重状态也恢复：重启后重发同 epoch 同 sequence 仍幂等
    const dup = broker2.append({
      topic: TOPIC,
      partition: 0,
      producerId: 'p-a',
      epoch: 1,
      sequence: 4,
      payload: 'msg-p-a-1-4',
    });
    assert.equal(dup.duplicate, true);
    assert.equal(dup.offset, 4);
  } finally {
    fs.rmSync(dataDir, { recursive: true, force: true });
  }
});
