'use strict';

class BrokerError extends Error {
  constructor(message, code) {
    super(message);
    this.name = this.constructor.name;
    this.code = code;
  }
}

// 生产者 epoch 过期：旧 epoch 的写入必须拒绝（fencing）
class ProducerFencedError extends BrokerError {
  constructor(message) {
    super(message, 'PRODUCER_FENCED');
  }
}

// 同一 epoch 内 sequence 必须严格连续递增
class OutOfOrderSequenceError extends BrokerError {
  constructor(message) {
    super(message, 'OUT_OF_ORDER_SEQUENCE');
  }
}

// 租约 fencing token 不是当前持有者：旧 owner 的迟到提交
class StaleLeaseError extends BrokerError {
  constructor(message) {
    super(message, 'STALE_LEASE');
  }
}

// 租约已过期（消费者超时）
class LeaseExpiredError extends BrokerError {
  constructor(message) {
    super(message, 'LEASE_EXPIRED');
  }
}

// 分区上没有有效租约
class UnknownLeaseError extends BrokerError {
  constructor(message) {
    super(message, 'UNKNOWN_LEASE');
  }
}

// 提交位点超出日志末端
class OffsetOutOfRangeError extends BrokerError {
  constructor(message) {
    super(message, 'OFFSET_OUT_OF_RANGE');
  }
}

module.exports = {
  BrokerError,
  ProducerFencedError,
  OutOfOrderSequenceError,
  StaleLeaseError,
  LeaseExpiredError,
  UnknownLeaseError,
  OffsetOutOfRangeError,
};
