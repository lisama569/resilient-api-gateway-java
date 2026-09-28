package com.github.lisama569.resilientapigatewayjava.idempotency;

/**
 * 幂等登记令牌：leader 持有，处理结束后用于发布结果或标记失败。
 * 令牌与其在途条目一一绑定，已完成 / 已失败条目的令牌再次发布不会生效。
 */
public class IdempotencyLease {

    final IdempotencyStore.Entry entry;

    IdempotencyLease(IdempotencyStore.Entry entry) {
        this.entry = entry;
    }
}
