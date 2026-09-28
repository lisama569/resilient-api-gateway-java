package com.github.lisama569.resilientapigatewayjava.idempotency;

/**
 * 幂等键状态快照（查询入口与晚到请求使用）。
 */
public record IdempotencySnapshot(Status status, String body) {

    public enum Status {
        IN_FLIGHT, COMPLETED
    }
}
