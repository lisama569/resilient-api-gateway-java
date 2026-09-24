package com.github.lisama569.resilientapigatewayjava.error;

/**
 * 归一化失败原因大类。路由失败、限流拒绝、上游失败、超时、
 * 预算耗尽等必须对外可区分。
 */
public enum FailureKind {
    ROUTE_NOT_FOUND,
    RATE_LIMITED,
    CONCURRENCY_LIMITED,
    CIRCUIT_OPEN,
    UPSTREAM_CONNECT,
    UPSTREAM_TIMEOUT,
    UPSTREAM_BUSINESS,
    RETRY_BUDGET_EXHAUSTED
}
