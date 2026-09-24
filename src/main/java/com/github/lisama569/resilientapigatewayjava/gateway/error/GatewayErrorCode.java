package com.github.lisama569.resilientapigatewayjava.gateway.error;

/**
 * 对外可区分的归一化错误码。
 */
public enum GatewayErrorCode {
    ROUTE_NOT_FOUND,            // 路由失败：无匹配规则
    NO_HEALTHY_UPSTREAM,        // 路由失败：无健康实例
    CONCURRENCY_LIMIT_EXCEEDED, // 网关限流：并发超限
    RATE_LIMIT_EXCEEDED,        // 网关限流：速率超限
    CIRCUIT_OPEN,               // 熔断中，快速拒绝
    UPSTREAM_TIMEOUT,           // 上游超时
    UPSTREAM_CONNECT_FAILURE,   // 上游连接失败
    UPSTREAM_BUSINESS_ERROR,    // 上游业务错误（5xx）
    RETRY_BUDGET_EXHAUSTED,     // 整体超时预算耗尽
    INTERNAL_ERROR
}
