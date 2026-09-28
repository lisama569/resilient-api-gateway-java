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
    UPSTREAM_BUSINESS_ERROR,    // 上游内部故障（5xx，重试耗尽后）
    UPSTREAM_REJECTED,          // 上游受理前拒绝（调用方原因，4xx，不重试）
    IDEMPOTENCY_IN_PROGRESS,    // 同幂等键的请求仍在处理中（晚到请求快速返回，409）
    IDEMPOTENCY_KEY_NOT_FOUND,  // 查询入口：幂等键无任何记录（404）
    RETRY_BUDGET_EXHAUSTED,     // 整体超时预算耗尽
    INTERNAL_ERROR
}
