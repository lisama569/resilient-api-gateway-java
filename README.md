# Resilient API Gateway (Java)

面向多实例上游的请求转发网关。使用可在本地启动的模拟上游（JDK `HttpServer`）与进程内状态
（限流器、熔断器、幂等表）模拟真实调用，使路由、限流与失败恢复在各入口下行为一致且可解释。

## 快速开始

```bash
mvn spring-boot:run
```

启动后会按 `application.properties` 自动拉起两个模拟上游（`upstream-a:19001`、`upstream-b:19002`）。

```bash
# 正常转发（轮询分发到 upstream-a / upstream-b）
curl -X POST http://127.0.0.1:8080/gateway/orders/work

# 携带幂等键：同一业务请求重复投递不会产生重复副作用
curl -X POST -H "Idempotency-Key: order-123" http://127.0.0.1:8080/gateway/orders/work
curl -X POST -H "Idempotency-Key: order-123" http://127.0.0.1:8080/gateway/orders/work   # 返回首个结果

# 未知路由：可区分的路由失败
curl -X POST http://127.0.0.1:8080/gateway/unknown/work
# => {"code":"ROUTE_NOT_FOUND","message":"no route for unknown"}
```

运行过程中日志会打印 `[gateway-diag]` 诊断事件：路由决策、每次尝试的上游/熔断状态/在途并发、
限流拒绝、重试次数与最终结果。

## 路由与负载均衡规则

- 路由规则（`gateway.routes`）：`routeId` + 路径前缀 + 有序上游实例列表。
  入口为 `POST /gateway/{routeId}/**`，路径透传给上游。
- 无匹配路由 → `ROUTE_NOT_FOUND`（404）；路由无可用实例 → `NO_HEALTHY_UPSTREAM`（503）。
  二者与上游自身错误严格区分。
- 负载均衡：轮询（`RoundRobinLoadBalancer`）决定首个候选起点；某次尝试失败后按序轮转到下一个
  **熔断器未打开** 的实例，实现故障转移。全部实例熔断 → `CIRCUIT_OPEN`（503）。
- 幂等：同一业务请求（相同 `Idempotency-Key`，缺省时网关生成并在重试间共用）在网关层
  （`IdempotencyStore` 缓存首个成功响应）与上游层（模拟上游的幂等表）双重去重，
  重试 / 故障转移 / 客户端重复投递都不会产生重复副作用。

## 限流与熔断语义

按上游实例维度各有一套限流器与熔断器，**超限快速拒绝，不排队**：

- 并发限制（`ConcurrencyLimiter`，信号量）：在途请求达到 `max-concurrency` 时
  立即拒绝 → `CONCURRENCY_LIMIT_EXCEEDED`（429）。
- 速率限制（`RateLimiter`，令牌桶，容量允许 1 秒突发）：无令牌时
  立即拒绝 → `RATE_LIMIT_EXCEEDED`（429）。
- 熔断器（`CircuitBreaker`）：
  - `CLOSED`：正常放行；连续失败（超时 / 连接失败 / 5xx）达到 `circuit-failure-threshold` 后熔断。
  - `OPEN`：快速拒绝 `circuit-open-duration-ms`；期间请求不会打到上游。
  - `HALF_OPEN`：冷却结束后进入，仅放行 `circuit-half-open-permits` 个探测请求；
    探测成功则闭合，探测失败则重新熔断并重新计时。
  - 所有状态迁移在 `synchronized` 内完成，半开许可计数保证并发下不误放行、不状态错乱
    （有并发测试覆盖）。

检查顺序：并发限流 → 速率限流 → 熔断许可 → 调用上游。限流拒绝发生在获取熔断许可之前，
许可的获取 / 释放在所有路径上成对出现，不会泄漏。

## 重试与错误映射

- 可重试错误：超时、连接失败、上游 5xx。重试次数上限 `max-retries`，
  且受整体超时预算 `overall-timeout-ms` 约束（单次尝试超时取
  `min(per-attempt-timeout-ms, 剩余预算)`），预算耗尽 → `RETRY_BUDGET_EXHAUSTED`。
- 重试在同一幂等键下进行，不会放大副作用；限流拒绝属于快速失败，不重试。

| 对外错误码 | HTTP | 含义 |
|---|---|---|
| `ROUTE_NOT_FOUND` | 404 | 路由失败：无匹配规则 |
| `NO_HEALTHY_UPSTREAM` | 503 | 路由失败：无可用实例 |
| `CONCURRENCY_LIMIT_EXCEEDED` | 429 | 网关并发限流 |
| `RATE_LIMIT_EXCEEDED` | 429 | 网关速率限流 |
| `CIRCUIT_OPEN` | 503 | 熔断中快速拒绝 |
| `UPSTREAM_TIMEOUT` | 504 | 上游超时（重试耗尽后） |
| `UPSTREAM_CONNECT_FAILURE` | 502 | 上游连接失败（重试耗尽后） |
| `UPSTREAM_BUSINESS_ERROR` | 502 | 上游 5xx（重试耗尽后） |
| `RETRY_BUDGET_EXHAUSTED` | 504 | 整体超时预算耗尽 |

错误体同时携带 `upstreamId`（如适用），路由失败、网关限流与上游自身错误两两可区分。

## 配置

见 `src/main/resources/application.properties`：上游实例（地址、`max-concurrency`、
`rate-limit-per-second`）、路由规则、重试与超时预算、熔断参数。
`gateway.mock-upstreams.enabled=true` 时本地自动启动与配置对应的模拟上游。

## 验证方法

```bash
mvn test
```

- `GatewayRoutingFailoverTest`：正常路由、上游 5xx 故障转移、重复投递幂等去重、
  上游级幂等、路由失败与上游失败可区分。
- `GatewayResilienceTest`：超时归一化与有限重试、整体超时预算约束、并发超限快速拒绝、
  速率超限快速拒绝、连续失败熔断 → 冷却 → 半开探测恢复、探测失败重新熔断、
  半开并发下唯一探测许可不误放行。
- 每个用例打印 `==== 诊断事件 ====`：路由决策、限流与熔断状态、重试次数，结论稳定可重复。
