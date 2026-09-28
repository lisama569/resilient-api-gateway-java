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

# 同一幂等键几乎同时打进来：真实处理只发生一次。
# 晚到的请求会先等待一小段时间（gateway.idempotency-wait-ms，默认 1000ms）：
#   - 等到先到请求完成 -> 直接拿到同一个结果；
#   - 还没完成       -> 快速返回 409 IDEMPOTENCY_IN_PROGRESS，可随后查询，不要立刻重投。
for i in $(seq 1 10); do
  curl -s -o /dev/null -w "%{http_code}\n" -X POST \
    -H "Idempotency-Key: order-race" http://127.0.0.1:8080/gateway/orders/work
done

# 调用方原因被上游拒绝（参数不合法 / 业务规则不允许）：422 UPSTREAM_REJECTED，
# 与上游自己的 5xx（502 UPSTREAM_BUSINESS_ERROR）严格区分；不该改请求时就等上游恢复
curl -i -X POST -H "Idempotency-Key: order-bad" http://127.0.0.1:8080/gateway/orders/work
# （需要上游处于拒绝模式，见下文“本地复现新场景”）

# 超时 / 断连后先查再决定：只读查询幂等键当前状态
curl http://127.0.0.1:8080/idempotency/keys/order-123
# 已完成: {"key":"order-123","status":"COMPLETED","body":"upstream=upstream-a sideEffect=1"}
# 处理中: {"key":"order-race","status":"IN_FLIGHT"}
# 无记录: 404 {"code":"IDEMPOTENCY_KEY_NOT_FOUND", ...}

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
- 幂等与在途去重：同一业务请求（相同 `Idempotency-Key`，缺省时网关生成并在重试间共用）
  在网关层（`IdempotencyStore`）与上游层（模拟上游的幂等表）双重去重。
  - **并发唯一处理（leader / follower）**：同一键几乎同时打进网关时，只有一个请求登记为
    leader 真正调用上游，其余 follower 在该键的在途条目上等待。即使各请求被负载均衡 /
    故障转移分散到不同上游，落地上游的真实处理也只发生一次。
  - leader 成功：结果发布到条目并缓存，所有等待者及后续重复投递拿到同一个结果。
  - leader 失败（超时 / 连接失败 / 5xx / 限流 / 熔断拒绝）：等待者被唤醒，通过
    `putIfAbsent` 原子竞争出唯一的新 leader 接管，绝不各自重打上游；再叠加多轮重试与
    故障转移，这个“只处理一次”的保证也不会被打破。
  - follower 等待超过 `gateway.idempotency-wait-ms` 仍未完成：快速返回
    `IDEMPOTENCY_IN_PROGRESS`（409），不占用上游额度；调用方应走查询入口而不是盲目重投。

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

## 重试、故障转移与“调用方拒绝 / 上游故障”的区分

- **调用方原因被拒绝（4xx）**：上游在受理前直接拒绝（参数不合法、业务规则不允许等）。
  网关归一化为 `UPSTREAM_REJECTED`（**422**），**不重试、不故障转移、不计入熔断失败、
  不占用幂等结果缓存**——调用方改好请求后可用同一幂等键重新投递。错误体额外携带
  `upstreamStatus`（上游原始状态码）与 `detail`（上游拒绝原因），一眼看出“该改请求”。
- **上游内部故障（5xx）**：仍归一化为 `UPSTREAM_BUSINESS_ERROR`（502），属于上游自身问题，
  正常参与重试 / 故障转移并累计熔断失败，调用方应“等上游恢复”。4xx 与 5xx 因此严格分开。
- 其他可重试错误：超时、连接失败。重试次数上限 `max-retries`，
  且受整体超时预算 `overall-timeout-ms` 约束（单次尝试超时取
  `min(per-attempt-timeout-ms, 剩余预算)`），预算耗尽 → `RETRY_BUDGET_EXHAUSTED`。
- 重试在同一幂等键下由唯一 leader 进行，不会放大副作用；限流拒绝属于快速失败，不重试。

## 幂等键只读查询入口

`GET /idempotency/keys/{key}`：调用方超时或中途断连后，先查再决定，而不是不管三七二十一重投。

| 查询结果 | HTTP | 响应 |
|---|---|---|
| 已完成 | 200 | `{"key":...,"status":"COMPLETED","body":...}`（首个结果） |
| 仍在处理 | 200 | `{"key":...,"status":"IN_FLIGHT"}`（明确“暂时没有结果”，不硬报成功/失败） |
| 无任何记录 | 404 | `{"code":"IDEMPOTENCY_KEY_NOT_FOUND",...}` |

查询只读，不触发上游调用，也不改变幂等状态。

| 对外错误码 | HTTP | 含义 |
|---|---|---|
| `ROUTE_NOT_FOUND` | 404 | 路由失败：无匹配规则 |
| `IDEMPOTENCY_KEY_NOT_FOUND` | 404 | 查询入口：幂等键无任何记录 |
| `NO_HEALTHY_UPSTREAM` | 503 | 路由失败：无可用实例 |
| `CONCURRENCY_LIMIT_EXCEEDED` | 429 | 网关并发限流 |
| `RATE_LIMIT_EXCEEDED` | 429 | 网关速率限流 |
| `IDEMPOTENCY_IN_PROGRESS` | 409 | 同键请求仍在处理（晚到请求快速返回，可随后查询） |
| `CIRCUIT_OPEN` | 503 | 熔断中快速拒绝 |
| `UPSTREAM_REJECTED` | 422 | **调用方原因**被上游受理前拒绝（4xx，不重试/不熔断） |
| `UPSTREAM_TIMEOUT` | 504 | 上游超时（重试耗尽后） |
| `UPSTREAM_CONNECT_FAILURE` | 502 | 上游连接失败（重试耗尽后） |
| `UPSTREAM_BUSINESS_ERROR` | 502 | 上游内部故障 5xx（重试耗尽后） |
| `RETRY_BUDGET_EXHAUSTED` | 504 | 整体超时预算耗尽 |

错误体同时携带 `upstreamId`（如适用）；4xx 拒绝还带 `upstreamStatus` 与 `detail`，
路由失败、网关限流、在途处理中、调用方被拒与上游自身错误两两可区分。

## 配置

见 `src/main/resources/application.properties`：上游实例（地址、`max-concurrency`、
`rate-limit-per-second`）、路由规则、重试与超时预算、熔断参数、
`gateway.idempotency-wait-ms`（晚到同键请求等待先到结果的最长时间）。
`gateway.mock-upstreams.enabled=true` 时本地自动启动与配置对应的模拟上游。

## 对老调用方的兼容范围

- **不带 `Idempotency-Key` 的请求**：行为与以前一致——网关为每次投递生成独立随机键，
  不参与跨请求去重，路由 / 限流 / 重试 / 熔断语义不变。新增的在途协调只对“同键”生效，
  随机键之间互不影响。
- **老错误码全部保留、HTTP 状态不变**：`ROUTE_NOT_FOUND`(404)、`NO_HEALTHY_UPSTREAM`(503)、
  `CONCURRENCY_LIMIT_EXCEEDED`(429)、`RATE_LIMIT_EXCEEDED`(429)、`CIRCUIT_OPEN`(503)、
  `UPSTREAM_TIMEOUT`(504)、`UPSTREAM_CONNECT_FAILURE`(502)、`UPSTREAM_BUSINESS_ERROR`(502)、
  `RETRY_BUDGET_EXHAUSTED`(504) 的码值与状态码均未改，只按老码判断的调用方不受影响。
- **新增码 / 新字段属于增量**：
  - 新增 `IDEMPOTENCY_IN_PROGRESS`(409)、`UPSTREAM_REJECTED`(422)、
    `IDEMPOTENCY_KEY_NOT_FOUND`(404)，只在对应新场景出现；
  - 错误体新增的 `upstreamStatus` / `detail` 字段只在 4xx 拒绝时出现，老字段 `code` /
    `message` / `upstreamId` 保持不变；
  - 行为修正：以前上游若返回 4xx，会被网关当作成功（200）透传；现在改为显式 422。
    这是把“调用方该改请求”与“上游内部故障”分开的必要修正，2xx / 5xx 路径不变。
- **幂等查询入口**（`GET /idempotency/keys/{key}`）为纯新增只读端点，不改变
  `POST /gateway/**` 的既有行为。

## 本地复现新场景

除单测外，可用 JDK 模拟上游在本地手动复现（模拟上游通过 `FailureMode` 控制：
`NONE` / `ALWAYS_500` / `REJECT_400` / `HANG` / `RESET`，可用调试器或临时配置切换）：

1. **同键高并发只处理一次**：让上游带几百毫秒延迟，对 `/gateway/orders/work` 用同一个
   `Idempotency-Key` 并发 POST；观察所有响应相同、模拟上游 `sideEffectCount` 合计为 1。
   自动化版本：`IdempotencyConcurrencyTest#concurrentSameKeyHitsUpstreamExactlyOnce`。
2. **并发 + 故障转移仍只一次**：把 upstream-a 设为 `ALWAYS_500`、upstream-b 带延迟，
   同键并发 POST；真实副作用只在 B 上发生一次。
   自动化版本：`...#concurrentSameKeyWithFailoverStillProcessedExactlyOnce`。
3. **晚到快速返回处理中**：把 `gateway.idempotency-wait-ms` 调小（如 100ms）、上游延迟调大
   （如 900ms），先发一个请求再立即用同键发第二个；第二个得到 409
   `IDEMPOTENCY_IN_PROGRESS`，随后 `GET /idempotency/keys/{key}` 从 `IN_FLIGHT` 变为
   `COMPLETED`。自动化版本：`...#lateRequestGetsDistinctInProgressStatusWhenWaitExhausted`。
4. **4xx 与 5xx 分开**：把上游设为 `REJECT_400`：一次请求即返回 422
   `UPSTREAM_REJECTED`（带 `upstreamStatus=400` 与 `detail`），诊断里只有一次
   `upstream rejected`，熔断器保持 `CLOSED`；切成 `ALWAYS_500` 则仍是 502
   `UPSTREAM_BUSINESS_ERROR` 并触发重试 / 熔断。自动化版本：`UpstreamRejectionTest`。

## 验证方法

```bash
mvn test
```

- `GatewayRoutingFailoverTest`：正常路由、上游 5xx 故障转移、重复投递幂等去重、
  上游级幂等、路由失败与上游失败可区分（既有，无回归）。
- `GatewayResilienceTest`：超时归一化与有限重试、整体超时预算约束、并发超限快速拒绝、
  速率超限快速拒绝、连续失败熔断 → 冷却 → 半开探测恢复、探测失败重新熔断、
  半开并发下唯一探测许可不误放行（既有，无回归）。
- `IdempotencyConcurrencyTest`（新增）：同键高并发唯一处理、并发 + 故障转移仍只一次、
  晚到等待复用首个结果、等待超时快速返回可区分的 409、leader 失败后等待者接管仍只一次。
- `UpstreamRejectionTest`（新增）：4xx 拒绝返回独立码且不重试 / 不故障转移、
  拒绝不计入熔断失败、错误体携带上游状态与详情、改请求后同键重投成功。
- `IdempotencyQueryTest`（新增）：查询入口无记录（null→404）、处理中（IN_FLIGHT）、
  已完成（COMPLETED + 首个结果）。
- `GatewayHttpMappingTest`（新增）：422 / 409 / 查询 200 / 404 的 HTTP 与 JSON 映射。
- 每个用例打印 `==== 诊断事件 ====`：路由决策、在途协调、限流与熔断状态、重试次数，
  结论稳定可重复。
