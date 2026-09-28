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

# 同键并发：只有一个请求真正处理，晚到者等待同一结果；等不到则快速返回 409 IDEMPOTENT_PROCESSING
for i in $(seq 1 20); do
  curl -s -X POST -H "Idempotency-Key: order-race" http://127.0.0.1:8080/gateway/orders/work &
done; wait

# 只读查询幂等键状态：超时/断连后先查再决定是否重投，而不是盲目再发一遍
curl http://127.0.0.1:8080/gateway/idempotency/order-123
# 已完成 => 200 {"status":"COMPLETED","key":"order-123","result":"..."}
# 处理中 => 409 {"code":"IDEMPOTENT_PROCESSING",...}
# 没见过 => 404 {"code":"IDEMPOTENT_KEY_NOT_FOUND",...}

# 未知路由：可区分的路由失败
curl -X POST http://127.0.0.1:8080/gateway/unknown/work
# => {"code":"ROUTE_NOT_FOUND","message":"no route for unknown"}
```

> 说明：模拟上游默认是正常 200。要本地复现“上游 4xx 拒绝 / 5xx 内部故障 / 超时”等场景，
> 见文末[本地复现新场景](#本地复现新场景)。

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
  （`IdempotencyStore`）与上游层（模拟上游的幂等表）双重去重，
  重试 / 故障转移 / 客户端重复投递都不会产生重复副作用。

## 并发幂等（单飞）与状态查询

针对真实调用方反馈的三类新场景，幂等语义从“缓存首个成功结果”加强为**单飞（single-flight）**：

- **同键并发只处理一次**：相同 `Idempotency-Key` 的请求几乎同时到达（并发打在同一上游，
  或因故障转移被分到不同上游）时，网关对该键原子占位，只有先到者（owner）真正调用上游；
  晚到者等待 owner 的结果。
  - 在 `gateway.idempotent-wait-ms`（默认 1500ms）内拿到结果：直接返回与先到者**完全相同**的响应；
  - 超时仍未出结果：快速返回 **409 `IDEMPOTENT_PROCESSING`**——与限流（429）、熔断（503）、
    上游故障（502/504）都不同，调用方可一眼分辨“还在处理，稍后再查”，而不会各自再打一遍上游。
  - owner 失败（超时 / 连接失败 / 5xx / 限流 / 熔断等不确定结果）时释放占位：等待者可接管重试，
    后续同键请求也能重新发起，不会被一个失败结果永久钉死。
  - 叠加多轮重试与故障转移、并发压力再大，真实副作用仍只发生一次（有并发测试覆盖，
    含“请求分散到不同上游”的断言）。
- **只读状态查询**：`GET /gateway/idempotency/{key}`，绝不触发上游调用，供调用方在超时 / 断连后
  先查后决定，而不是盲目重新投递：

| 查询结果 | HTTP | 响应 |
|---|---|---|
| 已处理完成 | 200 | `{"status":"COMPLETED","key":...,"result":...}` |
| 正在处理，暂无结果 | 409 | `{"code":"IDEMPOTENT_PROCESSING",...}` |
| 从未见过该键（或此前失败已清理） | 404 | `{"code":"IDEMPOTENT_KEY_NOT_FOUND",...}` |

  处理中不会被硬报成成功或失败；失败的请求也不会在查询接口里谎报成 COMPLETED。

## 调用方拒绝（4xx）与上游内部故障（5xx）分离表达

上游对**调用方自身原因**的拒绝（参数不合法、业务规则不允许等，HTTP 4xx）与上游**自身内部故障**
（5xx）严格分开：

- 4xx 归一化为 **422 `UPSTREAM_REJECTED`**，错误体带 `upstreamId`、`upstreamStatus`、`detail`：
  含义是“改请求，而不是等上游恢复”；
- **不重试、不故障转移到别的实例、不计入会把上游打到熔断的失败统计**；
- 5xx 维持原语义：`UPSTREAM_BUSINESS_ERROR`（502），可重试、可故障转移、计入熔断。
- 正常业务结果不受影响。

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
- 重试在同一幂等键下进行，且同键同时只有一个真实处理者（单飞），不会放大副作用；
  限流拒绝与上游 4xx 拒绝都属于快速失败，不重试。

| 对外错误码 | HTTP | 含义 |
|---|---|---|
| `ROUTE_NOT_FOUND` | 404 | 路由失败：无匹配规则 |
| `NO_HEALTHY_UPSTREAM` | 503 | 路由失败：无可用实例 |
| `CONCURRENCY_LIMIT_EXCEEDED` | 429 | 网关并发限流 |
| `RATE_LIMIT_EXCEEDED` | 429 | 网关速率限流 |
| `CIRCUIT_OPEN` | 503 | 熔断中快速拒绝 |
| `UPSTREAM_TIMEOUT` | 504 | 上游超时（重试耗尽后） |
| `UPSTREAM_CONNECT_FAILURE` | 502 | 上游连接失败（重试耗尽后） |
| `UPSTREAM_BUSINESS_ERROR` | 502 | 上游内部错误 5xx（重试耗尽后，计入熔断） |
| `UPSTREAM_REJECTED` | 422 | **新增**：调用方原因被上游拒绝 4xx（不重试/不转移/不计熔断），请改请求 |
| `IDEMPOTENT_PROCESSING` | 409 | **新增**：同幂等键仍在处理中，暂无结果（晚到等待超时或查询处理中键） |
| `IDEMPOTENT_KEY_NOT_FOUND` | 404 | **新增**：查询的幂等键从未出现过 |
| `RETRY_BUDGET_EXHAUSTED` | 504 | 整体超时预算耗尽 |

错误体同时携带 `upstreamId`（如适用）；`UPSTREAM_REJECTED` 还带 `upstreamStatus` 与
上游返回的 `detail`，路由失败、网关限流、调用方拒绝与上游自身错误两两可区分。

## 对老调用方的兼容范围

- **不带 `Idempotency-Key` 的请求**：行为与旧版完全一致——网关内部生成唯一随机键，
  该键不参与跨请求单飞合并，也无法通过查询接口查到（调用方拿不到键）。
  路由、限流、重试、熔断、故障转移均不受影响。
- **老错误码与 HTTP 状态保持不变**：`ROUTE_NOT_FOUND` / `NO_HEALTHY_UPSTREAM` /
  `CONCURRENCY_LIMIT_EXCEEDED` / `RATE_LIMIT_EXCEEDED` / `CIRCUIT_OPEN` /
  `UPSTREAM_TIMEOUT` / `UPSTREAM_CONNECT_FAILURE` / `UPSTREAM_BUSINESS_ERROR` /
  `RETRY_BUDGET_EXHAUSTED` 的码值与 HTTP 状态均未改动，已有错误处理分支无需修改。
- 新增的 `UPSTREAM_REJECTED`（422）、`IDEMPOTENT_PROCESSING`（409）、
  `IDEMPOTENT_KEY_NOT_FOUND`（404）只在对应新场景出现；旧版里上游 4xx 会被当成成功透传，
  现在改为显式 422——若老调用方此前依赖“4xx 也走 200 通道”，需要改为识别该错误码
  （这是本次唯一需要留意的行为差异）。
- 查询入口 `GET /gateway/idempotency/{key}` 是纯增量路径，不影响任何既有 `POST /gateway/{routeId}/**`。

## 配置

见 `src/main/resources/application.properties`：上游实例（地址、`max-concurrency`、
`rate-limit-per-second`）、路由规则、重试与超时预算、熔断参数、幂等单飞等待
`gateway.idempotent-wait-ms`（晚到者等待在途结果的上限，超时返回 `IDEMPOTENT_PROCESSING`）。
`gateway.mock-upstreams.enabled=true` 时本地自动启动与配置对应的模拟上游。

## 验证方法

```bash
mvn test
```

- `GatewayRoutingFailoverTest`：正常路由、上游 5xx 故障转移、重复投递幂等去重、
  上游级幂等、路由失败与上游失败可区分（**既有，保持通过**）。
- `GatewayResilienceTest`：超时归一化与有限重试、整体超时预算约束、并发超限快速拒绝、
  速率超限快速拒绝、连续失败熔断 → 冷却 → 半开探测恢复、探测失败重新熔断、
  半开并发下唯一探测许可不误放行（**既有，保持通过**）。
- `IdempotentSingleFlightTest`（**新增**）：同一键 20 路高并发只落一次真实处理；
  首个上游 500 触发故障转移、请求分散到不同上游时仍只处理一次；完成后晚到投递直接拿缓存、
  不再触达上游；处理中超时返回可区分的 `IDEMPOTENT_PROCESSING`；owner 失败后同键可重新成功；
  并发风暴叠加多轮重试 + 故障转移保证不破。
- `UpstreamRejectionTest`（**新增**）：4xx 表达为 `UPSTREAM_REJECTED`（422，带上游状态与详情）、
  与 5xx 的 `UPSTREAM_BUSINESS_ERROR` 分离；4xx 不重试、不故障转移、不计入熔断；5xx 维持旧语义。
- `IdempotencyQueryTest`（**新增**）：未知键 NOT_FOUND、处理中 PROCESSING（不报成功/失败、
  查询不触发上游）、已完成返回同一结果、失败不谎报完成且可重试。
- `GatewayControllerWebTest`（**新增**）：查询入口路径优先级与 JSON、新错误码 HTTP 映射、
  老错误码映射不变。
- 每个用例打印 `==== 诊断事件 ====`：路由决策、单飞占位 / 等待 / 接管、限流与熔断状态、重试次数，
  结论稳定可重复。

## 本地复现新场景

启动 `mvn spring-boot:run` 后，可用下列方式复现（默认两个模拟上游）：

1. **同键并发只处理一次**：

   ```bash
   for i in $(seq 1 20); do
     curl -s -X POST -H "Idempotency-Key: order-race" \
       http://127.0.0.1:8080/gateway/orders/work &
   done; wait
   # 20 个响应完全一致；服务端两个上游的 sideEffect 合计只增加 1
   curl -s http://127.0.0.1:8080/gateway/idempotency/order-race
   # => {"status":"COMPLETED",...,"result":"upstream=upstream-x sideEffect=1"}
   ```

2. **处理中 / 未知键查询**：

   ```bash
   curl -s -i http://127.0.0.1:8080/gateway/idempotency/never-used
   # => 404 {"code":"IDEMPOTENT_KEY_NOT_FOUND",...}
   # 处理中（上游在途时查询）=> 409 {"code":"IDEMPOTENT_PROCESSING",...}
   ```

3. **调用方拒绝 vs 上游内部故障**：把模拟上游切到对应失败模式（测试用
   `MockUpstreamServer.setFailureMode(ALWAYS_400 / ALWAYS_500)` 直接驱动；
   `ALWAYS_400` 立即返回 422 `UPSTREAM_REJECTED` 且不重试，`ALWAYS_500` 走重试 / 故障转移 /
   熔断，返回 502 `UPSTREAM_BUSINESS_ERROR` 或 503 `CIRCUIT_OPEN`）。相关断言见
   `UpstreamRejectionTest`，可在 IDE 中单独运行观察 `[gateway-diag]` 事件差异。
