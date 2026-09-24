# 多实例上游请求转发网关（Resilient API Gateway）

一个基于 Spring Boot 3 / Java 21 的请求转发网关。它把请求按规则分发到多个上游实例，
在上游不可用、超时、连续失败时进行**故障转移、有限重试、熔断与限流**，并保证
**同一次业务请求在重试 / 故障转移中不产生重复副作用**。工程内置一个**可本地启动的
模拟上游**（同一进程内通过真实 HTTP 调用多个实例），用进程内状态模拟健康、延迟、
超时、业务错误与不可用，使路由、限流与失败恢复行为可重复、可解释。

## 1. 快速开始

```bash
mvn spring-boot:run            # 或 mvn -DskipTests package 后 java -jar target/*.jar
```

默认监听 `8080`。网关入口：

| 入口 | 说明 |
| --- | --- |
| `/api/**` | 主业务入口（示例：`/api/orders/**`、`/api/payment/**`） |
| `/gw/**`  | 等价入口，与 `/api/**` 行为完全一致（同一编排器） |
| 其它路径 | 统一进入路由层，未命中规则时返回可区分的 `ROUTE_NOT_FOUND`（404），而非容器默认 404 |

内置模拟上游（网关以真实 HTTP 回环调用）：

- 业务端点：`/__mock/{instanceId}/**`（GET 只读；POST 产生一次带幂等保护的副作用）
- 管理端点：`/__mock-admin/**`，运行时注入行为、查看副作用计数、重置状态

### 管理接口示例

```bash
curl http://localhost:8080/__mock-admin/reset                         # 重置全部模拟状态
curl 'http://localhost:8080/__mock-admin/up-a/healthy'                # up-a 恢复健康
curl 'http://localhost:8080/__mock-admin/up-a/delay?millis=1200'      # up-a 延迟
curl 'http://localhost:8080/__mock-admin/up-a/timeout'                # up-a 永远超时
curl 'http://localhost:8080/__mock-admin/up-a/status?code=500'        # up-a 返回业务错误码
curl 'http://localhost:8080/__mock-admin/up-a/fail-first-n?n=3'       # up-a 前 3 次请求 503，之后自愈
curl http://localhost:8080/__mock-admin/state                         # 查看副作用计数与幂等键
```

## 2. 路由与负载均衡规则

- **路由匹配**：按请求路径前缀匹配，采用**最长前缀优先**（例如 `/api/orders` 优先于 `/api`）。
- **路由失败可区分**：没有任何规则命中时，在路由阶段即返回 `404 ROUTE_NOT_FOUND`，
  绝不会与“上游连不通”混淆。
- **负载均衡**：对每条路由独立维护一个线程安全的轮询游标（`RoundRobinLoadBalancer`），
  不同路由各自从第一个实例开始公平轮转。
- **故障转移选点**：一次业务请求维护“已访问实例集”，**先尝试未访问过的实例**；
  全部实例都试过且仍有重试预算时，才会回到此前真正调用过的实例重试。
  被熔断 / 限流拒绝的实例本轮跳过，不会无谓重复请求。
- **实例寻址**：
  - `mock://<id>` 解析为本进程模拟上游 `http://127.0.0.1:<port>/__mock/<id>`；
  - `http(s)://...` 原样透传，可对接真实上游；
  - `dead://<name>` 解析到保留封闭端口（端口 1），稳定模拟“连接被拒绝”。

## 3. 幂等：重试 / 故障转移不放大副作用

- **网关入站去重**：以 `Idempotency-Key` 请求头标识一次业务请求（未提供时自动生成）。
  进程内 `IdempotencyStore` 保证同键请求只有第一个能真正向下游发起：
  - 首个请求执行期间，并发同键请求立即 `409`（in-flight 短路）；
  - 首个请求完成后，同键请求直接返回缓存结果，不再访问任何上游。
- **上游去重（双保险）**：幂等键以 `Idempotency-Key` 头透传。模拟上游维护
  **跨实例共享的幂等键表**，复刻真实带幂等表的上游集群——即使请求在实例间故障转移，
  同一业务键的副作用也只执行一次，重复投递响应带 `X-Upstream-Duplicate: true`。
- 这意味着“先超时、换实例重试”等场景下，下游业务（如下单、扣款）绝不会被执行多次。

## 4. 限流与熔断语义（均按上游实例维度）

### 4.1 限流（`RateLimiter`，每实例独立）

- **并发闸**：非阻塞信号量，超过 `max-concurrent` 的请求**立即拒绝**，绝不排队；
  请求结束归还并发名额。
- **速率闸**：1 秒固定窗口计数，超过 `max-per-second` 立即拒绝。
- 两类拒绝都**不会真正调用上游**，响应 `429` 并在错误体中区分：
  - `CONCURRENCY_LIMITED`（并发超限）
  - `RATE_LIMITED`（速率超限）
- 它们与上游自身错误（`UPSTREAM_*`）以及熔断（`CIRCUIT_OPEN`）明确区分。
  入口侧拒绝不消耗上游重试次数，网关会先尝试故障转移到其它实例。

### 4.2 熔断（`CircuitBreaker`，三态，每实例独立）

```
CLOSED ── 连续失败达到阈值 ──▶ OPEN ── 等待 openMillis ──▶ HALF_OPEN
  ▲                                                          │
  └──────────── 全部探测成功 ◀── HALF_OPEN ── 任一探测失败 ──┘
                                  └──────────▶ OPEN（重新计时）
```

- **OPEN**：恢复窗口未到时所有请求**无副作用快速失败**（`503 CIRCUIT_OPEN`，不触达上游）。
- **HALF_OPEN**：窗口结束后只允许 `half-open-permits` 个**探测请求**进入；
  多余请求立即拒绝。状态迁移与探测名额发放在同步块内原子完成，
  并发下不会出现“误放行多个探测”或状态错乱。
- **探测成功**：达到所需成功探测数后回到 CLOSED，逐步恢复全量流量。
- **探测失败**：立即重新 OPEN 并重新计时。
- 注意：熔断是**快速失败**机制。熔断器打开期间，单实例路由会立刻返回 503，
  而不是占用超时预算反复重试。

## 5. 重试与错误映射规则

### 5.1 重试策略（`RetryPolicy`）

- **可重试错误**：连接失败（`UPSTREAM_CONNECT`）、超时（`UPSTREAM_TIMEOUT`）、
  上游暂时性业务错误 `502/503/504`。
- **不可重试错误**：`4xx` 与 `500` 等确定性业务错误，立即归一化返回，不做重试。
- **总超时预算**：所有尝试共享 `retry-total-timeout-millis` 总预算与
  `retry-max-attempts` 次数上限；单次调用还有 `call-timeout-millis` 上限。
  预算同时约束“真实上游调用次数”（防止放大）与“总尝试次数”
  （防止在被熔断 / 限流实例上空转）。每次重试间有很短的退避。
- **不突破限流**：重试 / 故障转移每次都重新经过该实例的限流器与熔断器。

### 5.2 对外错误归一化（失败原因可区分）

| 归一化 kind | HTTP 状态 | 含义 |
| --- | --- | --- |
| `ROUTE_NOT_FOUND` | 404 | 路由阶段失败，无匹配规则 / 无可用实例 |
| `CONCURRENCY_LIMITED` | 429 | 该实例并发超限，快速拒绝，未调用上游 |
| `RATE_LIMITED` | 429 | 该实例每秒速率超限，快速拒绝，未调用上游 |
| `CIRCUIT_OPEN` | 503 | 熔断器打开（或半开探测名额已满），快速失败 |
| `UPSTREAM_CONNECT` | 502 | 上游连接失败 / 连接被拒绝 |
| `UPSTREAM_TIMEOUT` | 504 | 上游响应超时（受单次 / 总预算约束） |
| `UPSTREAM_BUSINESS` | 上游原始状态码 | 有 HTTP 响应但状态码异常（保留原始码） |
| `RETRY_BUDGET_EXHAUSTED` | 504 | 从未触达上游且预算耗尽（真实触达过时保留最后的真实错误原因） |

底层差异在调用边界（`HttpUpstreamClient`）即被归一化，超时与连接失败来自不同异常类型。

## 6. 可解释性

- 每个请求生成短 `requestId`，全过程写入**决策轨迹**并通过日志打印：
  路由命中、候选实例、每步熔断状态（CLOSED/OPEN/HALF_OPEN、是否探测）、
  限流 inflight、单次超时、失败原因、是否可重试、重试次数等。
- 响应头：
  - `X-Gateway-Trace`：以 ` || ` 分隔的完整决策轨迹；
  - `X-Gateway-Retries`：真实上游调用次数减一（即重试次数）。
- 错误体统一为 `{"error":true,"kind":"...","message":"..."}`。

## 7. 配置说明（`application.properties`）

```properties
gateway.instances[i].id=<实例id>
gateway.instances[i].base-url=mock://<id> | http(s)://... | dead://<name>
gateway.instances[i].max-concurrent=2     # 该实例最大并发
gateway.instances[i].max-per-second=5    # 该实例每秒最大请求数

gateway.routes[i].id=<规则id>
gateway.routes[i].path-prefix=/api/orders
gateway.routes[i].instances=up-a,up-b,up-d,up-dead

gateway.breaker-failure-threshold=3       # 连续失败几次后 OPEN
gateway.breaker-open-millis=2000          # OPEN 保持时长
gateway.breaker-half-open-permits=1       # HALF_OPEN 探测名额
gateway.retry-max-attempts=3              # 最大尝试次数
gateway.retry-total-timeout-millis=3000   # 总超时预算
gateway.call-timeout-millis=600           # 单次调用超时
```

## 8. 验证方法

```bash
mvn test
```

18 个集成测试用随机端口启动完整服务，通过真实 HTTP 调用网关与模拟上游，覆盖：

- **路由**：正常分发、未命中路由返回 `ROUTE_NOT_FOUND`、两个入口行为一致；
- **幂等**：重试 + 故障转移下同键副作用恰好一次；同键并发请求仅一个执行、其余 409；
- **故障转移**：暂时性 503 自动转移到健康实例；封闭端口归一化为 `UPSTREAM_CONNECT`；
- **超时**：慢上游归一化为 `UPSTREAM_TIMEOUT`(504)，且重试受总预算截断；
- **业务错误**：500 等不可重试错误立即返回、不重试，与连接 / 超时错误可区分；
- **并发限流**：8 并发下恰好 2 个进入上游、其余 `429 CONCURRENCY_LIMITED` 快速拒绝；
  顺序压测触发 `RATE_LIMITED` 且与并发限制区分；
- **熔断恢复**：连续失败触发 OPEN 并快速失败；窗口后 HALF_OPEN 单探测成功才恢复；
  6 并发下恰好 1 个探测、其余被拒；失败探测立即重新 OPEN。

测试在标准输出打印每次调用的 `HTTP 状态`、`TRACE`、`RETRIES`、`BODY`，
便于核对路由决策、限流 / 熔断状态与重试次数。`src/test/resources/application.properties`
使用更短的熔断窗口（1200ms）与更大的单次超时（1500ms），保证结论稳定、快速。

### 手动 curl 演示

```bash
curl -D - http://localhost:8080/api/orders/1                         # 200 + 决策轨迹
curl http://localhost:8080/no/route                                  # 404 ROUTE_NOT_FOUND
curl http://localhost:8080/api/dead-only/x                           # 502 UPSTREAM_CONNECT
curl -X POST -H 'Idempotency-Key: k1' http://localhost:8080/api/orders   # 201 首次
curl -X POST -H 'Idempotency-Key: k1' http://localhost:8080/api/orders   # 201 重复，无新副作用
curl http://localhost:8080/__mock-admin/state                        # effectCount 保持 1
```

## 9. 主要代码结构

```
config/      GatewayProperties（配置）、ResilienceRegistry（每实例限流器/熔断器注册表）
model/       UpstreamInstance、RouteRule、GatewayResponse（含 attempts）
routing/     Router（最长前缀路由）、RoundRobinLoadBalancer（按路由轮询）、IdempotencyStore
resilience/  RateLimiter（并发+速率）、CircuitBreaker（三态熔断）、RetryPolicy（预算）
upstream/    UpstreamClient、HttpUpstreamClient（JDK HttpClient + 错误归一化）、
             MockAddressResolver/LocalPortHolder（mock:// 寻址）、
             MockUpstreamController/MockUpstreamState（本地模拟上游与进程内状态）
core/        GatewayExecutor（统一编排：幂等→路由→熔断/限流→调用→重试/故障转移）
web/         GatewayController（/api、/gw）、CatchAllController（路由失败归一化）
error/       FailureKind 与各类可区分异常
```
