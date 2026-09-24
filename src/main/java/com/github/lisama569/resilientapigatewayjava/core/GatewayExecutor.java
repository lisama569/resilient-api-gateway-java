package com.github.lisama569.resilientapigatewayjava.core;

import com.github.lisama569.resilientapigatewayjava.config.GatewayProperties;
import com.github.lisama569.resilientapigatewayjava.config.ResilienceRegistry;
import com.github.lisama569.resilientapigatewayjava.error.FailureKind;
import com.github.lisama569.resilientapigatewayjava.error.UpstreamFailureException;
import com.github.lisama569.resilientapigatewayjava.model.GatewayResponse;
import com.github.lisama569.resilientapigatewayjava.model.RouteRule;
import com.github.lisama569.resilientapigatewayjava.model.UpstreamInstance;
import com.github.lisama569.resilientapigatewayjava.resilience.BreakerState;
import com.github.lisama569.resilientapigatewayjava.resilience.CircuitBreaker;
import com.github.lisama569.resilientapigatewayjava.resilience.RateLimiter;
import com.github.lisama569.resilientapigatewayjava.resilience.RetryPolicy;
import com.github.lisama569.resilientapigatewayjava.routing.IdempotencyStore;
import com.github.lisama569.resilientapigatewayjava.routing.RoundRobinLoadBalancer;
import com.github.lisama569.resilientapigatewayjava.routing.Router;
import com.github.lisama569.resilientapigatewayjava.upstream.UpstreamClient;
import com.github.lisama569.resilientapigatewayjava.upstream.UpstreamResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * 网关主编排器，所有入口（HTTP、测试直连）都走这里，保证行为一致。
 * <pre>
 *  入站幂等去重（同键并发/重复请求短路）
 *   -> 最长前缀路由（失败 = ROUTE_NOT_FOUND）
 *   -> 轮询选择实例，逐实例：
 *        熔断器判定（OPEN 快速跳过/拒绝）
 *        -> 限流器判定（并发/速率超限快速拒绝，与上游错误区分）
 *        -> 受预算约束的调用（超时/连接/业务错误归一化）
 *        -> 可重试错误在总超时预算内换实例重试 + 故障转移
 * </pre>
 * 每一步都写入决策轨迹并打印，使路由、限流、熔断、重试行为可解释。
 */
@Component
public class GatewayExecutor {

    private static final Logger log = LoggerFactory.getLogger(GatewayExecutor.class);

    private final Router router;
    private final UpstreamClient client;
    private final ResilienceRegistry registry;
    private final IdempotencyStore idempotency;
    private final RoundRobinLoadBalancer loadBalancer;
    private final GatewayProperties properties;

    public GatewayExecutor(Router router, UpstreamClient client,
                           ResilienceRegistry registry,
                           IdempotencyStore idempotency,
                           RoundRobinLoadBalancer loadBalancer,
                           GatewayProperties properties) {
        this.router = router;
        this.client = client;
        this.registry = registry;
        this.idempotency = idempotency;
        this.loadBalancer = loadBalancer;
        this.properties = properties;
    }

    public GatewayResponse execute(String method, String path, String body,
                                   String incomingKey) {
        List<String> trace = new ArrayList<>();
        String requestId = UUID.randomUUID().toString().substring(0, 8);
        String key = (incomingKey == null || incomingKey.isBlank())
                ? "auto-" + UUID.randomUUID()
                : incomingKey;
        trace.add("[" + requestId + "] receive " + method + " " + path
                + " idempotencyKey=" + key);
        log.info("[{}] receive {} {} key={}", requestId, method, path, key);

        // 1) 入站幂等：同键请求首个执行，其余短路，杜绝重复副作用
        Optional<IdempotencyStore.CachedOutcome> cached = idempotency.lookup(key);
        if (cached.isPresent()) {
            IdempotencyStore.CachedOutcome c = cached.get();
            if (c.inflight()) {
                trace.add("idempotency: same key request still in-flight -> reject 409");
                log.info("[{}] idempotency in-flight reject", requestId);
                return emit(new GatewayResponse(409,
                        errorBody(FailureKind.UPSTREAM_BUSINESS,
                                "duplicate in-flight request: " + key), trace));
            }
            trace.add("idempotency: replay cached outcome status=" + c.status()
                    + " without re-invoking upstream");
            log.info("[{}] idempotency replay status={}", requestId, c.status());
            return emit(new GatewayResponse(c.status(), c.body(), trace));
        }
        boolean ownsKey = idempotency.firstAttempt(key);
        if (!ownsKey) {
            return emit(new GatewayResponse(409,
                    errorBody(FailureKind.UPSTREAM_BUSINESS,
                            "duplicate concurrent request: " + key), trace));
        }

        // 2) 路由
        RouteRule rule = router.match(path);
        if (rule == null) {
            trace.add("routing: no rule matches " + path + " -> ROUTE_NOT_FOUND");
            log.warn("[{}] route not found: {}", requestId, path);
            return finish(key, new GatewayResponse(404,
                    errorBody(FailureKind.ROUTE_NOT_FOUND,
                            "no route for path: " + path), trace), trace);
        }
        List<UpstreamInstance> pool = router.instancesOf(rule);
        trace.add("routing: matched rule=" + rule.getId() + " prefix="
                + rule.getPathPrefix() + " instances=" + ids(pool));
        log.info("[{}] routed rule={} instances={}", requestId, rule.getId(), ids(pool));
        if (pool.isEmpty()) {
            return finish(key, new GatewayResponse(503,
                    errorBody(FailureKind.ROUTE_NOT_FOUND,
                            "route has no available instance: " + rule.getId()), trace),
                    trace);
        }

        // 3) 重试 / 故障转移主循环
        RetryPolicy retry = new RetryPolicy(properties.getRetryMaxAttempts(),
                properties.getRetryTotalTimeoutMillis());
        long start = System.currentTimeMillis();
        int attempts = 0;
        // 本轮故障转移中已访问过的实例（入口拒绝或已调用），用于排除重复选择
        Set<String> visited = new HashSet<>();
        // 真正调用过（已触达上游）的实例，预算允许时可回到它们再试
        Set<String> upstreamCalled = new HashSet<>();
        FailureKind lastKind = null;
        int lastUpstreamStatus = 0;
        String lastMessage = null;
        boolean everReachedUpstream = false;
        int realCalls = 0;

        while (true) {
            long elapsed = System.currentTimeMillis() - start;
            // 预算双重约束：真实上游调用次数（重试放大防护）与总尝试次数
            // （熔断/限流空转防护，避免在被拒实例上无限循环）
            if (!retry.hasBudget(elapsed, realCalls)
                    || attempts >= retry.maxAttempts()) {
                String note = "retry budget exhausted: upstreamCalls=" + realCalls
                        + " totalAttempts=" + attempts
                        + " elapsed=" + elapsed + "ms budget="
                        + retry.totalTimeoutMillis() + "ms, last="
                        + (lastKind == null ? "no-instance" : lastKind);
                trace.add("retry: " + note);
                log.warn("[{}] {}", requestId, note);
                // 只要真正触达过上游，就保留最后的真实错误原因，
                // 避免把“上游超时/连接失败”错误地改报为预算耗尽
                if (!everReachedUpstream) {
                    lastKind = FailureKind.RETRY_BUDGET_EXHAUSTED;
                    lastMessage = note;
                }
                break;
            }

            // 第一轮：排除所有已访问实例做故障转移；
            // 所有实例都访问过后，只有“调用过上游”的实例可以在预算内再试。
            Set<String> exclude = visited.containsAll(idsSet(pool))
                    ? complement(pool, upstreamCalled)
                    : visited;
            UpstreamInstance instance = loadBalancer.pick(
                    rule.getId(), pool, exclude);
            // 选中的实例若熔断仍 OPEN，则本轮无需再试它（窗口内不会恢复）：
            // 仅在还有其它未访问实例时继续故障转移，否则立即快速失败，
            // 既不调用上游也不空转重试。
            if (instance != null && registry.breaker(instance.getId()).isOpen()) {
                visited.add(instance.getId());
                attempts++;
                CircuitBreaker b = registry.breaker(instance.getId());
                trace.add("attempt#" + attempts + " instance=" + instance.getId()
                        + " breaker=OPEN -> fast-skip (circuit open, upstream never called)");
                log.info("[{}] attempt#{} {} breaker=OPEN skip", requestId, attempts,
                        instance.getId());
                if (lastKind == null) {
                    lastKind = FailureKind.CIRCUIT_OPEN;
                    lastMessage = "circuit open for instance " + instance.getId();
                }
                if (visited.containsAll(idsSet(pool))) {
                    String note = "all instances circuit-open; fail fast";
                    trace.add("failover: " + note);
                    log.warn("[{}] {}", requestId, note);
                    break;
                }
                continue;
            }
            if (instance == null) {
                String note = "no more candidate instance (all rejected by "
                        + "breaker/limiter or exhausted): visited=" + visited;
                trace.add("failover: " + note);
                log.warn("[{}] {}", requestId, note);
                // 从未触达上游：结论就是入口侧拒绝（限流/熔断），保持其原因；
                // 已触达过上游：lastKind 已保留真实上游错误
                if (!everReachedUpstream && lastKind == null) {
                    lastKind = FailureKind.CIRCUIT_OPEN;
                }
                lastMessage = note;
                break;
            }
            visited.add(instance.getId());
            attempts++;

            CircuitBreaker breaker = registry.breaker(instance.getId());
            RateLimiter limiter = registry.rateLimiter(instance.getId());

            // 熔断放行判定：可能完成 OPEN->HALF_OPEN 并原子占用探测名额；
            // HALF_OPEN 名额已满时快速拒绝（不会误放行多余探测）
            if (!breaker.allowRequest()) {
                trace.add("attempt#" + attempts + " instance=" + instance.getId()
                        + " breaker=HALF_OPEN -> fast-skip (probe permits exhausted)");
                log.info("[{}] attempt#{} {} half-open permits exhausted",
                        requestId, attempts, instance.getId());
                lastKind = FailureKind.CIRCUIT_OPEN;
                lastMessage = "half-open probe capacity full for " + instance.getId();
                continue;
            }
            boolean probe = breaker.state() == BreakerState.HALF_OPEN;

            // 限流判定
            RateLimiter.AcquireStatus acquire = limiter.tryAcquire();
            if (acquire != RateLimiter.AcquireStatus.ALLOWED) {
                FailureKind k = acquire == RateLimiter.AcquireStatus.CONCURRENCY_EXHAUSTED
                        ? FailureKind.CONCURRENCY_LIMITED
                        : FailureKind.RATE_LIMITED;
                trace.add("attempt#" + attempts + " instance=" + instance.getId()
                        + " limiter=" + acquire + " inflight="
                        + limiter.currentInflight()
                        + " -> fast-reject (upstream never called)");
                log.info("[{}] attempt#{} {} rejected by limiter: {}", requestId,
                        attempts, instance.getId(), acquire);
                lastKind = k;
                lastUpstreamStatus = 0;
                lastMessage = acquire + " on " + instance.getId();
                continue;
            }

            long remaining = retry.totalTimeoutMillis()
                    - (System.currentTimeMillis() - start);
            long timeout = Math.max(50L,
                    Math.min(properties.getCallTimeoutMillis(), remaining));
            trace.add("attempt#" + attempts + " instance=" + instance.getId()
                    + " breaker=" + breaker.state()
                    + (probe ? "(probe)" : "") + " limiter inflight="
                    + limiter.currentInflight() + " timeout=" + timeout + "ms -> call");
            log.info("[{}] attempt#{} -> {} breaker={} timeout={}ms", requestId,
                    attempts, instance.getId(), breaker.state(), timeout);

            UpstreamResult result;
            everReachedUpstream = true;
            realCalls++;
            upstreamCalled.add(instance.getId());
            try {
                result = client.call(instance, method, path, key, timeout);
            } catch (UpstreamFailureException e) {
                limiter.release();
                breaker.recordFailure();
                lastKind = e.getKind();
                lastUpstreamStatus = e.getUpstreamStatus();
                lastMessage = e.getMessage();
                trace.add("attempt#" + attempts + " instance=" + instance.getId()
                        + " -> failure kind=" + e.getKind()
                        + " breaker now=" + breaker.state()
                        + (retry.isRetryable(e.getKind(), e.getUpstreamStatus())
                                ? " [retryable]" : " [non-retryable]"));
                log.info("[{}] attempt#{} {} failure kind={} breaker={}", requestId,
                        attempts, instance.getId(), e.getKind(), breaker.state());
                if (!retry.isRetryable(e.getKind(), e.getUpstreamStatus())) {
                    break;
                }
                backoff();
                continue;
            }
            limiter.release();

            if (result.getStatus() >= 400) {
                // 统一进入错误归类（4xx/5xx 都是业务响应，是否可重试由策略判定）
                if (result.getStatus() >= 500) {
                    breaker.recordFailure();
                } else {
                    // 4xx 是客户端请求问题，不计入上游健康失败
                    breaker.recordNeutral();
                }
                lastKind = FailureKind.UPSTREAM_BUSINESS;
                lastUpstreamStatus = result.getStatus();
                lastMessage = "upstream business status " + result.getStatus();
                trace.add("attempt#" + attempts + " instance=" + instance.getId()
                        + " -> business status=" + result.getStatus()
                        + " breaker now=" + breaker.state()
                        + (retry.isRetryable(FailureKind.UPSTREAM_BUSINESS,
                                result.getStatus()) ? " [retryable]" : " [final]"));
                log.info("[{}] attempt#{} {} business status={} breaker={}",
                        requestId, attempts, instance.getId(), result.getStatus(),
                        breaker.state());
                if (!retry.isRetryable(FailureKind.UPSTREAM_BUSINESS,
                        result.getStatus())) {
                    break;
                }
                backoff();
                continue;
            }

            // 成功
            breaker.recordSuccess();
            trace.add("attempt#" + attempts + " instance=" + instance.getId()
                    + " -> success status=" + result.getStatus()
                    + (result.isDuplicate() ? " duplicate-side-effect-blocked" : "")
                    + " breaker now=" + breaker.state() + " totalRetries="
                    + (attempts - 1));
            log.info("[{}] success via {} status={} attempts={} duplicate={}",
                    requestId, instance.getId(), result.getStatus(), attempts,
                    result.isDuplicate());
            String bodyText = result.getBody();
            return finish(key, new GatewayResponse(result.getStatus(), bodyText, trace,
                    realCalls), trace);
        }

        GatewayResponse failure = mapFailure(lastKind, lastUpstreamStatus,
                lastMessage, realCalls, trace);
        return finish(key, failure, trace);
    }

    private GatewayResponse finish(String key, GatewayResponse response,
                                   List<String> trace) {
        idempotency.complete(key, response.getStatus(), response.getBody());
        // 在重新构造前追加轨迹，保证不可变拷贝包含最终结论行
        trace.add("idempotency: outcome committed for key=" + key + " status="
                + response.getStatus());
        return new GatewayResponse(response.getStatus(), response.getBody(), trace,
                response.getAttempts());
    }

    private GatewayResponse emit(GatewayResponse response) {
        return response;
    }

    private GatewayResponse mapFailure(FailureKind kind, int upstreamStatus,
                                       String message, int realCalls,
                                       List<String> trace) {
        FailureKind effective = kind == null
                ? FailureKind.RETRY_BUDGET_EXHAUSTED : kind;
        String text = errorBody(effective, message
                + (effective == FailureKind.UPSTREAM_BUSINESS
                        ? " (upstreamStatus=" + upstreamStatus + ")" : "")
                + " | attempts=" + realCalls);
        int status = switch (effective) {
            case ROUTE_NOT_FOUND -> 404;
            case RATE_LIMITED, CONCURRENCY_LIMITED -> 429;
            case CIRCUIT_OPEN -> 503;
            case UPSTREAM_CONNECT -> 502;
            case UPSTREAM_TIMEOUT -> 504;
            case UPSTREAM_BUSINESS -> upstreamStatus > 0 ? upstreamStatus : 502;
            case RETRY_BUDGET_EXHAUSTED -> 504;
        };
        trace.add("result: status=" + status + " kind=" + effective
                + " totalAttempts=" + realCalls);
        log.warn("final failure status={} kind={} attempts={}", status, effective,
                realCalls);
        return new GatewayResponse(status, text, trace, realCalls);
    }

    private static String errorBody(FailureKind kind, String message) {
        return "{\"error\":true,\"kind\":\"" + kind + "\",\"message\":\""
                + message.replace("\"", "'") + "\"}";
    }

    private static List<String> ids(List<UpstreamInstance> pool) {
        return pool.stream().map(UpstreamInstance::getId).toList();
    }

    private static Set<String> idsSet(List<UpstreamInstance> pool) {
        Set<String> out = new HashSet<>();
        for (UpstreamInstance i : pool) {
            out.add(i.getId());
        }
        return out;
    }

    /** 池内全部实例 id 中去掉 subset，得到“未真正调用过上游”的实例集合。 */
    private static Set<String> complement(List<UpstreamInstance> pool,
                                          Set<String> subset) {
        Set<String> out = idsSet(pool);
        out.removeAll(subset);
        return out;
    }

    private static void backoff() {
        try {
            Thread.sleep(30L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
