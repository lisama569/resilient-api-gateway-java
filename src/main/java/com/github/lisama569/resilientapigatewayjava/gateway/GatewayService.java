package com.github.lisama569.resilientapigatewayjava.gateway;

import com.github.lisama569.resilientapigatewayjava.config.GatewayProperties;
import com.github.lisama569.resilientapigatewayjava.gateway.circuit.CircuitBreaker;
import com.github.lisama569.resilientapigatewayjava.gateway.circuit.CircuitState;
import com.github.lisama569.resilientapigatewayjava.gateway.diag.GatewayDiagnostics;
import com.github.lisama569.resilientapigatewayjava.gateway.error.GatewayErrorCode;
import com.github.lisama569.resilientapigatewayjava.gateway.error.GatewayException;
import com.github.lisama569.resilientapigatewayjava.gateway.error.UpstreamResult;
import com.github.lisama569.resilientapigatewayjava.gateway.limit.ConcurrencyLimiter;
import com.github.lisama569.resilientapigatewayjava.gateway.limit.RateLimiter;
import com.github.lisama569.resilientapigatewayjava.gateway.route.LoadBalancer;
import com.github.lisama569.resilientapigatewayjava.gateway.route.RouteRule;
import com.github.lisama569.resilientapigatewayjava.idempotency.IdempotencyLease;
import com.github.lisama569.resilientapigatewayjava.idempotency.IdempotencyPrepareResult;
import com.github.lisama569.resilientapigatewayjava.idempotency.IdempotencySnapshot;
import com.github.lisama569.resilientapigatewayjava.idempotency.IdempotencyStore;
import com.github.lisama569.resilientapigatewayjava.upstream.UpstreamInstance;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 网关核心编排：
 * 路由匹配 -> 在途幂等协调（同键并发唯一 leader，晚到者等待复用或快速返回处理中）
 * -> leader 逐次尝试（并发限流 -> 速率限流 -> 熔断许可 -> 调用上游）
 * -> 可重试错误在整体超时预算内有限重试并故障转移到健康实例；4xx 拒绝直接终态返回。
 */
public class GatewayService {

    private final GatewayProperties properties;
    private final UpstreamClient client;
    private final GatewayDiagnostics diagnostics;
    private final IdempotencyStore idempotencyStore;
    private final LoadBalancer loadBalancer;

    private final Map<String, UpstreamInstance> upstreamById = new LinkedHashMap<>();
    private final Map<String, ConcurrencyLimiter> concurrencyLimiters = new LinkedHashMap<>();
    private final Map<String, RateLimiter> rateLimiters = new LinkedHashMap<>();
    private final Map<String, CircuitBreaker> circuitBreakers = new LinkedHashMap<>();

    public GatewayService(GatewayProperties properties,
                          UpstreamClient client,
                          GatewayDiagnostics diagnostics,
                          IdempotencyStore idempotencyStore,
                          LoadBalancer loadBalancer) {
        this.properties = properties;
        this.client = client;
        this.diagnostics = diagnostics;
        this.idempotencyStore = idempotencyStore;
        this.loadBalancer = loadBalancer;
        for (UpstreamInstance upstream : properties.getUpstreams()) {
            upstreamById.put(upstream.id(), upstream);
            concurrencyLimiters.put(upstream.id(), new ConcurrencyLimiter(upstream.maxConcurrency()));
            rateLimiters.put(upstream.id(), new RateLimiter(upstream.rateLimitPerSecond()));
            circuitBreakers.put(upstream.id(), new CircuitBreaker(
                    upstream.id(),
                    properties.getCircuitFailureThreshold(),
                    properties.getCircuitOpenDurationMs(),
                    properties.getCircuitHalfOpenPermits()));
        }
    }

    /**
     * 处理一次业务请求。
     *
     * @param routeId        路由标识
     * @param path           请求路径（透传给上游）
     * @param idempotencyKey 幂等键；为 null 时由网关生成，同一业务请求的重试共用一个键
     * @return 上游响应体
     */
    public String handle(String routeId, String path, String idempotencyKey) throws GatewayException {
        RouteRule route = findRoute(routeId);
        if (route == null) {
            diagnostics.record("route miss: routeId=" + routeId);
            throw new GatewayException(GatewayErrorCode.ROUTE_NOT_FOUND, "no route for " + routeId);
        }
        List<UpstreamInstance> candidates = resolveCandidates(route);
        if (candidates.isEmpty()) {
            diagnostics.record("route decision: routeId=" + routeId + " no usable upstream configured");
            throw new GatewayException(GatewayErrorCode.NO_HEALTHY_UPSTREAM,
                    "route " + routeId + " has no usable upstream");
        }
        String key = idempotencyKey == null ? UUID.randomUUID().toString() : idempotencyKey;
        var cached = idempotencyStore.getIfPresent(key);
        if (cached.isPresent()) {
            diagnostics.record("idempotent-hit: key=" + key + " routeId=" + routeId);
            return cached.get();
        }
        diagnostics.record("route decision: routeId=" + routeId + " path=" + path
                + " candidates=" + candidateIds(candidates) + " key=" + key);

        // 在途去重：同键并发只有一个 leader 真正打上游；follower 等待复用结果，
        // 等待超时则快速返回与限流/熔断/上游故障都不同的“处理中”状态
        IdempotencyPrepareResult prepared =
                idempotencyStore.prepare(key, properties.getIdempotencyWaitMs());
        if (prepared.isCompleted()) {
            diagnostics.record("idempotent-hit: key=" + key + " routeId=" + routeId
                    + " via=inflight");
            return prepared.getBody();
        }
        if (prepared.isStillInFlight()) {
            diagnostics.record("idempotency in-flight fast-return: key=" + key
                    + " routeId=" + routeId);
            throw new GatewayException(GatewayErrorCode.IDEMPOTENCY_IN_PROGRESS,
                    "request with idempotency key '" + key + "' is still being processed",
                    null, 0, key);
        }
        IdempotencyLease lease = prepared.getLease();
        try {
            String body = executeWithFailover(routeId, path, key, candidates);
            idempotencyStore.complete(lease, body);
            return body;
        } catch (GatewayException e) {
            // leader 未成功（含 4xx 拒绝终态与各类可重试失败）：释放在途条目，
            // 唤醒的等待者可竞争成为新 leader；4xx 是否重试由 executeWithFailover 保证（不重试）
            idempotencyStore.fail(lease);
            throw e;
        }
    }

    /**
     * leader 执行：并发限流 -> 速率限流 -> 熔断许可 -> 调用上游，
     * 可重试错误（超时 / 连接失败 / 5xx）在整体超时预算内有限重试并故障转移；
     * 4xx 拒绝立即终态返回。
     */
    private String executeWithFailover(String routeId, String path, String key,
                                       List<UpstreamInstance> candidates) throws GatewayException {
        long deadlineNanos = System.nanoTime() + properties.getOverallTimeoutMs() * 1_000_000L;
        UpstreamResult.FailureKind lastFailure = null;
        String lastUpstreamId = null;
        int attemptsUsed = 0;
        int permitSkips = 0;
        // 负载均衡决定首个候选起点，之后按序轮转实现故障转移
        int nextIndex = Math.max(0, candidates.indexOf(loadBalancer.choose(candidates)));

        while (attemptsUsed <= properties.getMaxRetries()) {
            long remainingMs = remainingMs(deadlineNanos);
            if (remainingMs <= 0) {
                diagnostics.record("retry budget exhausted: routeId=" + routeId
                        + " attempts=" + attemptsUsed);
                throw new GatewayException(GatewayErrorCode.RETRY_BUDGET_EXHAUSTED,
                        "overall timeout budget exhausted after " + attemptsUsed + " attempts",
                        lastUpstreamId);
            }
            UpstreamInstance upstream = null;
            for (int i = 0; i < candidates.size(); i++) {
                UpstreamInstance candidate = candidates.get((nextIndex + i) % candidates.size());
                if (circuitBreakers.get(candidate.id()).getState() != CircuitState.OPEN) {
                    upstream = candidate;
                    nextIndex = (nextIndex + i + 1) % candidates.size();
                    break;
                }
            }
            if (upstream == null) {
                diagnostics.record("all circuits open: routeId=" + routeId
                        + " candidates=" + candidateIds(candidates));
                throw new GatewayException(GatewayErrorCode.CIRCUIT_OPEN,
                        "all upstream circuits are open for route " + routeId);
            }
            String uid = upstream.id();
            ConcurrencyLimiter concurrencyLimiter = concurrencyLimiters.get(uid);
            RateLimiter rateLimiter = rateLimiters.get(uid);
            CircuitBreaker breaker = circuitBreakers.get(uid);

            if (!concurrencyLimiter.tryAcquire()) {
                diagnostics.record("concurrency-limit reject: upstream=" + uid
                        + " inFlight=" + concurrencyLimiter.getInFlight()
                        + "/" + concurrencyLimiter.getMaxConcurrency());
                throw new GatewayException(GatewayErrorCode.CONCURRENCY_LIMIT_EXCEEDED,
                        "upstream " + uid + " concurrency limit exceeded", uid);
            }
            try {
                if (!rateLimiter.tryAcquire()) {
                    diagnostics.record("rate-limit reject: upstream=" + uid
                            + " permitsPerSecond=" + rateLimiter.getPermitsPerSecond());
                    throw new GatewayException(GatewayErrorCode.RATE_LIMIT_EXCEEDED,
                            "upstream " + uid + " rate limit exceeded", uid);
                }
                if (!breaker.tryAcquirePermit()) {
                    // 半开探测许可被占满等场景：换下一个实例，不消耗重试次数
                    permitSkips++;
                    diagnostics.record("circuit-open skip: upstream=" + uid
                            + " state=" + breaker.getState());
                    if (permitSkips > candidates.size()) {
                        throw new GatewayException(GatewayErrorCode.CIRCUIT_OPEN,
                                "no circuit permits available for route " + routeId, uid);
                    }
                    continue;
                }
                permitSkips = 0;
                attemptsUsed++;
                long attemptTimeout = Math.min(properties.getPerAttemptTimeoutMs(), remainingMs);
                diagnostics.record("attempt " + attemptsUsed + ": upstream=" + uid
                        + " breaker=" + breaker.getState()
                        + " inFlight=" + concurrencyLimiter.getInFlight()
                        + " budgetMs=" + remainingMs);
                UpstreamResult result = client.call(upstream, path, key, attemptTimeout);
                if (result.isSuccess()) {
                    breaker.onSuccess();
                    diagnostics.record("success: upstream=" + uid
                            + " attempts=" + attemptsUsed
                            + " latencyMs=" + result.getLatencyMs()
                            + " breaker=" + breaker.getState());
                    return result.getBody();
                }
                if (result.getFailureKind() == UpstreamResult.FailureKind.REJECTED) {
                    // 调用方原因（4xx）：不算熔断失败、不重试、不故障转移，直接终态返回
                    breaker.onRejected();
                    diagnostics.record("upstream rejected: status=" + result.getStatus()
                            + " upstream=" + uid + " attempt=" + attemptsUsed);
                    throw new GatewayException(GatewayErrorCode.UPSTREAM_REJECTED,
                            "upstream " + uid + " rejected the request as client error",
                            uid, result.getStatus(), result.getBody());
                }
                breaker.onFailure();
                lastFailure = result.getFailureKind();
                lastUpstreamId = uid;
                diagnostics.record("upstream failure: kind=" + result.getFailureKind()
                        + " upstream=" + uid
                        + " attempt=" + attemptsUsed
                        + " status=" + result.getStatus()
                        + " breaker=" + breaker.getState()
                        + " consecutiveFailures=" + breaker.getConsecutiveFailures());
            } finally {
                concurrencyLimiter.release();
            }
        }
        throw mapLastFailure(lastFailure, lastUpstreamId, attemptsUsed);
    }

    private GatewayException mapLastFailure(UpstreamResult.FailureKind lastFailure,
                                            String lastUpstreamId, int attemptsUsed) {
        diagnostics.record("retries exhausted: attempts=" + attemptsUsed
                + " lastFailure=" + lastFailure + " upstream=" + lastUpstreamId);
        if (lastFailure == null) {
            return new GatewayException(GatewayErrorCode.INTERNAL_ERROR, "no attempt executed");
        }
        return switch (lastFailure) {
            case TIMEOUT -> new GatewayException(GatewayErrorCode.UPSTREAM_TIMEOUT,
                    "upstream timeout after " + attemptsUsed + " attempts", lastUpstreamId);
            case CONNECT_FAILURE -> new GatewayException(GatewayErrorCode.UPSTREAM_CONNECT_FAILURE,
                    "upstream connect failure after " + attemptsUsed + " attempts", lastUpstreamId);
            case BUSINESS_ERROR -> new GatewayException(GatewayErrorCode.UPSTREAM_BUSINESS_ERROR,
                    "upstream business error after " + attemptsUsed + " attempts", lastUpstreamId);
            default -> new GatewayException(GatewayErrorCode.INTERNAL_ERROR, "unexpected");
        };
    }

    private RouteRule findRoute(String routeId) {
        for (RouteRule route : properties.getRoutes()) {
            if (route.routeId().equals(routeId)) {
                return route;
            }
        }
        return null;
    }

    private List<UpstreamInstance> resolveCandidates(RouteRule route) {
        List<UpstreamInstance> candidates = new ArrayList<>();
        for (String uid : route.upstreamIds()) {
            UpstreamInstance upstream = upstreamById.get(uid);
            if (upstream != null) {
                candidates.add(upstream);
            }
        }
        return candidates;
    }

    private long remainingMs(long deadlineNanos) {
        return (deadlineNanos - System.nanoTime()) / 1_000_000L;
    }

    private List<String> candidateIds(List<UpstreamInstance> candidates) {
        return candidates.stream().map(UpstreamInstance::id).toList();
    }

    /**
     * 只读查询幂等键状态：无记录返回 null（控制器映射 404），
     * 处理中返回 IN_FLIGHT 快照，已完成返回 COMPLETED 与结果体。
     */
    public IdempotencySnapshot queryStatus(String key) {
        return idempotencyStore.statusOf(key).orElse(null);
    }

    public CircuitBreaker getCircuitBreaker(String upstreamId) {        return circuitBreakers.get(upstreamId);
    }

    public ConcurrencyLimiter getConcurrencyLimiter(String upstreamId) {
        return concurrencyLimiters.get(upstreamId);
    }

    public RateLimiter getRateLimiter(String upstreamId) {
        return rateLimiters.get(upstreamId);
    }
}
