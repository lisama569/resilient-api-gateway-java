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
import com.github.lisama569.resilientapigatewayjava.idempotency.IdempotencyStore;
import com.github.lisama569.resilientapigatewayjava.upstream.UpstreamInstance;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 网关核心编排：
 * 路由匹配 -> 幂等单飞占位（同键并发仅一个真实处理者）->
 * 逐次尝试（并发限流 -> 速率限流 -> 熔断许可 -> 调用上游）
 * -> 可重试错误在整体超时预算内有限重试并故障转移到健康实例。
 *
 * <p>同一业务请求共用同一幂等键：先到者真实处理，晚到者等待同一结果或收到
 * 可区分的“处理中”快速失败；上游 4xx 拒绝不重试、不转移、不计入熔断。
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

        // 未显式携带幂等键的请求走独立执行路径：键唯一，不参与单飞合并，行为与旧版一致
        if (idempotencyKey == null) {
            return processAsOwner(routeId, path, key, candidates, null);
        }

        // 携带幂等键：原子占位，保证同键并发只有一个真实处理者
        for (int round = 0; round < 2; round++) {
            IdempotencyStore.AcquireOutcome outcome = idempotencyStore.acquire(key);
            if (outcome.isCompletedHit()) {
                diagnostics.record("idempotent-hit: key=" + key + " routeId=" + routeId);
                return outcome.getBody();
            }
            if (outcome.isAcquired()) {
                return processAsOwner(routeId, path, key, candidates, outcome.getEntry());
            }
            // 晚到者：等待在途处理结果
            diagnostics.record("idempotent-wait: key=" + key + " routeId=" + routeId
                    + " waitMs=" + properties.getIdempotentWaitMs());
            IdempotencyStore.AwaitOutcome awaited;
            try {
                awaited = outcome.getEntry().await(properties.getIdempotentWaitMs());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new GatewayException(GatewayErrorCode.INTERNAL_ERROR, "interrupted while waiting");
            }
            switch (awaited.getKind()) {
                case SUCCESS -> {
                    diagnostics.record("idempotent-wait-result: key=" + key + " routeId=" + routeId);
                    return awaited.getBody();
                }
                case TIMEOUT -> throw new GatewayException(GatewayErrorCode.IDEMPOTENT_PROCESSING,
                        "request with idempotency key " + key + " is still being processed; "
                                + "query the status endpoint or retry later");
                case FAILED -> {
                    // owner 失败已释放占位：第一轮允许本线程重新占位接管处理，避免晚到者盲目报错
                    diagnostics.record("idempotent-owner-failed-reacquire: key=" + key
                            + " routeId=" + routeId + " round=" + round);
                    if (round == 0) {
                        continue;
                    }
                    throw new GatewayException(GatewayErrorCode.IDEMPOTENT_PROCESSING,
                            "request with idempotency key " + key + " has no settled result yet; "
                                    + "query the status endpoint or retry later");
                }
            }
        }
        throw new GatewayException(GatewayErrorCode.IDEMPOTENT_PROCESSING,
                "request with idempotency key " + key + " is still being processed");
    }

    /**
     * 只读查询幂等键处理状态：绝不触发上游调用。
     */
    public IdempotencyStore.QueryResult queryIdempotent(String key) {
        return idempotencyStore.query(key);
    }

    /**
     * owner 执行路径：真实发起上游尝试。成功发布幂等结果；任何失败都释放占位，
     * 使后续同键请求可以重新发起（失败本身向上抛出，由调用方决定是否再来）。
     */
    private String processAsOwner(String routeId, String path, String key,
                                  List<UpstreamInstance> candidates,
                                  IdempotencyStore.Entry entry) throws GatewayException {
        diagnostics.record("route decision: routeId=" + routeId + " path=" + path
                + " candidates=" + candidateIds(candidates) + " key=" + key
                + (entry == null ? " (ephemeral key)" : " (idempotency owner)"));
        try {
            String body = executeAttempts(routeId, path, key, candidates);
            idempotencyStore.complete(key, body);
            return body;
        } catch (GatewayException e) {
            if (entry != null) {
                idempotencyStore.fail(key, entry);
            }
            throw e;
        }
    }

    private String executeAttempts(String routeId, String path, String key,
                                  List<UpstreamInstance> candidates) throws GatewayException {
        long deadlineNanos = System.nanoTime() + properties.getOverallTimeoutMs() * 1_000_000L;
        UpstreamResult.FailureKind lastFailure = null;
        String lastUpstreamId = null;
        Integer lastUpstreamStatus = null;
        String lastDetail = null;
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
                if (result.getFailureKind() == UpstreamResult.FailureKind.CLIENT_REJECTED) {
                    // 调用方自身原因：不重试、不故障转移、不计入熔断失败，直接区分表达；
                    // 必须释放半开探测许可，否则许可泄漏会把上游永久卡在 HALF_OPEN
                    breaker.onRejected();
                    diagnostics.record("upstream client rejection: upstream=" + uid
                            + " status=" + result.getStatus()
                            + " attempt=" + attemptsUsed
                            + " breaker=" + breaker.getState());
                    throw new GatewayException(GatewayErrorCode.UPSTREAM_REJECTED,
                            "upstream " + uid + " rejected the request as client error; "
                                    + "fix the request instead of retrying",
                            uid, result.getStatus(), result.getBody());
                }
                breaker.onFailure();
                lastFailure = result.getFailureKind();
                lastUpstreamId = uid;
                lastUpstreamStatus = result.getStatus();
                lastDetail = result.getBody();
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
        throw mapLastFailure(lastFailure, lastUpstreamId, lastUpstreamStatus, lastDetail, attemptsUsed);
    }

    private GatewayException mapLastFailure(UpstreamResult.FailureKind lastFailure,
                                            String lastUpstreamId,
                                            Integer lastUpstreamStatus,
                                            String lastDetail,
                                            int attemptsUsed) {
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
                    "upstream internal error after " + attemptsUsed + " attempts",
                    lastUpstreamId, lastUpstreamStatus, lastDetail);
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

    public CircuitBreaker getCircuitBreaker(String upstreamId) {
        return circuitBreakers.get(upstreamId);
    }

    public ConcurrencyLimiter getConcurrencyLimiter(String upstreamId) {
        return concurrencyLimiters.get(upstreamId);
    }

    public RateLimiter getRateLimiter(String upstreamId) {
        return rateLimiters.get(upstreamId);
    }
}
