package com.github.lisama569.resilientapigatewayjava.tests;

import com.github.lisama569.resilientapigatewayjava.config.GatewayProperties;
import com.github.lisama569.resilientapigatewayjava.gateway.GatewayService;
import com.github.lisama569.resilientapigatewayjava.gateway.UpstreamClient;
import com.github.lisama569.resilientapigatewayjava.gateway.circuit.CircuitState;
import com.github.lisama569.resilientapigatewayjava.gateway.diag.GatewayDiagnostics;
import com.github.lisama569.resilientapigatewayjava.gateway.error.GatewayErrorCode;
import com.github.lisama569.resilientapigatewayjava.gateway.error.GatewayException;
import com.github.lisama569.resilientapigatewayjava.gateway.route.RouteRule;
import com.github.lisama569.resilientapigatewayjava.gateway.route.RoundRobinLoadBalancer;
import com.github.lisama569.resilientapigatewayjava.idempotency.IdempotencyStore;
import com.github.lisama569.resilientapigatewayjava.upstream.MockUpstreamServer;
import com.github.lisama569.resilientapigatewayjava.upstream.UpstreamInstance;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 上游调用方拒绝（4xx）与上游内部故障（5xx）必须分开对外表达：
 * 4xx 不重试、不故障转移、不计入熔断失败；5xx 维持可重试/可转移/计入熔断。
 */
class UpstreamRejectionTest {

    private MockUpstreamServer upstreamA;
    private MockUpstreamServer upstreamB;
    private GatewayDiagnostics diagnostics;
    private GatewayService service;
    private GatewayProperties properties;

    @BeforeEach
    void setUp() {
        upstreamA = new MockUpstreamServer("upstream-a", 0);
        upstreamB = new MockUpstreamServer("upstream-b", 0);
        upstreamA.start();
        upstreamB.start();
        diagnostics = new GatewayDiagnostics();
        properties = new GatewayProperties();
        properties.setUpstreams(List.of(
                new UpstreamInstance("upstream-a", "127.0.0.1", upstreamA.getPort(), 10, 1000, 1),
                new UpstreamInstance("upstream-b", "127.0.0.1", upstreamB.getPort(), 10, 1000, 1)));
        properties.setRoutes(List.of(
                new RouteRule("orders", "/orders", List.of("upstream-a", "upstream-b"))));
        properties.setMaxRetries(2);
        properties.setOverallTimeoutMs(3000);
        properties.setPerAttemptTimeoutMs(1000);
        properties.setCircuitFailureThreshold(1);
        properties.setCircuitOpenDurationMs(400);
        properties.setCircuitHalfOpenPermits(1);
        service = new GatewayService(properties, new UpstreamClient(), diagnostics,
                new IdempotencyStore(), new RoundRobinLoadBalancer());
    }

    @AfterEach
    void tearDown() {
        upstreamA.stop();
        upstreamB.stop();
    }

    @Test
    void clientRejectionIsExpressedSeparatelyFromInternalUpstreamFailure() {
        upstreamA.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_400);
        upstreamB.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_400);
        GatewayException rejected = assertThrows(GatewayException.class,
                () -> service.handle("orders", "/work", "key-4xx"));
        assertEquals(GatewayErrorCode.UPSTREAM_REJECTED, rejected.getCode());
        assertEquals(400, rejected.getUpstreamStatus());
        assertTrue(rejected.getMessage().contains("rejected"));

        // 用独立服务对比 5xx（高熔断阈值，避免 3 次重试把两个实例打熔后变成 CIRCUIT_OPEN）
        GatewayProperties fivexxProps = new GatewayProperties();
        fivexxProps.setUpstreams(properties.getUpstreams());
        fivexxProps.setRoutes(properties.getRoutes());
        fivexxProps.setMaxRetries(2);
        fivexxProps.setOverallTimeoutMs(3000);
        fivexxProps.setPerAttemptTimeoutMs(1000);
        fivexxProps.setCircuitFailureThreshold(10);
        fivexxProps.setCircuitOpenDurationMs(400);
        fivexxProps.setCircuitHalfOpenPermits(1);
        GatewayService internalService = new GatewayService(fivexxProps, new UpstreamClient(),
                new GatewayDiagnostics(), new IdempotencyStore(), new RoundRobinLoadBalancer());
        upstreamA.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_500);
        upstreamB.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_500);
        GatewayException internal = assertThrows(GatewayException.class,
                () -> internalService.handle("orders", "/work", "key-5xx"));
        assertEquals(GatewayErrorCode.UPSTREAM_BUSINESS_ERROR, internal.getCode());
        assertNotEquals(GatewayErrorCode.UPSTREAM_REJECTED, internal.getCode());
    }

    @Test
    void clientRejectionIsNotRetriedAndNotFailedOver() {
        upstreamA.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_400);
        upstreamB.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_400);
        GatewayException e = assertThrows(GatewayException.class,
                () -> service.handle("orders", "/work", "key-4xx-no-retry"));
        assertEquals(GatewayErrorCode.UPSTREAM_REJECTED, e.getCode());
        // 无论轮询先落到 A 还是 B：只能触及一个实例一次，不得重试或故障转移
        int hits = upstreamA.getTotalRequestCount() + upstreamB.getTotalRequestCount();
        assertEquals(1, hits, "4xx 必须立即返回，不得重试/故障转移，实际触达=" + hits);
        assertEquals(1, Math.max(upstreamA.getTotalRequestCount(), upstreamB.getTotalRequestCount()));
        assertEquals(0, Math.min(upstreamA.getTotalRequestCount(), upstreamB.getTotalRequestCount()),
                "另一个实例不应被故障转移触及");
        assertEquals(0, upstreamA.getSideEffectCount() + upstreamB.getSideEffectCount());
    }

    @Test
    void clientRejectionDoesNotCountTowardCircuitBreaker() throws Exception {
        // 阈值为 1：若是内部错误一次就熔断；4xx 连续多次后熔断器必须仍 CLOSED
        upstreamA.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_400);
        upstreamB.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_400);
        for (int i = 0; i < 4; i++) {
            final String key = "key-4xx-breaker-" + i;
            GatewayException e = assertThrows(GatewayException.class,
                    () -> service.handle("orders", "/work", key));
            assertEquals(GatewayErrorCode.UPSTREAM_REJECTED, e.getCode());
        }
        assertEquals(CircuitState.CLOSED, service.getCircuitBreaker("upstream-a").getState(),
                "调用方拒绝不得计入熔断失败");
        assertEquals(CircuitState.CLOSED, service.getCircuitBreaker("upstream-b").getState(),
                "调用方拒绝不得计入熔断失败");

        // 恢复后业务正常，不受影响
        upstreamA.setFailureMode(MockUpstreamServer.FailureMode.NONE);
        upstreamB.setFailureMode(MockUpstreamServer.FailureMode.NONE);
        String body = service.handle("orders", "/work", "key-normal-after-4xx");
        assertTrue(body.contains("sideEffect"));
    }

    @Test
    void normalBusinessResultUnaffectedByRejectionSemantics() throws Exception {
        // 正常请求直接成功
        String body = service.handle("orders", "/work", "key-happy");
        assertTrue(body.contains("upstream=upstream-a") || body.contains("upstream=upstream-b"));
        assertEquals(1, upstreamA.getSideEffectCount() + upstreamB.getSideEffectCount());

        // 5xx 仍按旧语义重试/故障转移并计入熔断（与 4xx 的 UPSTREAM_REJECTED 严格区分）
        upstreamA.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_500);
        upstreamB.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_500);
        GatewayException e = assertThrows(GatewayException.class,
                () -> service.handle("orders", "/work", "key-5xx-breaks"));
        assertTrue(e.getCode() == GatewayErrorCode.UPSTREAM_BUSINESS_ERROR
                        || e.getCode() == GatewayErrorCode.CIRCUIT_OPEN,
                "5xx 应表达为上游内部错误/熔断，实际=" + e.getCode());
        assertNotEquals(GatewayErrorCode.UPSTREAM_REJECTED, e.getCode());
        assertTrue(service.getCircuitBreaker("upstream-a").getConsecutiveFailures() >= 1
                || service.getCircuitBreaker("upstream-b").getConsecutiveFailures() >= 1,
                "5xx 必须计入熔断失败统计");

        // 两个实例都恢复后业务正常
        upstreamA.setFailureMode(MockUpstreamServer.FailureMode.NONE);
        upstreamB.setFailureMode(MockUpstreamServer.FailureMode.NONE);
        Thread.sleep(500); // 等待熔断冷却（open-duration=400ms）
        String recovered = service.handle("orders", "/work", "key-recovered");
        assertTrue(recovered.contains("sideEffect"));
    }
}
