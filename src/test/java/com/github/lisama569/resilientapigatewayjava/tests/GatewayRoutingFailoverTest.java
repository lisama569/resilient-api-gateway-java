package com.github.lisama569.resilientapigatewayjava.tests;

import com.github.lisama569.resilientapigatewayjava.config.GatewayProperties;
import com.github.lisama569.resilientapigatewayjava.gateway.GatewayService;
import com.github.lisama569.resilientapigatewayjava.gateway.UpstreamClient;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 路由分发、故障转移、幂等去重与错误可区分性测试。
 */
class GatewayRoutingFailoverTest {

    private MockUpstreamServer upstreamA;
    private MockUpstreamServer upstreamB;
    private GatewayDiagnostics diagnostics;
    private IdempotencyStore idempotencyStore;
    private GatewayService gatewayService;

    @BeforeEach
    void setUp() {
        upstreamA = new MockUpstreamServer("upstream-a", 0);
        upstreamB = new MockUpstreamServer("upstream-b", 0);
        upstreamA.start();
        upstreamB.start();
        diagnostics = new GatewayDiagnostics();
        idempotencyStore = new IdempotencyStore();
        gatewayService = newService(defaultProperties());
    }

    @AfterEach
    void tearDown() {
        upstreamA.stop();
        upstreamB.stop();
    }

    private GatewayProperties defaultProperties() {
        GatewayProperties properties = new GatewayProperties();
        properties.setUpstreams(List.of(
                new UpstreamInstance("upstream-a", "127.0.0.1", upstreamA.getPort(), 10, 1000, 1),
                new UpstreamInstance("upstream-b", "127.0.0.1", upstreamB.getPort(), 10, 1000, 1)));
        properties.setRoutes(List.of(
                new RouteRule("orders", "/orders", List.of("upstream-a", "upstream-b"))));
        properties.setMaxRetries(2);
        properties.setOverallTimeoutMs(3000);
        properties.setPerAttemptTimeoutMs(1000);
        properties.setCircuitFailureThreshold(3);
        properties.setCircuitOpenDurationMs(2000);
        properties.setCircuitHalfOpenPermits(1);
        return properties;
    }

    private GatewayService newService(GatewayProperties properties) {
        return new GatewayService(properties, new UpstreamClient(), diagnostics,
                idempotencyStore, new RoundRobinLoadBalancer());
    }

    private void printDiagnostics(String scenario) {
        System.out.println("==== 诊断事件 [" + scenario + "] ====");
        diagnostics.getEvents().forEach(e -> System.out.println("  " + e));
    }

    @Test
    void routesToConfiguredUpstreams() throws Exception {
        String body = gatewayService.handle("orders", "/work", null);
        printDiagnostics("正常路由");
        assertTrue(body.contains("sideEffect=1"));
        assertEquals(1, upstreamA.getSideEffectCount() + upstreamB.getSideEffectCount());
    }

    @Test
    void failsOverToHealthyInstanceWhenPrimaryReturns500() throws Exception {
        upstreamA.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_500);
        String body = gatewayService.handle("orders", "/work", "key-failover");
        printDiagnostics("故障转移");
        assertTrue(body.contains("upstream=upstream-b"), "应故障转移到 upstream-b，实际: " + body);
        assertEquals(1, upstreamB.getSideEffectCount());
        assertEquals(0, upstreamA.getSideEffectCount(), "失败的 A 不应产生副作用");
        assertTrue(diagnostics.countEventsContaining("upstream failure: kind=BUSINESS_ERROR upstream=upstream-a") >= 1);
        assertTrue(diagnostics.countEventsContaining("success: upstream=upstream-b") >= 1);
    }

    @Test
    void duplicateDeliveryOfSameBusinessRequestHasNoDuplicateSideEffect() throws Exception {
        upstreamA.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_500);
        String key = "key-idempotent";
        String first = gatewayService.handle("orders", "/work", key);
        // 同一业务请求被客户端重复投递（相同幂等键）
        String second = gatewayService.handle("orders", "/work", key);
        printDiagnostics("幂等去重");
        assertEquals(first, second);
        assertEquals(1, upstreamA.getSideEffectCount() + upstreamB.getSideEffectCount(),
                "重复投递不得产生重复副作用");
        assertTrue(diagnostics.countEventsContaining("idempotent-hit: key=" + key) >= 1);
    }

    @Test
    void upstreamLevelIdempotencyDeduplicatesRetries() throws Exception {
        // 直接验证模拟上游的幂等表：同一键重复调用只计一次副作用
        UpstreamClient client = new UpstreamClient();
        var first = client.call(
                new UpstreamInstance("upstream-a", "127.0.0.1", upstreamA.getPort(), 10, 1000, 1),
                "/work", "key-upstream-idem", 1000);
        var second = client.call(
                new UpstreamInstance("upstream-a", "127.0.0.1", upstreamA.getPort(), 10, 1000, 1),
                "/work", "key-upstream-idem", 1000);
        assertTrue(first.isSuccess() && second.isSuccess());
        assertEquals(first.getBody(), second.getBody());
        assertEquals(1, upstreamA.getSideEffectCount());
        assertEquals(2, upstreamA.getTotalRequestCount(), "两次投递都到达上游但被去重");
    }

    @Test
    void routeNotFoundIsDistinguishable() {
        GatewayException e = assertThrows(GatewayException.class,
                () -> gatewayService.handle("unknown-route", "/work", null));
        printDiagnostics("路由不存在");
        assertEquals(GatewayErrorCode.ROUTE_NOT_FOUND, e.getCode());
    }

    @Test
    void upstreamBusinessErrorIsDistinguishableFromRoutingFailure() {
        upstreamA.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_500);
        upstreamB.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_500);
        GatewayException e = assertThrows(GatewayException.class,
                () -> gatewayService.handle("orders", "/work", "key-all-500"));
        printDiagnostics("全部上游业务错误");
        assertEquals(GatewayErrorCode.UPSTREAM_BUSINESS_ERROR, e.getCode());
        // maxRetries=2 -> 共 3 次尝试
        assertEquals(3, upstreamA.getTotalRequestCount() + upstreamB.getTotalRequestCount());
        assertTrue(diagnostics.countEventsContaining("retries exhausted: attempts=3") >= 1);
    }

    @Test
    void connectFailureFailsOverAndIsDistinguishable() throws Exception {
        // 停掉 A 模拟连接失败（端口关闭）
        int portA = upstreamA.getPort();
        upstreamA.stop();
        GatewayProperties properties = defaultProperties();
        properties.setRoutes(List.of(new RouteRule("orders", "/orders", List.of("upstream-a", "upstream-b"))));
        GatewayService service = newService(properties);

        String body = service.handle("orders", "/work", "key-conn-failover");
        printDiagnostics("连接失败故障转移");
        assertTrue(body.contains("upstream=upstream-b"));
        assertTrue(diagnostics.countEventsContaining("upstream failure: kind=CONNECT_FAILURE upstream=upstream-a") >= 1);

        // 单实例路由 + 连接失败 -> 对外为 UPSTREAM_CONNECT_FAILURE，区别于路由失败
        GatewayProperties single = defaultProperties();
        single.setRoutes(List.of(new RouteRule("orders", "/orders", List.of("upstream-a"))));
        GatewayService singleService = newService(single);
        GatewayException e = assertThrows(GatewayException.class,
                () -> singleService.handle("orders", "/work", "key-conn-single"));
        assertEquals(GatewayErrorCode.UPSTREAM_CONNECT_FAILURE, e.getCode());
        assertTrue(e.getCode() != GatewayErrorCode.ROUTE_NOT_FOUND
                && e.getCode() != GatewayErrorCode.NO_HEALTHY_UPSTREAM);
    }
}
