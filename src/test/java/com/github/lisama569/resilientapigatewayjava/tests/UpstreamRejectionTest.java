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
 * 上游受理前拒绝（调用方原因 4xx）与上游内部故障（5xx）分开表达：
 * 拒绝不重试、不故障转移、不计入熔断失败；正常业务结果不受影响。
 */
class UpstreamRejectionTest {

    private MockUpstreamServer upstreamA;
    private MockUpstreamServer upstreamB;
    private GatewayDiagnostics diagnostics;
    private GatewayService service;

    @BeforeEach
    void setUp() {
        upstreamA = new MockUpstreamServer("upstream-a", 0);
        upstreamB = new MockUpstreamServer("upstream-b", 0);
        upstreamA.start();
        upstreamB.start();
        diagnostics = new GatewayDiagnostics();
        service = newService(properties());
    }

    @AfterEach
    void tearDown() {
        upstreamA.stop();
        upstreamB.stop();
    }

    private GatewayProperties properties() {
        GatewayProperties properties = new GatewayProperties();
        properties.setUpstreams(List.of(
                new UpstreamInstance("upstream-a", "127.0.0.1", upstreamA.getPort(), 10, 10000, 1),
                new UpstreamInstance("upstream-b", "127.0.0.1", upstreamB.getPort(), 10, 10000, 1)));
        properties.setRoutes(List.of(
                new RouteRule("orders", "/orders", List.of("upstream-a", "upstream-b"))));
        properties.setMaxRetries(2);
        properties.setOverallTimeoutMs(3000);
        properties.setPerAttemptTimeoutMs(1000);
        properties.setCircuitFailureThreshold(2);
        properties.setCircuitOpenDurationMs(2000);
        properties.setCircuitHalfOpenPermits(1);
        properties.setIdempotencyWaitMs(1000);
        return properties;
    }

    private GatewayService newService(GatewayProperties properties) {
        return new GatewayService(properties, new UpstreamClient(), diagnostics,
                new IdempotencyStore(), new RoundRobinLoadBalancer());
    }

    /** 单上游路由：所有拒绝都直达 A，便于验证不重试 / 不计熔断。 */
    private GatewayService singleUpstreamService() {
        GatewayProperties properties = properties();
        properties.setRoutes(List.of(
                new RouteRule("orders", "/orders", List.of("upstream-a"))));
        return newService(properties);
    }

    private void printDiag(String scenario) {
        System.out.println("==== 诊断事件 [" + scenario + "] ====");
        diagnostics.getEvents().forEach(e -> System.out.println("  " + e));
    }

    @Test
    void clientRejectionReturnsDistinctCodeWithoutRetryOrFailover() {
        upstreamA.setFailureMode(MockUpstreamServer.FailureMode.REJECT_400);
        upstreamB.setFailureMode(MockUpstreamServer.FailureMode.REJECT_400);

        GatewayException e = assertThrows(GatewayException.class,
                () -> service.handle("orders", "/work", "key-reject-1"));
        printDiag("调用方原因被拒绝");

        // 与上游内部故障（UPSTREAM_BUSINESS_ERROR）明确区分
        assertEquals(GatewayErrorCode.UPSTREAM_REJECTED, e.getCode());
        assertNotEquals(GatewayErrorCode.UPSTREAM_BUSINESS_ERROR, e.getCode());
        // 不重试、不故障转移：只打一次，且只打被选中的那一个实例
        assertEquals(1, upstreamA.getTotalRequestCount() + upstreamB.getTotalRequestCount(),
                "4xx 拒绝必须立即终态，不得重试或故障转移");
        assertTrue(diagnostics.countEventsContaining("upstream rejected:") == 1);
        assertEquals(0, diagnostics.countEventsContaining("attempt 2:"));
    }

    @Test
    void clientRejectionDoesNotCountTowardCircuitBreaker() {
        GatewayService single = singleUpstreamService();
        upstreamA.setFailureMode(MockUpstreamServer.FailureMode.REJECT_400);

        // 阈值只有 2：若错误地计入熔断失败，两次拒绝后 A 应已熔断；实际不应
        for (int i = 0; i < 5; i++) {
            String k = "key-reject-cb-" + i;
            GatewayException e = assertThrows(GatewayException.class,
                    () -> single.handle("orders", "/work", k));
            assertEquals(GatewayErrorCode.UPSTREAM_REJECTED, e.getCode());
        }
        printDiag("拒绝不计熔断失败");
        assertEquals(CircuitState.CLOSED, single.getCircuitBreaker("upstream-a").getState(),
                "调用方拒绝是上游健康响应，不得把熔断器打到打开");
        // 每次拒绝都直达 A，没有被熔断快速拒绝
        assertEquals(5, upstreamA.getTotalRequestCount());
    }

    @Test
    void rejectionCarriesUpstreamStatusAndDetail() {
        GatewayService single = singleUpstreamService();
        upstreamA.setFailureMode(MockUpstreamServer.FailureMode.REJECT_400);
        GatewayException e = assertThrows(GatewayException.class,
                () -> single.handle("orders", "/work", "key-reject-detail"));

        assertEquals("upstream-a", e.getUpstreamId());
        assertEquals(400, e.getUpstreamStatus());
        assertTrue(e.getDetail() != null && e.getDetail().contains("invalid request"),
                "应携带上游拒绝详情，实际: " + e.getDetail());
        printDiag("拒绝携带状态与详情");
    }

    @Test
    void rejectedRequestCanBeResubmittedAfterFixAndSucceeds() throws Exception {
        GatewayService single = singleUpstreamService();
        upstreamA.setFailureMode(MockUpstreamServer.FailureMode.REJECT_400);
        // 被拒绝的键不缓存失败结果：调用方改好请求后用同一键重投，应正常成功且只处理一次
        String key = "key-reject-then-fix";
        GatewayException rejected = assertThrows(GatewayException.class,
                () -> single.handle("orders", "/work", key));
        assertEquals(GatewayErrorCode.UPSTREAM_REJECTED, rejected.getCode());

        upstreamA.setFailureMode(MockUpstreamServer.FailureMode.NONE);
        String body = single.handle("orders", "/work", key);
        assertTrue(body.contains("sideEffect=1"));
        printDiag("拒绝后改请求重投成功");

        // 再次同键投递复用成功结果，不产生第二次处理
        String again = single.handle("orders", "/work", key);
        assertEquals(body, again);
        assertEquals(1, upstreamA.getSideEffectCount());
    }
}
