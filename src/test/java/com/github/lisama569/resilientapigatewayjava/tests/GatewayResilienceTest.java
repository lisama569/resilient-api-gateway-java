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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 超时归一化、重试预算、并发/速率限流与熔断恢复测试。
 */
class GatewayResilienceTest {

    private MockUpstreamServer upstream;
    private GatewayDiagnostics diagnostics;
    private GatewayService gatewayService;
    private GatewayProperties properties;

    @BeforeEach
    void setUp() {
        upstream = new MockUpstreamServer("upstream-a", 0);
        upstream.start();
        diagnostics = new GatewayDiagnostics();
        properties = new GatewayProperties();
        properties.setUpstreams(List.of(
                new UpstreamInstance("upstream-a", "127.0.0.1", upstream.getPort(), 10, 1000, 1)));
        properties.setRoutes(List.of(
                new RouteRule("orders", "/orders", List.of("upstream-a"))));
        properties.setMaxRetries(1);
        properties.setOverallTimeoutMs(3000);
        properties.setPerAttemptTimeoutMs(300);
        properties.setCircuitFailureThreshold(3);
        properties.setCircuitOpenDurationMs(400);
        properties.setCircuitHalfOpenPermits(1);
        gatewayService = newService(properties);
    }

    @AfterEach
    void tearDown() {
        upstream.stop();
    }

    private GatewayService newService(GatewayProperties p) {
        return new GatewayService(p, new UpstreamClient(), diagnostics,
                new IdempotencyStore(), new RoundRobinLoadBalancer());
    }

    private void printDiagnostics(String scenario) {
        System.out.println("==== 诊断事件 [" + scenario + "] ====");
        diagnostics.getEvents().forEach(e -> System.out.println("  " + e));
    }

    @Test
    void timeoutIsNormalizedAndRetriedWithLimitedAttempts() {
        upstream.setFailureMode(MockUpstreamServer.FailureMode.HANG);
        GatewayException e = assertThrows(GatewayException.class,
                () -> gatewayService.handle("orders", "/work", "key-timeout"));
        printDiagnostics("超时归一化");
        assertEquals(GatewayErrorCode.UPSTREAM_TIMEOUT, e.getCode());
        // maxRetries=1 -> 共 2 次尝试，每次 300ms
        assertTrue(diagnostics.countEventsContaining("upstream failure: kind=TIMEOUT") == 2);
        assertTrue(diagnostics.countEventsContaining("attempt 2:") == 1);
    }

    @Test
    void retriesAreBoundedByOverallTimeoutBudget() {
        upstream.setFailureMode(MockUpstreamServer.FailureMode.HANG);
        properties.setMaxRetries(5);
        properties.setOverallTimeoutMs(450);
        properties.setPerAttemptTimeoutMs(300);
        GatewayService service = newService(properties);

        long start = System.nanoTime();
        GatewayException e = assertThrows(GatewayException.class,
                () -> service.handle("orders", "/work", "key-budget"));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
        printDiagnostics("整体超时预算");
        assertEquals(GatewayErrorCode.RETRY_BUDGET_EXHAUSTED, e.getCode());
        assertTrue(elapsedMs < 2000, "重试必须受整体预算约束，实际耗时 " + elapsedMs + "ms");
        assertTrue(diagnostics.countEventsContaining("retry budget exhausted") >= 1);
    }

    @Test
    void concurrencyLimitRejectsExcessRequestsFast() throws Exception {
        upstream.setLatencyMs(400);
        properties.setPerAttemptTimeoutMs(2000);
        properties.setUpstreams(List.of(
                new UpstreamInstance("upstream-a", "127.0.0.1", upstream.getPort(), 2, 1000, 1)));
        GatewayService service = newService(properties);

        int parallelism = 3;
        ExecutorService pool = Executors.newFixedThreadPool(parallelism);
        CountDownLatch gate = new CountDownLatch(1);
        AtomicInteger success = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        try {
            Future<?>[] futures = new Future<?>[parallelism];
            for (int i = 0; i < parallelism; i++) {
                futures[i] = pool.submit(() -> {
                    try {
                        gate.await();
                        service.handle("orders", "/work", null);
                        success.incrementAndGet();
                    } catch (GatewayException e) {
                        if (e.getCode() == GatewayErrorCode.CONCURRENCY_LIMIT_EXCEEDED) {
                            rejected.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            gate.countDown();
            for (Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        printDiagnostics("并发限流");
        assertEquals(2, success.get(), "应放行 2 个并发请求");
        assertEquals(1, rejected.get(), "超出并发的请求应快速拒绝");
        assertTrue(diagnostics.countEventsContaining("concurrency-limit reject") >= 1);
    }

    @Test
    void rateLimitRejectsExcessRequests() {
        properties.setUpstreams(List.of(
                new UpstreamInstance("upstream-a", "127.0.0.1", upstream.getPort(), 10, 2, 1)));
        GatewayService service = newService(properties);

        int success = 0;
        int rejected = 0;
        for (int i = 0; i < 4; i++) {
            try {
                service.handle("orders", "/work", null);
                success++;
            } catch (GatewayException e) {
                assertEquals(GatewayErrorCode.RATE_LIMIT_EXCEEDED, e.getCode());
                rejected++;
            }
        }
        printDiagnostics("速率限流");
        assertEquals(2, success, "桶容量 2，应放行 2 个请求");
        assertEquals(2, rejected, "超出速率的请求应快速拒绝");
        assertTrue(diagnostics.countEventsContaining("rate-limit reject") == 2);
    }

    @Test
    void circuitOpensAfterConsecutiveFailuresAndRecoversViaProbe() throws Exception {
        upstream.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_500);
        properties.setMaxRetries(0);
        GatewayService service = newService(properties);

        // 连续 3 次失败触发熔断
        for (int i = 0; i < 3; i++) {
            GatewayException e = assertThrows(GatewayException.class,
                    () -> service.handle("orders", "/work", null));
            assertEquals(GatewayErrorCode.UPSTREAM_BUSINESS_ERROR, e.getCode());
        }
        assertEquals(CircuitState.OPEN, service.getCircuitBreaker("upstream-a").getState());
        int requestsAtOpen = upstream.getTotalRequestCount();

        // 熔断中：快速拒绝，不再打到上游
        GatewayException openError = assertThrows(GatewayException.class,
                () -> service.handle("orders", "/work", null));
        assertEquals(GatewayErrorCode.CIRCUIT_OPEN, openError.getCode());
        assertEquals(requestsAtOpen, upstream.getTotalRequestCount(), "熔断期间不得再调用上游");

        // 冷却期结束 + 上游恢复：半开探测成功后闭合
        Thread.sleep(500);
        upstream.setFailureMode(MockUpstreamServer.FailureMode.NONE);
        String body = service.handle("orders", "/work", null);
        assertTrue(body.contains("sideEffect"));
        assertEquals(CircuitState.CLOSED, service.getCircuitBreaker("upstream-a").getState());
        printDiagnostics("熔断与恢复");
        assertTrue(diagnostics.countEventsContaining("all circuits open") >= 1);
    }

    @Test
    void halfOpenProbeFailureReopensCircuit() throws Exception {
        upstream.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_500);
        properties.setMaxRetries(0);
        properties.setCircuitFailureThreshold(1);
        GatewayService service = newService(properties);

        assertThrows(GatewayException.class, () -> service.handle("orders", "/work", null));
        assertEquals(CircuitState.OPEN, service.getCircuitBreaker("upstream-a").getState());

        // 冷却期结束后探测仍失败 -> 重新熔断
        Thread.sleep(500);
        assertThrows(GatewayException.class, () -> service.handle("orders", "/work", null));
        assertEquals(CircuitState.OPEN, service.getCircuitBreaker("upstream-a").getState());
        int requestsAfterProbe = upstream.getTotalRequestCount();

        GatewayException e = assertThrows(GatewayException.class,
                () -> service.handle("orders", "/work", null));
        assertEquals(GatewayErrorCode.CIRCUIT_OPEN, e.getCode());
        assertEquals(requestsAfterProbe, upstream.getTotalRequestCount(), "重新熔断后不得再调用上游");
        printDiagnostics("半开探测失败重新熔断");
    }

    @Test
    void halfOpenAllowsOnlyLimitedProbesUnderConcurrency() throws Exception {
        upstream.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_500);
        properties.setMaxRetries(0);
        properties.setCircuitFailureThreshold(1);
        properties.setCircuitHalfOpenPermits(1);
        properties.setPerAttemptTimeoutMs(2000);
        GatewayService service = newService(properties);

        assertThrows(GatewayException.class, () -> service.handle("orders", "/work", null));
        assertEquals(CircuitState.OPEN, service.getCircuitBreaker("upstream-a").getState());

        Thread.sleep(500);
        upstream.setFailureMode(MockUpstreamServer.FailureMode.NONE);
        upstream.setLatencyMs(400);

        // 第一个请求占住唯一的半开探测许可
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<String> probe = pool.submit(() -> service.handle("orders", "/work", null));
            // 等探测请求真正到达上游（探测许可已被占用）
            long deadline = System.nanoTime() + 5_000_000_000L;
            while (upstream.getTotalRequestCount() < 2 && System.nanoTime() < deadline) {
                Thread.sleep(5);
            }
            assertEquals(2, upstream.getTotalRequestCount(), "探测请求应已到达上游");
            assertEquals(CircuitState.HALF_OPEN, service.getCircuitBreaker("upstream-a").getState());

            // 并发第二个请求：半开许可已占满，必须拒绝而非误放行
            GatewayException e = assertThrows(GatewayException.class,
                    () -> service.handle("orders", "/work", null));
            assertEquals(GatewayErrorCode.CIRCUIT_OPEN, e.getCode());
            assertEquals(2, upstream.getTotalRequestCount(), "半开期间并发请求不得打到上游");

            String probeBody = probe.get(10, TimeUnit.SECONDS);
            assertTrue(probeBody.contains("sideEffect"));
            assertEquals(CircuitState.CLOSED, service.getCircuitBreaker("upstream-a").getState());
        } finally {
            pool.shutdownNow();
        }
        printDiagnostics("半开并发探测");
    }
}
