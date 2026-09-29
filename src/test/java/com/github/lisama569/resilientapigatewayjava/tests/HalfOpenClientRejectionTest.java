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

import java.util.ArrayList;
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
 * 回归场景：恢复试探（HALF_OPEN 探测）恰好收到调用方拒绝（4xx）时，
 * 探测许可必须被释放，上游不得被永久卡在不可用状态；
 * 上游恢复后正常请求必须能重新打进去。
 */
class HalfOpenClientRejectionTest {

    private MockUpstreamServer upstream;
    private GatewayDiagnostics diagnostics;
    private GatewayProperties properties;
    private GatewayService service;

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
        properties.setMaxRetries(0);
        properties.setOverallTimeoutMs(3000);
        properties.setPerAttemptTimeoutMs(1000);
        properties.setCircuitFailureThreshold(1);
        properties.setCircuitOpenDurationMs(400);
        properties.setCircuitHalfOpenPermits(1);
        service = newService(properties);
    }

    @AfterEach
    void tearDown() {
        upstream.stop();
    }

    private GatewayService newService(GatewayProperties p) {
        return new GatewayService(p, new UpstreamClient(), diagnostics,
                new IdempotencyStore(), new RoundRobinLoadBalancer());
    }

    @Test
    void halfOpenProbeClientRejectionDoesNotStickCircuit() throws Exception {
        // 1. 上游内部故障 -> 熔断
        upstream.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_500);
        GatewayException boom = assertThrows(GatewayException.class,
                () -> service.handle("orders", "/work", null));
        assertEquals(GatewayErrorCode.UPSTREAM_BUSINESS_ERROR, boom.getCode());
        assertEquals(CircuitState.OPEN, service.getCircuitBreaker("upstream-a").getState());

        // 2. 冷却结束，进入恢复试探；这次试探收到调用方拒绝（4xx）
        Thread.sleep(500);
        upstream.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_400);
        GatewayException rejected = assertThrows(GatewayException.class,
                () -> service.handle("orders", "/work", null));
        assertEquals(GatewayErrorCode.UPSTREAM_REJECTED, rejected.getCode(),
                "恢复试探阶段的 4xx 仍要按调用方拒绝表达");
        assertEquals(400, rejected.getUpstreamStatus());

        // 3. 上游彻底恢复：后续正常请求必须能重新打进去，不得被卡在熔断/半开
        upstream.setFailureMode(MockUpstreamServer.FailureMode.NONE);
        String body = service.handle("orders", "/work", null);
        assertTrue(body.contains("sideEffect"),
                "上游恢复后请求必须正常到达上游，实际响应=" + body);
        assertEquals(CircuitState.CLOSED, service.getCircuitBreaker("upstream-a").getState());
    }

    @Test
    void repeatedFaultCircuitProbeRejectionRecoveryCyclesLeaveNoResidualState() throws Exception {
        int rounds = 3;
        for (int round = 0; round < rounds; round++) {
            // 故障 -> 熔断
            upstream.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_500);
            assertThrows(GatewayException.class, () -> service.handle("orders", "/work", null));
            assertEquals(CircuitState.OPEN, service.getCircuitBreaker("upstream-a").getState(),
                    "第 " + round + " 轮：内部故障后应熔断");

            // 冷却 -> 恢复试探被拒（4xx）：不计失败、不重新熔断、不泄漏许可
            Thread.sleep(500);
            int failuresBeforeRejection =
                    service.getCircuitBreaker("upstream-a").getConsecutiveFailures();
            upstream.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_400);
            GatewayException rejected = assertThrows(GatewayException.class,
                    () -> service.handle("orders", "/work", null));
            assertEquals(GatewayErrorCode.UPSTREAM_REJECTED, rejected.getCode(),
                    "第 " + round + " 轮：试探期 4xx 必须按调用方拒绝表达");
            assertEquals(failuresBeforeRejection,
                    service.getCircuitBreaker("upstream-a").getConsecutiveFailures(),
                    "第 " + round + " 轮：调用方拒绝不得计入熔断失败");
            assertEquals(CircuitState.HALF_OPEN, service.getCircuitBreaker("upstream-a").getState(),
                    "第 " + round + " 轮：调用方拒绝不得重新熔断，也不得直接闭合");

            // 上游恢复 -> 正常流量重新打入，熔断器闭合且无残留
            upstream.setFailureMode(MockUpstreamServer.FailureMode.NONE);
            String body = service.handle("orders", "/work", null);
            assertTrue(body.contains("sideEffect"),
                    "第 " + round + " 轮：恢复后请求必须到达上游，实际=" + body);
            assertEquals(CircuitState.CLOSED, service.getCircuitBreaker("upstream-a").getState(),
                    "第 " + round + " 轮：恢复后熔断器必须闭合");
            assertEquals(0, service.getCircuitBreaker("upstream-a").getConsecutiveFailures(),
                    "第 " + round + " 轮：不得残留失败计数");
        }
        assertEquals(rounds, upstream.getSideEffectCount(),
                "每轮恢复后应各产生一次真实副作用，不得丢请求或重复");
    }

    @Test
    void concurrentProbesRejectedDuringHalfOpenDoNotCorruptState() throws Exception {
        // 熔断后进入冷却
        upstream.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_500);
        assertThrows(GatewayException.class, () -> service.handle("orders", "/work", null));
        assertEquals(CircuitState.OPEN, service.getCircuitBreaker("upstream-a").getState());
        Thread.sleep(500);

        // 恢复试探期间上游只回 4xx：并发打一波请求
        int failuresBeforeProbes = service.getCircuitBreaker("upstream-a").getConsecutiveFailures();
        upstream.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_400);
        int parallelism = 8;
        ExecutorService pool = Executors.newFixedThreadPool(parallelism);
        CountDownLatch gate = new CountDownLatch(1);
        AtomicInteger rejectedAsClientError = new AtomicInteger();
        AtomicInteger fastFailed = new AtomicInteger();
        AtomicInteger unexpected = new AtomicInteger();
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < parallelism; i++) {
                futures.add(pool.submit(() -> {
                    try {
                        gate.await();
                        service.handle("orders", "/work", null);
                        unexpected.incrementAndGet(); // 4xx 期间不应有成功
                    } catch (GatewayException e) {
                        if (e.getCode() == GatewayErrorCode.UPSTREAM_REJECTED) {
                            rejectedAsClientError.incrementAndGet();
                        } else if (e.getCode() == GatewayErrorCode.CIRCUIT_OPEN) {
                            fastFailed.incrementAndGet(); // 探测许可占满时的快速拒绝，允许
                        } else {
                            unexpected.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }));
            }
            gate.countDown();
            for (Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        assertEquals(0, unexpected.get(), "并发试探期间不得出现非预期结果");
        assertTrue(rejectedAsClientError.get() >= 1, "至少一个探测请求真正触达上游并被拒");
        assertTrue(upstream.getTotalRequestCount() >= 2, "被拒的探测必须真实到达过上游");
        assertEquals(failuresBeforeProbes,
                service.getCircuitBreaker("upstream-a").getConsecutiveFailures(),
                "调用方拒绝不得计入熔断失败");
        assertEquals(CircuitState.HALF_OPEN, service.getCircuitBreaker("upstream-a").getState(),
                "并发 4xx 试探不得重新熔断，也不得直接闭合");

        // 上游恢复：不得有任何残留状态，正常流量必须恢复
        upstream.setFailureMode(MockUpstreamServer.FailureMode.NONE);
        String body = service.handle("orders", "/work", null);
        assertTrue(body.contains("sideEffect"), "恢复后请求必须到达上游，实际=" + body);
        assertEquals(CircuitState.CLOSED, service.getCircuitBreaker("upstream-a").getState());
    }
}
