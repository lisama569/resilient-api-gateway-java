package com.github.lisama569.resilientapigatewayjava.tests;

import com.github.lisama569.resilientapigatewayjava.config.GatewayProperties;
import com.github.lisama569.resilientapigatewayjava.gateway.GatewayService;
import com.github.lisama569.resilientapigatewayjava.gateway.UpstreamClient;
import com.github.lisama569.resilientapigatewayjava.gateway.circuit.CircuitBreaker;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 恢复试探（HALF_OPEN）期间收到调用方拒绝（4xx）不得把上游永久卡死：
 * 探测许可必须释放、熔断器不得重新打开；上游真正恢复后请求必须能重新透传。
 * 覆盖单轮、连续多轮、并发试探与并发混合（4xx + 成功）场景。
 */
class HalfOpenRejectionRecoveryTest {

    /** 并发派发结果：成功数与各错误码计数。 */
    private static class DispatchResult {
        final AtomicInteger success = new AtomicInteger();
        final ConcurrentHashMap<GatewayErrorCode, AtomicInteger> errors = new ConcurrentHashMap<>();

        int errorCount(GatewayErrorCode code) {
            return errors.getOrDefault(code, new AtomicInteger()).get();
        }
    }

    private MockUpstreamServer upstream;
    private GatewayDiagnostics diagnostics;
    private GatewayProperties properties;

    @BeforeEach
    void setUp() {
        upstream = new MockUpstreamServer("upstream-a", 0);
        upstream.start();
        diagnostics = new GatewayDiagnostics();
        properties = new GatewayProperties();
        properties.setUpstreams(List.of(
                new UpstreamInstance("upstream-a", "127.0.0.1", upstream.getPort(), 50, 100000, 1)));
        properties.setRoutes(List.of(
                new RouteRule("orders", "/orders", List.of("upstream-a"))));
        properties.setMaxRetries(0);
        properties.setOverallTimeoutMs(5000);
        properties.setPerAttemptTimeoutMs(2000);
        properties.setCircuitFailureThreshold(1);
        properties.setCircuitOpenDurationMs(300);
        properties.setCircuitHalfOpenPermits(1);
    }

    @AfterEach
    void tearDown() {
        upstream.stop();
    }

    private GatewayService newService() {
        return new GatewayService(properties, new UpstreamClient(), diagnostics,
                new IdempotencyStore(), new RoundRobinLoadBalancer());
    }

    private void breakCircuitWithInternalFailure(GatewayService service) {
        upstream.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_500);
        GatewayException e = assertThrows(GatewayException.class,
                () -> service.handle("orders", "/work", null));
        assertEquals(GatewayErrorCode.UPSTREAM_BUSINESS_ERROR, e.getCode());
        assertEquals(CircuitState.OPEN, service.getCircuitBreaker("upstream-a").getState());
    }

    private void waitForCooldown() throws InterruptedException {
        Thread.sleep(450);
    }

    /** 核心场景：恢复试探拿到 4xx 后，上游恢复，下一次正常请求必须透传成功并闭合熔断。 */
    @Test
    void clientRejectionDuringHalfOpenDoesNotBlockRecovery() throws Exception {
        GatewayService service = newService();
        breakCircuitWithInternalFailure(service);
        waitForCooldown();

        // 冷却结束进入恢复试探，试探请求恰好是调用方原因被拒（4xx）
        upstream.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_400);
        CircuitBreaker breaker = service.getCircuitBreaker("upstream-a");
        assertEquals(CircuitState.HALF_OPEN, breaker.getState());
        GatewayException rejected = assertThrows(GatewayException.class,
                () -> service.handle("orders", "/work", null));
        assertEquals(GatewayErrorCode.UPSTREAM_REJECTED, rejected.getCode());

        // 4xx 不是上游内部故障：不得重新熔断，且试探许可必须归还，允许继续试探
        assertNotEquals(CircuitState.OPEN, breaker.getState(),
                "恢复试探期间的调用方拒绝不得重新熔断");

        // 上游完全恢复：请求必须真正到达上游并成功，熔断器闭合，无需重启网关
        upstream.setFailureMode(MockUpstreamServer.FailureMode.NONE);
        int requestsBefore = upstream.getTotalRequestCount();
        String body = service.handle("orders", "/work", null);
        assertTrue(body.contains("sideEffect"), "恢复后请求应正常透传，实际=" + body);
        assertEquals(requestsBefore + 1, upstream.getTotalRequestCount(),
                "恢复后的请求必须真正到达上游");
        assertEquals(CircuitState.CLOSED, breaker.getState());
        assertEquals(0, breaker.getConsecutiveFailures());
    }

    /** 连续多轮“故障→熔断→冷却→试探被 4xx 拒→上游恢复”不得留下残留状态。 */
    @Test
    void repeatedRoundsOfFailureRejectAndRecoverLeaveNoResidualState() throws Exception {
        GatewayService service = newService();
        for (int round = 0; round < 4; round++) {
            breakCircuitWithInternalFailure(service);
            waitForCooldown();

            upstream.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_400);
            GatewayException rejected = assertThrows(GatewayException.class,
                    () -> service.handle("orders", "/work", null),
                    "round " + round);
            assertEquals(GatewayErrorCode.UPSTREAM_REJECTED, rejected.getCode(), "round " + round);
            assertNotEquals(CircuitState.OPEN,
                    service.getCircuitBreaker("upstream-a").getState(),
                    "round " + round + ": 4xx 试探不得重新熔断");

            upstream.setFailureMode(MockUpstreamServer.FailureMode.NONE);
            String body = service.handle("orders", "/work", null);
            assertTrue(body.contains("sideEffect"), "round " + round + " 恢复后应正常透传");
            assertEquals(CircuitState.CLOSED,
                    service.getCircuitBreaker("upstream-a").getState(),
                    "round " + round + " 恢复后应闭合");
        }
        // 再多几笔正常流量确认状态完全正常
        for (int i = 0; i < 5; i++) {
            assertTrue(service.handle("orders", "/work", null).contains("sideEffect"));
        }
        assertEquals(CircuitState.CLOSED, service.getCircuitBreaker("upstream-a").getState());
    }

    /**
     * 恢复试探期间的并发：在途探测许可被占满时，并发多余请求按既有语义快速拒绝（CIRCUIT_OPEN）；
     * 占住许可的探测收到 4xx 后必须释放许可（修复点），之后继续试探，上游恢复后干净闭合。
     */
    @Test
    void concurrentHalfOpenProbesRejectedThenRecover() throws Exception {
        properties.setCircuitHalfOpenPermits(2);
        GatewayService service = newService();
        breakCircuitWithInternalFailure(service);
        waitForCooldown();
        assertEquals(CircuitState.HALF_OPEN, service.getCircuitBreaker("upstream-a").getState());

        // 用闩锁让两个半开探测请求“在途”：它们最终都会收到 4xx，但在放行前一直占着许可
        CountDownLatch reachedUpstream = new CountDownLatch(2);
        CountDownLatch letProbesFinish = new CountDownLatch(1);
        upstream.setFailureMode(() -> {
            reachedUpstream.countDown();
            try {
                letProbesFinish.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return MockUpstreamServer.FailureMode.ALWAYS_400;
        });

        ExecutorService probePool = Executors.newFixedThreadPool(2);
        ExecutorService burstPool = Executors.newFixedThreadPool(6);
        try {
            Future<?>[] probes = new Future<?>[2];
            for (int i = 0; i < 2; i++) {
                probes[i] = probePool.submit(() -> {
                    GatewayException e = assertThrows(GatewayException.class,
                            () -> service.handle("orders", "/work", null));
                    assertEquals(GatewayErrorCode.UPSTREAM_REJECTED, e.getCode());
                });
            }
            assertTrue(reachedUpstream.await(10, TimeUnit.SECONDS),
                    "两个半开探测请求都应已到达上游并占住许可");
            assertEquals(CircuitState.HALF_OPEN, service.getCircuitBreaker("upstream-a").getState());

            // 探测许可已占满：并发多余请求必须快速拒绝，且不得打到上游（既有语义保持不变）
            DispatchResult burst = dispatchConcurrent(burstPool, service, 6);
            assertEquals(0, burst.success.get());
            assertEquals(6, burst.errorCount(GatewayErrorCode.CIRCUIT_OPEN),
                    "探测许可占满时并发多余请求应快速拒绝；实际=" + burst.errors);

            // 放行在途探测：两笔都是调用方拒绝（4xx）
            letProbesFinish.countDown();
            for (Future<?> probe : probes) {
                probe.get(10, TimeUnit.SECONDS);
            }

            // 修复点：4xx 探测必须归还许可、熔断器保持半开且未重新计时/熔断
            assertEquals(CircuitState.HALF_OPEN, service.getCircuitBreaker("upstream-a").getState(),
                    "4xx 探测不得重新熔断");
            assertNotEquals(CircuitState.OPEN, service.getCircuitBreaker("upstream-a").getState());

            // 上游恢复后，新请求能立即拿到探测许可、真正透传并闭合，无需重启网关
            upstream.setFailureMode(MockUpstreamServer.FailureMode.NONE);
            int requestsBefore = upstream.getTotalRequestCount();
            String body = service.handle("orders", "/work", null);
            assertTrue(body.contains("sideEffect"));
            assertEquals(requestsBefore + 1, upstream.getTotalRequestCount(),
                    "恢复后的请求必须真正到达上游（许可未泄漏）");
            assertEquals(CircuitState.CLOSED, service.getCircuitBreaker("upstream-a").getState());
        } finally {
            letProbesFinish.countDown();
            probePool.shutdownNow();
            burstPool.shutdownNow();
        }
    }

    /** 并发试探中一笔 4xx 与一笔成功同时落盘：成功闭合，4xx 不计失败，之后流量正常。 */
    @Test
    void concurrentMixedRejectionAndSuccessClosesCleanly() throws Exception {
        properties.setCircuitHalfOpenPermits(2);
        GatewayService service = newService();
        breakCircuitWithInternalFailure(service);
        waitForCooldown();

        // 第一个请求 4xx，此后全部正常：无论两笔并发谁先谁后，最终都应干净闭合
        AtomicInteger seen = new AtomicInteger();
        upstream.setFailureMode(() -> seen.getAndIncrement() == 0
                ? MockUpstreamServer.FailureMode.ALWAYS_400
                : MockUpstreamServer.FailureMode.NONE);

        DispatchResult result = dispatchConcurrent(service, 2);
        assertEquals(1, result.errorCount(GatewayErrorCode.UPSTREAM_REJECTED),
                "一笔并发试探为调用方拒绝；实际=" + result.errors);
        assertEquals(1, result.success.get(), "另一笔并发试探应成功");
        assertEquals(CircuitState.CLOSED, service.getCircuitBreaker("upstream-a").getState());

        // 闭合后连续正常流量，且 4xx 未留下失败计数
        for (int i = 0; i < 3; i++) {
            assertTrue(service.handle("orders", "/work", null).contains("sideEffect"));
        }
        assertEquals(0, service.getCircuitBreaker("upstream-a").getConsecutiveFailures());
    }

    /** 并发派发 n 个请求并等待全部结束，统计成功数与错误码分布。 */
    private DispatchResult dispatchConcurrent(GatewayService service, int n) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(n);
        try {
            return dispatchConcurrent(pool, service, n);
        } finally {
            pool.shutdownNow();
        }
    }

    /** 在给定线程池上并发派发 n 个请求并等待全部结束，统计成功数与错误码分布。 */
    private DispatchResult dispatchConcurrent(ExecutorService pool, GatewayService service, int n)
            throws Exception {
        DispatchResult result = new DispatchResult();
        CountDownLatch gate = new CountDownLatch(1);
        Future<?>[] futures = new Future<?>[n];
        for (int i = 0; i < n; i++) {
            futures[i] = pool.submit(() -> {
                try {
                    gate.await();
                    service.handle("orders", "/work", null);
                    result.success.incrementAndGet();
                } catch (GatewayException e) {
                    result.errors.computeIfAbsent(e.getCode(), k -> new AtomicInteger())
                            .incrementAndGet();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        gate.countDown();
        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        return result;
    }
}
