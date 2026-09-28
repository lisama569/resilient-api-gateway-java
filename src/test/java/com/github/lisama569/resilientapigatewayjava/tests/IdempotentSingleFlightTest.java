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
 * 同一幂等键高并发投递的单飞（single-flight）保证：
 * 无论请求并发打在同一上游，还是因故障转移分散到不同上游，真实处理只发生一次。
 */
class IdempotentSingleFlightTest {

    private MockUpstreamServer upstreamA;
    private MockUpstreamServer upstreamB;
    private GatewayDiagnostics diagnostics;
    private IdempotencyStore idempotencyStore;
    private GatewayService gatewayService;
    private ExecutorService pool;

    @BeforeEach
    void setUp() {
        upstreamA = new MockUpstreamServer("upstream-a", 0);
        upstreamB = new MockUpstreamServer("upstream-b", 0);
        upstreamA.start();
        upstreamB.start();
        diagnostics = new GatewayDiagnostics();
        idempotencyStore = new IdempotencyStore();
        gatewayService = newService(defaultProperties());
        pool = Executors.newFixedThreadPool(24);
    }

    @AfterEach
    void tearDown() {
        pool.shutdownNow();
        upstreamA.stop();
        upstreamB.stop();
    }

    private GatewayProperties defaultProperties() {
        GatewayProperties properties = new GatewayProperties();
        properties.setUpstreams(List.of(
                new UpstreamInstance("upstream-a", "127.0.0.1", upstreamA.getPort(), 32, 1000, 1),
                new UpstreamInstance("upstream-b", "127.0.0.1", upstreamB.getPort(), 32, 1000, 1)));
        properties.setRoutes(List.of(
                new RouteRule("orders", "/orders", List.of("upstream-a", "upstream-b"))));
        properties.setMaxRetries(2);
        properties.setOverallTimeoutMs(5000);
        properties.setPerAttemptTimeoutMs(2000);
        properties.setCircuitFailureThreshold(10);
        properties.setCircuitOpenDurationMs(2000);
        properties.setCircuitHalfOpenPermits(1);
        properties.setIdempotentWaitMs(3000);
        return properties;
    }

    private GatewayService newService(GatewayProperties properties) {
        return new GatewayService(properties, new UpstreamClient(), diagnostics,
                idempotencyStore, new RoundRobinLoadBalancer());
    }

    @Test
    void concurrentSameKeyRequestsProcessedExactlyOnceAcrossUpstreams() throws Exception {
        upstreamA.setLatencyMs(150);
        upstreamB.setLatencyMs(150);
        int parallelism = 20;
        String key = "key-concurrent-once";
        CountDownLatch gate = new CountDownLatch(1);
        Future<String>[] futures = new Future[parallelism];
        for (int i = 0; i < parallelism; i++) {
            futures[i] = pool.submit(() -> {
                gate.await();
                return gatewayService.handle("orders", "/work", key);
            });
        }
        gate.countDown();
        String first = futures[0].get(10, TimeUnit.SECONDS);
        for (Future<String> f : futures) {
            assertEquals(first, f.get(10, TimeUnit.SECONDS), "所有并发同键请求必须拿到同一结果");
        }
        assertEquals(1, upstreamA.getSideEffectCount() + upstreamB.getSideEffectCount(),
                "高并发同键请求真实副作用只能发生一次");
        long totalHits = upstreamA.getTotalRequestCount() + upstreamB.getTotalRequestCount();
        assertEquals(1, totalHits, "并发重复投递不应到达上游，实际到达次数=" + totalHits);
    }

    @Test
    void sameKeyConcurrentWithFailoverStillProcessedExactlyOnce() throws Exception {
        // A 返回 500 强制 owner 故障转移到 B；同时大量同键请求并发涌入
        upstreamA.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_500);
        upstreamB.setLatencyMs(200);
        int parallelism = 20;
        String key = "key-concurrent-failover";
        CountDownLatch gate = new CountDownLatch(1);
        Future<String>[] futures = new Future[parallelism];
        for (int i = 0; i < parallelism; i++) {
            futures[i] = pool.submit(() -> {
                gate.await();
                return gatewayService.handle("orders", "/work", key);
            });
        }
        gate.countDown();
        String first = futures[0].get(15, TimeUnit.SECONDS);
        for (Future<String> f : futures) {
            assertEquals(first, f.get(15, TimeUnit.SECONDS));
        }
        assertTrue(first.contains("upstream=upstream-b"), "最终结果必须来自故障转移后的 B: " + first);
        assertEquals(1, upstreamB.getSideEffectCount(), "B 上真实处理只能一次");
        assertEquals(0, upstreamA.getSideEffectCount(), "500 失败的 A 不产生副作用");
        // 只有 owner 触达 A(500) 与 B(成功)；等待者不得各自再打一遍
        assertEquals(1, upstreamA.getTotalRequestCount(), "A 只应被 owner 的故障转移尝试触及一次");
        assertEquals(1, upstreamB.getTotalRequestCount(), "B 只应被真实处理一次");
    }

    @Test
    void lateArrivingRequestAfterCompletionGetsSameResultWithoutUpstreamCall() throws Exception {
        String key = "key-late";
        String first = gatewayService.handle("orders", "/work", key);
        int hitsAfterFirst = upstreamA.getTotalRequestCount() + upstreamB.getTotalRequestCount();
        String second = gatewayService.handle("orders", "/work", key);
        assertEquals(first, second);
        assertEquals(hitsAfterFirst, upstreamA.getTotalRequestCount() + upstreamB.getTotalRequestCount(),
                "完成后的重复投递不得再触达上游");
    }

    @Test
    void stillProcessingReturnsDistinguishableStatusWhenOwnerSlow() throws Exception {
        upstreamA.setLatencyMs(800);
        GatewayProperties props = defaultProperties();
        props.setIdempotentWaitMs(150);
        GatewayService service = newService(props);
        String key = "key-slow-owner";
        Future<String> owner = pool.submit(() -> service.handle("orders", "/work", key));
        // 等 owner 占位后再发晚到请求
        waitUntilProcessing(key, service);
        long start = System.nanoTime();
        GatewayException e = assertThrows(GatewayException.class,
                () -> service.handle("orders", "/work", key));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
        assertEquals(GatewayErrorCode.IDEMPOTENT_PROCESSING, e.getCode(),
                "处理中超时必须返回与限流/熔断/上游故障不同的状态");
        assertTrue(elapsedMs < 700, "处理中应快速返回，实际等待 " + elapsedMs + "ms");
        // owner 仍会正常完成并只处理一次
        String body = owner.get(10, TimeUnit.SECONDS);
        assertTrue(body.contains("sideEffect=1"));
        assertEquals(1, upstreamA.getSideEffectCount() + upstreamB.getSideEffectCount());
    }

    @Test
    void failedOwnerReleasesKeySoLaterRequestCanSucceed() throws Exception {
        // 第一轮：唯一上游 500，owner 重试耗尽失败；第二轮：恢复后同键可重新处理且只一次
        upstreamA.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_500);
        upstreamB.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_500);
        GatewayProperties props = defaultProperties();
        props.setMaxRetries(0);
        GatewayService service = newService(props);
        String key = "key-owner-fail-retry";
        assertThrows(GatewayException.class, () -> service.handle("orders", "/work", key));
        assertTrue(diagnostics.countEventsContaining("upstream failure") >= 1);

        upstreamA.setFailureMode(MockUpstreamServer.FailureMode.NONE);
        upstreamB.setFailureMode(MockUpstreamServer.FailureMode.NONE);
        String body = service.handle("orders", "/work", key);
        assertTrue(body.contains("sideEffect"));
        String again = service.handle("orders", "/work", key);
        assertEquals(body, again);
        assertEquals(1, upstreamA.getSideEffectCount() + upstreamB.getSideEffectCount(),
                "失败后重新处理也只能产生一次副作用");
    }

    @Test
    void repeatedRoundsOfRetriesAndFailoverDoNotBreakSingleProcessing() throws Exception {
        // A 前 2 次请求 500（owner 的两轮故障转移尝试），第 3 次起恢复；B 始终正常
        AtomicInteger flakyRemaining = new AtomicInteger(2);
        upstreamA.setFailureMode(() -> flakyRemaining.getAndDecrement() > 0
                ? MockUpstreamServer.FailureMode.ALWAYS_500
                : MockUpstreamServer.FailureMode.NONE);
        upstreamB.setLatencyMs(50);
        int parallelism = 16;
        String key = "key-storm";
        CountDownLatch gate = new CountDownLatch(1);
        AtomicInteger processing = new AtomicInteger();
        Future<?>[] futures = new Future[parallelism];
        for (int i = 0; i < parallelism; i++) {
            futures[i] = pool.submit(() -> {
                gate.await();
                try {
                    return gatewayService.handle("orders", "/work", key);
                } catch (GatewayException e) {
                    if (e.getCode() == GatewayErrorCode.IDEMPOTENT_PROCESSING) {
                        processing.incrementAndGet();
                        return "processing";
                    }
                    throw e;
                }
            });
        }
        gate.countDown();
        String winner = null;
        for (Future<?> f : futures) {
            Object r = f.get(15, TimeUnit.SECONDS);
            if (!"processing".equals(r)) {
                if (winner == null) {
                    winner = (String) r;
                } else {
                    assertEquals(winner, r);
                }
            }
        }
        // 至少一轮真实成功，且全局副作用恰好一次（风暴 + 重试 + 故障转移下保证不破）
        assertEquals(1, upstreamA.getSideEffectCount() + upstreamB.getSideEffectCount(),
                "并发风暴叠加重试与故障转移，真实处理仍只能一次，A="
                        + upstreamA.getSideEffectCount() + " B=" + upstreamB.getSideEffectCount());
        assertTrue(winner != null && winner.contains("sideEffect=1"));
    }

    private void waitUntilProcessing(String key, GatewayService service) throws InterruptedException {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (System.nanoTime() < deadline) {
            if (service.queryIdempotent(key).getStatus() == IdempotencyStore.QueryStatus.PROCESSING) {
                return;
            }
            Thread.sleep(5);
        }
        throw new IllegalStateException("owner 未在预期时间内进入处理中");
    }
}
