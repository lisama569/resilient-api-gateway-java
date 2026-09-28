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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 同一幂等键高并发投递：验证即使并发请求可能被路由分散到不同上游，
 * 落地上游的真实处理（副作用）也只发生一次，晚到者复用同一结果。
 */
class IdempotencyConcurrencyTest {

    private MockUpstreamServer upstreamA;
    private MockUpstreamServer upstreamB;
    private GatewayDiagnostics diagnostics;
    private IdempotencyStore store;
    private ExecutorService pool;

    @BeforeEach
    void setUp() {
        upstreamA = new MockUpstreamServer("upstream-a", 0);
        upstreamB = new MockUpstreamServer("upstream-b", 0);
        upstreamA.start();
        upstreamB.start();
        diagnostics = new GatewayDiagnostics();
        store = new IdempotencyStore();
        // 自定义线程工厂：每个用例使用全新线程，避免 shutdownNow 的中断状态被线程复用时
        // 带到后续用例（会让模拟上游的 sleep 立即失败）
        pool = Executors.newFixedThreadPool(32, r -> {
            Thread t = new Thread(r, "idem-test-" + System.nanoTime());
            t.setDaemon(true);
            return t;
        });
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        upstreamA.stop();
        upstreamB.stop();
    }

    private GatewayProperties properties() {
        GatewayProperties properties = new GatewayProperties();
        properties.setUpstreams(List.of(
                new UpstreamInstance("upstream-a", "127.0.0.1", upstreamA.getPort(), 50, 10000, 1),
                new UpstreamInstance("upstream-b", "127.0.0.1", upstreamB.getPort(), 50, 10000, 1)));
        properties.setRoutes(List.of(
                new RouteRule("orders", "/orders", List.of("upstream-a", "upstream-b"))));
        properties.setMaxRetries(2);
        properties.setOverallTimeoutMs(5000);
        properties.setPerAttemptTimeoutMs(3000);
        properties.setCircuitFailureThreshold(10);
        properties.setCircuitOpenDurationMs(2000);
        properties.setCircuitHalfOpenPermits(1);
        properties.setIdempotencyWaitMs(4000);
        return properties;
    }

    private GatewayService newService(GatewayProperties properties) {
        return new GatewayService(properties, new UpstreamClient(), diagnostics, store,
                new RoundRobinLoadBalancer());
    }

    @Test
    void concurrentSameKeyHitsUpstreamExactlyOnce() throws Exception {
        upstreamA.setLatencyMs(150);
        upstreamB.setLatencyMs(150);
        GatewayService service = newService(properties());

        int n = 30;
        String key = "key-concurrent-" + System.nanoTime();
        CyclicBarrier barrier = new CyclicBarrier(n);
        List<Future<String>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            futures.add(pool.submit(() -> {
                barrier.await();
                return service.handle("orders", "/work", key);
            }));
        }
        List<String> bodies = new ArrayList<>();
        for (Future<String> future : futures) {
            bodies.add(future.get(15, TimeUnit.SECONDS));
        }
        printDiag("同键高并发唯一处理");

        // 无论请求分散到哪个上游，真实副作用只发生一次
        int sideEffects = upstreamA.getSideEffectCount() + upstreamB.getSideEffectCount();
        assertEquals(1, sideEffects, "同键高并发下真实处理必须只落一次");
        // 所有调用方拿到完全相同的首个结果
        assertEquals(1, bodies.stream().distinct().count(), "晚到请求必须复用首个结果");
        // 完成后键被缓存
        assertEquals(1, store.size());
    }

    @Test
    void concurrentSameKeyWithFailoverStillProcessedExactlyOnce() throws Exception {
        // A 内部故障：leader 先撞 A 的 500，故障转移到 B；
        // 并发的晚到请求即使在网关层被调度到 B，也不得各处理一遍
        upstreamA.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_500);
        upstreamB.setLatencyMs(200);
        GatewayProperties properties = properties();
        properties.setCircuitFailureThreshold(50); // 避免高并发下 A 被熔断改变路径
        GatewayService service = newService(properties);

        int n = 30;
        String key = "key-concurrent-failover-" + System.nanoTime();
        CyclicBarrier barrier = new CyclicBarrier(n);
        List<Future<String>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            futures.add(pool.submit(() -> {
                barrier.await();
                return service.handle("orders", "/work", key);
            }));
        }
        List<String> bodies = new ArrayList<>();
        for (Future<String> future : futures) {
            bodies.add(future.get(15, TimeUnit.SECONDS));
        }
        printDiag("同键高并发+故障转移唯一处理");

        int sideEffects = upstreamA.getSideEffectCount() + upstreamB.getSideEffectCount();
        assertEquals(1, sideEffects, "故障转移 + 高并发下真实处理仍然必须只落一次");
        assertTrue(bodies.get(0).contains("upstream=upstream-b"),
                "应故障转移到健康的 B，实际: " + bodies.get(0));
        assertEquals(1, bodies.stream().distinct().count(), "所有晚到请求复用同一结果");
    }

    @Test
    void lateRequestCanWaitAndReuseLeaderResult() throws Exception {
        upstreamA.setLatencyMs(300);
        GatewayService service = newService(properties());
        String key = "key-follower-wait-" + System.nanoTime();

        // leader 先开始
        Future<String> leader = pool.submit(() -> service.handle("orders", "/work", key));
        awaitStarted(1);
        // 稍晚打进的 follower：等待 leader 完成并复用同一结果，不产生第二次处理
        Future<String> follower = pool.submit(() -> service.handle("orders", "/work", key));

        String leaderBody = leader.get(10, TimeUnit.SECONDS);
        String followerBody = follower.get(10, TimeUnit.SECONDS);
        printDiag("晚到等待复用结果");

        assertEquals(leaderBody, followerBody);
        assertEquals(1, upstreamA.getSideEffectCount() + upstreamB.getSideEffectCount(),
                "follower 必须等待并复用，不得再打一次上游");
        assertTrue(diagnostics.countEventsContaining("idempotent-hit: key=" + key
                + " routeId=orders via=inflight") >= 1);
    }

    @Test
    void lateRequestGetsDistinctInProgressStatusWhenWaitExhausted() throws Exception {
        upstreamA.setLatencyMs(900);
        upstreamB.setLatencyMs(900);
        GatewayProperties properties = properties();
        properties.setIdempotencyWaitMs(100);
        GatewayService service = newService(properties);
        String key = "key-inflight-409-" + System.nanoTime();

        Future<String> leader = pool.submit(() -> service.handle("orders", "/work", key));
        awaitStarted(1); // leader 的请求已进入上游处理（副作用产生前的延迟窗口）

        long start = System.nanoTime();
        GatewayException e = assertThrows(GatewayException.class,
                () -> service.handle("orders", "/work", key));
        long waitedMs = (System.nanoTime() - start) / 1_000_000L;
        printDiag("等待超时快速返回处理中");

        // 与限流 / 熔断 / 上游故障都不同的可分辨状态
        assertEquals(GatewayErrorCode.IDEMPOTENCY_IN_PROGRESS, e.getCode());
        assertTrue(waitedMs < 800, "应在等待预算耗尽后快速返回，而不是一直等到 leader 结束，实际 "
                + waitedMs + "ms");
        // follower 快速返回时只有 leader 那一个请求在上游处理，不得触发第二次上游处理
        assertEquals(1, upstreamA.getStartedProcessingCount() + upstreamB.getStartedProcessingCount(),
                "快速返回不得触发第二次上游处理");
        // 错误体 detail 带上幂等键，便于调用方随后查询
        assertEquals(key, e.getDetail());

        // leader 随后正常完成，只有一次真实处理
        assertNotNull(leader.get(10, TimeUnit.SECONDS));
        assertEquals(1, upstreamA.getSideEffectCount() + upstreamB.getSideEffectCount());
    }

    @Test
    void followerGetsSameResultAfterLeaderFailureAndFailover() throws Exception {
        // A 与 B 前几次都内部故障（但很快恢复）：先到的 leader 把重试 / 故障转移预算耗尽而失败，
        // 在途等待者被唤醒后接管成为新 leader，最终只有一次真实处理成功
        upstreamA.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_500);
        upstreamB.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_500);
        GatewayProperties properties = properties();
        properties.setMaxRetries(0);
        properties.setCircuitFailureThreshold(50);
        GatewayService service = newService(properties);
        String key = "key-takeover-" + System.nanoTime();

        Future<String> leader = pool.submit(() -> service.handle("orders", "/work", key));
        // follower 在 leader 失败前进入在途等待（等待预算给足，它会被 fail 唤醒接管）
        Thread.sleep(50);
        Future<String> follower = pool.submit(() -> service.handle("orders", "/work", key));
        // 等 leader 确定失败
        java.util.concurrent.ExecutionException leaderExec =
                assertThrows(java.util.concurrent.ExecutionException.class,
                        () -> leader.get(5, TimeUnit.SECONDS));
        assertTrue(leaderExec.getCause() instanceof GatewayException);
        assertEquals(GatewayErrorCode.UPSTREAM_BUSINESS_ERROR,
                ((GatewayException) leaderExec.getCause()).getCode());

        // 上游恢复：被唤醒的等待者接管处理
        upstreamA.setFailureMode(MockUpstreamServer.FailureMode.NONE);
        upstreamB.setFailureMode(MockUpstreamServer.FailureMode.NONE);
        String followerBody = follower.get(10, TimeUnit.SECONDS);
        printDiag("leader 失败后等待者接管");

        assertTrue(followerBody.contains("sideEffect="));
        assertEquals(1, upstreamA.getSideEffectCount() + upstreamB.getSideEffectCount(),
                "等待者接管后真实处理仍然只能有一次");
        // 接管完成后，新的同键重复投递直接复用结果
        String third = service.handle("orders", "/work", key);
        assertEquals(followerBody, third);
        assertEquals(1, upstreamA.getSideEffectCount() + upstreamB.getSideEffectCount());
    }

    private void awaitStarted(int expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (upstreamA.getStartedProcessingCount() + upstreamB.getStartedProcessingCount() < expected
                && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertTrue(upstreamA.getStartedProcessingCount() + upstreamB.getStartedProcessingCount() >= expected,
                "等待上游开始处理超时");
    }

    private void printDiag(String scenario) {
        System.out.println("==== 诊断事件 [" + scenario + "] ====");
        diagnostics.getEvents().stream()
                .filter(e -> e.contains("idempot") || e.contains("success") || e.contains("attempt"))
                .limit(12)
                .forEach(e -> System.out.println("  " + e));
    }
}
