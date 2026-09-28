package com.github.lisama569.resilientapigatewayjava.tests;

import com.github.lisama569.resilientapigatewayjava.config.GatewayProperties;
import com.github.lisama569.resilientapigatewayjava.gateway.GatewayService;
import com.github.lisama569.resilientapigatewayjava.gateway.UpstreamClient;
import com.github.lisama569.resilientapigatewayjava.gateway.diag.GatewayDiagnostics;
import com.github.lisama569.resilientapigatewayjava.gateway.route.RouteRule;
import com.github.lisama569.resilientapigatewayjava.gateway.route.RoundRobinLoadBalancer;
import com.github.lisama569.resilientapigatewayjava.idempotency.IdempotencyStore;
import com.github.lisama569.resilientapigatewayjava.upstream.MockUpstreamServer;
import com.github.lisama569.resilientapigatewayjava.upstream.UpstreamInstance;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 幂等键只读查询入口：已完成 -> 返回结果；处理中 -> 明确“还在处理”；
 * 未知键 -> 明确“没见过”。查询本身绝不触发上游调用。
 */
class IdempotencyQueryTest {

    private MockUpstreamServer upstreamA;
    private GatewayService service;
    private ExecutorService pool;

    @BeforeEach
    void setUp() {
        upstreamA = new MockUpstreamServer("upstream-a", 0);
        upstreamA.start();
        GatewayProperties properties = new GatewayProperties();
        properties.setUpstreams(List.of(
                new UpstreamInstance("upstream-a", "127.0.0.1", upstreamA.getPort(), 10, 1000, 1)));
        properties.setRoutes(List.of(
                new RouteRule("orders", "/orders", List.of("upstream-a"))));
        properties.setMaxRetries(0);
        properties.setOverallTimeoutMs(5000);
        properties.setPerAttemptTimeoutMs(2000);
        properties.setIdempotentWaitMs(2000);
        service = new GatewayService(properties, new UpstreamClient(), new GatewayDiagnostics(),
                new IdempotencyStore(), new RoundRobinLoadBalancer());
        pool = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void tearDown() {
        pool.shutdownNow();
        upstreamA.stop();
    }

    @Test
    void unknownKeyIsReportedAsNotFound() {
        IdempotencyStore.QueryResult result = service.queryIdempotent("never-seen-key");
        assertEquals(IdempotencyStore.QueryStatus.NOT_FOUND, result.getStatus());
        assertNull(result.getBody());
    }

    @Test
    void inFlightKeyIsReportedAsProcessingWithoutResult() throws Exception {
        upstreamA.setLatencyMs(600);
        String key = "key-query-processing";
        Future<String> owner = pool.submit(() -> service.handle("orders", "/work", key));
        waitForStatus(key, IdempotencyStore.QueryStatus.PROCESSING);

        IdempotencyStore.QueryResult processing = service.queryIdempotent(key);
        assertEquals(IdempotencyStore.QueryStatus.PROCESSING, processing.getStatus(),
                "处理中必须明确告知，不能硬报成功/失败");
        assertNull(processing.getBody());

        // 等在途请求真正到达上游后，多次查询不得触发额外上游调用
        waitForUpstreamHits(1);
        for (int i = 0; i < 3; i++) {
            assertEquals(IdempotencyStore.QueryStatus.PROCESSING, service.queryIdempotent(key).getStatus());
            Thread.sleep(20);
        }
        assertEquals(1, upstreamA.getTotalRequestCount());

        String body = owner.get(10, TimeUnit.SECONDS);
        IdempotencyStore.QueryResult completed = service.queryIdempotent(key);
        assertEquals(IdempotencyStore.QueryStatus.COMPLETED, completed.getStatus());
        assertEquals(body, completed.getBody());
        assertEquals(1, upstreamA.getTotalRequestCount(), "查询全程不得触发上游调用");
    }

    @Test
    void completedKeyReturnsTheSameResultAsDelivery() throws Exception {
        String key = "key-query-done";
        String delivered = service.handle("orders", "/work", key);
        IdempotencyStore.QueryResult result = service.queryIdempotent(key);
        assertEquals(IdempotencyStore.QueryStatus.COMPLETED, result.getStatus());
        assertNotNull(result.getBody());
        assertEquals(delivered, result.getBody());
    }

    @Test
    void failedRequestIsNotReportedAsCompletedAndKeyCanBeRetried() {
        upstreamA.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_500);
        String key = "key-query-failed";
        try {
            service.handle("orders", "/work", key);
        } catch (Exception ignored) {
            // 预期失败
        }
        // 失败不应谎报成功：查询结果为 NOT_FOUND（没有已落定的结果）
        IdempotencyStore.QueryResult afterFailure = service.queryIdempotent(key);
        assertEquals(IdempotencyStore.QueryStatus.NOT_FOUND, afterFailure.getStatus(),
                "失败请求不能被查询接口报成已完成");

        upstreamA.setFailureMode(MockUpstreamServer.FailureMode.NONE);
        try {
            String retried = service.handle("orders", "/work", key);
            IdempotencyStore.QueryResult done = service.queryIdempotent(key);
            assertEquals(IdempotencyStore.QueryStatus.COMPLETED, done.getStatus());
            assertEquals(retried, done.getBody());
        } catch (Exception e) {
            throw new AssertionError("失败清理后同键重试应成功", e);
        }
    }

    private void waitForStatus(String key, IdempotencyStore.QueryStatus expected) throws InterruptedException {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (System.nanoTime() < deadline) {
            if (service.queryIdempotent(key).getStatus() == expected) {
                return;
            }
            Thread.sleep(5);
        }
        throw new IllegalStateException("未在预期时间内进入状态 " + expected);
    }

    private void waitForUpstreamHits(int expected) throws InterruptedException {
        long deadline = System.nanoTime() + 3_000_000_000L;
        while (System.nanoTime() < deadline) {
            if (upstreamA.getTotalRequestCount() >= expected) {
                return;
            }
            Thread.sleep(5);
        }
        throw new IllegalStateException("上游未在预期时间内收到 " + expected + " 个请求");
    }
}
