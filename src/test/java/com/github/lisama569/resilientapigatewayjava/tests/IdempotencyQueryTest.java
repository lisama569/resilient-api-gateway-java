package com.github.lisama569.resilientapigatewayjava.tests;

import com.github.lisama569.resilientapigatewayjava.config.GatewayProperties;
import com.github.lisama569.resilientapigatewayjava.gateway.GatewayService;
import com.github.lisama569.resilientapigatewayjava.gateway.UpstreamClient;
import com.github.lisama569.resilientapigatewayjava.gateway.diag.GatewayDiagnostics;
import com.github.lisama569.resilientapigatewayjava.gateway.route.RouteRule;
import com.github.lisama569.resilientapigatewayjava.gateway.route.RoundRobinLoadBalancer;
import com.github.lisama569.resilientapigatewayjava.idempotency.IdempotencySnapshot;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 幂等键只读查询：无记录、处理中、已完成三种结果。
 * 处理中必须明确表达“还在处理”，不得硬报成功或失败。
 */
class IdempotencyQueryTest {

    private MockUpstreamServer upstream;
    private GatewayService service;
    private ExecutorService pool;

    @BeforeEach
    void setUp() {
        upstream = new MockUpstreamServer("upstream-a", 0);
        upstream.start();
        GatewayDiagnostics diagnostics = new GatewayDiagnostics();
        GatewayProperties properties = new GatewayProperties();
        properties.setUpstreams(List.of(
                new UpstreamInstance("upstream-a", "127.0.0.1", upstream.getPort(), 10, 10000, 1)));
        properties.setRoutes(List.of(
                new RouteRule("orders", "/orders", List.of("upstream-a"))));
        properties.setMaxRetries(1);
        properties.setOverallTimeoutMs(3000);
        properties.setPerAttemptTimeoutMs(2000);
        properties.setCircuitFailureThreshold(3);
        properties.setCircuitOpenDurationMs(400);
        properties.setCircuitHalfOpenPermits(1);
        properties.setIdempotencyWaitMs(1000);
        service = new GatewayService(properties, new UpstreamClient(), diagnostics,
                new IdempotencyStore(), new RoundRobinLoadBalancer());
        pool = Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "query-test-" + System.nanoTime());
            t.setDaemon(true);
            return t;
        });
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        upstream.stop();
    }

    private void awaitStarted(int expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (upstream.getStartedProcessingCount() < expected && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertTrue(upstream.getStartedProcessingCount() >= expected);
    }

    @Test
    void unknownKeyReturnsNotFound() {
        assertNull(service.queryStatus("never-seen-key"), "无记录键查询应返回 null（控制器映射 404）");
    }

    @Test
    void inFlightKeyReportsProcessingWithoutPretendingSuccessOrFailure() throws Exception {
        upstream.setLatencyMs(600);
        String key = "key-query-inflight";
        Future<String> leader = pool.submit(() -> service.handle("orders", "/work", key));
        awaitStarted(1);

        IdempotencySnapshot snapshot = service.queryStatus(key);
        assertNotNull(snapshot, "处理中的键必须查得到");
        assertEquals(IdempotencySnapshot.Status.IN_FLIGHT, snapshot.status(),
                "必须明确告诉调用方还在处理，而不是硬报成功/失败");
        assertNull(snapshot.body(), "处理中不得伪造结果体");
        System.out.println("==== 处理中查询: status=" + snapshot.status() + " ====");

        assertNotNull(leader.get(10, TimeUnit.SECONDS));
    }

    @Test
    void completedKeyReturnsStoredResult() throws Exception {
        String key = "key-query-done";
        String body = service.handle("orders", "/work", key);
        assertTrue(body.contains("sideEffect=1"));

        IdempotencySnapshot snapshot = service.queryStatus(key);
        assertNotNull(snapshot);
        assertEquals(IdempotencySnapshot.Status.COMPLETED, snapshot.status());
        assertEquals(body, snapshot.body());
        System.out.println("==== 已完成查询: status=" + snapshot.status() + " body=" + snapshot.body() + " ====");
    }
}
