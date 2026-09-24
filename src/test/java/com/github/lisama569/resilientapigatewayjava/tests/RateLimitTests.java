package com.github.lisama569.resilientapigatewayjava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 按上游维度的并发与速率限制：
 * 超出限额必须快速拒绝（不调用上游），原因标记为 CONCURRENCY_LIMITED /
 * RATE_LIMITED，可与上游自身错误（UPSTREAM_*）明确区分。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class RateLimitTests extends AbstractGatewayTest {

    @Test
    void concurrencyOverflowIsFastRejectedAndDistinguishable() throws Exception {
        // payment 路由仅含 up-c，其 max-concurrent=2。
        // 让 up-c 慢（延迟 800ms，小于调用超时），同时打 8 个并发请求：
        // 至多 2 个进入上游，其余立即 429 CONCURRENCY_LIMITED。
        rest.getForEntity(admin("/up-c/delay?millis=800"), String.class);

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<ResponseEntity<String>>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                return get("/api/payment/c/" + System.nanoTime());
            }));
        }
        start.countDown();

        int rejected = 0;
        int passed = 0;
        for (Future<ResponseEntity<String>> f : futures) {
            ResponseEntity<String> r = f.get(20, TimeUnit.SECONDS);
            String trace = r.getHeaders().getFirst("X-Gateway-Trace");
            if (r.getStatusCode().value() == 429
                    && r.getBody().contains("CONCURRENCY_LIMITED")) {
                assertTrue(trace.contains("CONCURRENCY_EXHAUSTED"), trace);
                assertTrue(trace.contains("upstream never called"), trace);
                rejected++;
            } else if (r.getStatusCode().is2xxSuccessful()) {
                passed++;
            }
        }
        pool.shutdownNow();

        System.out.println("concurrency test: passed=" + passed
                + " fastRejected=" + rejected);
        assertTrue(passed <= 2, "at most 2 concurrent calls, passed=" + passed);
        assertTrue(rejected >= threads - 2,
                "expected fast rejection for overflow, rejected=" + rejected);
        assertTrue(passed + rejected == threads);
    }

    @Test
    void rateOverflowIsDistinctFromConcurrencyAndUpstreamError() {
        // up-c max-per-second=5。顺序快速发 9 个请求，超出部分必须 429
        // RATE_LIMITED，且与 CONCURRENCY_LIMITED 区分（用不同标记）。
        int rateLimited = 0;
        int concurrencyLimited = 0;
        for (int i = 0; i < 9; i++) {
            ResponseEntity<String> r = get("/api/payment/r/" + i);
            String body = r.getBody() == null ? "" : r.getBody();
            if (body.contains("RATE_LIMITED")) {
                rateLimited++;
            } else if (body.contains("CONCURRENCY_LIMITED")) {
                concurrencyLimited++;
            }
        }
        System.out.println("rate test: rateLimited=" + rateLimited
                + " concurrencyLimited=" + concurrencyLimited);
        assertTrue(rateLimited >= 1, "expected RATE_LIMITED rejections");
        // 顺序请求不会触及并发上限
        assertTrue(concurrencyLimited == 0,
                "sequential calls must not hit concurrency limit");
    }
}
