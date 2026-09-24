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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发同键幂等：同一次业务请求的并发投递不得产生重复副作用；
 * 慢上游受总超时预算约束：最终归一化超时且不无限重试。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class ConcurrencyIdempotencyAndBudgetTests extends AbstractGatewayTest {

    @Test
    void concurrentSameKeyRequestsProduceSingleSideEffect() throws Exception {
        // up-c 慢但能在调用超时内成功，多个并发请求使用同一幂等键：
        // 恰好 1 个真正执行，其余要么 409 进行中短路，要么复用结果
        rest.getForEntity(admin("/up-c/delay?millis=300"), String.class);

        int threads = 5;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<ResponseEntity<String>>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                return post("/api/payment/once", "same-key-XYZ");
            }));
        }
        start.countDown();

        int created = 0;
        int rejected = 0;
        for (Future<ResponseEntity<String>> f : futures) {
            ResponseEntity<String> r = f.get(30, TimeUnit.SECONDS);
            String body = r.getBody() == null ? "" : r.getBody();
            if (r.getStatusCode().value() == 201 && body.contains("createdBy")) {
                created++;
            } else if (r.getStatusCode().value() == 409) {
                rejected++;
            }
        }
        pool.shutdownNow();

        System.out.println("idempotency concurrency: created=" + created
                + " rejected409=" + rejected);
        assertEquals(1, created, "exactly one side effect allowed");
        assertTrue(created + rejected == threads,
                "others must be short-circuited, created=" + created
                        + " rejected=" + rejected);
        assertEquals(1, mockState.effectCount());
    }

    @Test
    void totalTimeoutBudgetBoundsRetriesOnSlowUpstream() {
        // up-c 每次都超时（单次 1500ms，总预算 3000ms）：最多 2 次真实调用即被
        // 总预算截断，最终归一化为 UPSTREAM_TIMEOUT（504）
        rest.getForEntity(admin("/up-c/timeout"), String.class);
        ResponseEntity<String> resp = get("/api/payment/slow");
        assertEquals(504, resp.getStatusCode().value(), resp.getBody());
        assertTrue(resp.getBody().contains("UPSTREAM_TIMEOUT"), resp.getBody());
        String trace = resp.getHeaders().getFirst("X-Gateway-Trace");
        int calls = trace.split("-> call").length - 1;
        assertTrue(calls <= 2, "budget must cap retries, calls=" + calls);
        System.out.println("budget test capped upstream calls=" + calls);
    }
}
