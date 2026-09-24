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
 * 熔断三态与恢复：连续失败触发 OPEN 并快速失败；OPEN 窗口结束后进入
 * HALF_OPEN，只放 1 个探测；探测成功才 CLOSED 全量恢复；
 * HALF_OPEN 下并发请求不得误放行（只有 halfOpenPermits 个能真正探测）。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class CircuitBreakerTests extends AbstractGatewayTest {

    @Test
    void opensAfterThresholdThenFastFailsWithoutCallingUpstream() {
        rest.getForEntity(admin("/up-a/status?code=503"), String.class);

        String lastTrace = "";
        for (int i = 0; i < 4; i++) {
            ResponseEntity<String> r = get("/api/a-only/bo/" + i);
            lastTrace = r.getHeaders().getFirst("X-Gateway-Trace");
        }
        // 阈值=3：前 3 次真实调用失败，第 4 次熔断器 OPEN 直接快速失败
        assertTrue(lastTrace.contains("breaker=OPEN -> fast-skip"), lastTrace);
        assertTrue(lastTrace.contains("upstream never called"), lastTrace);
        // OPEN 后结论是入口侧 CIRCUIT_OPEN（503），与上游错误区分
        ResponseEntity<String> r = get("/api/a-only/bo/after-open");
        assertEquals(503, r.getStatusCode().value());
        assertTrue(r.getBody().contains("CIRCUIT_OPEN"), r.getBody());
    }

    @Test
    void halfOpenProbeSucceedsThenFullyRecovers() throws Exception {
        rest.getForEntity(admin("/up-a/status?code=503"), String.class);
        for (int i = 0; i < 3; i++) {
            get("/api/a-only/rec/" + i);
        }
        // 此时 OPEN。恢复为健康，等待 OPEN 窗口（测试配置 1200ms）过后：
        rest.getForEntity(admin("/up-a/healthy"), String.class);
        Thread.sleep(1400);

        // 第一个请求作为 HALF_OPEN 探测，成功后熔断器关闭
        ResponseEntity<String> probe = get("/api/a-only/rec/probe");
        assertEquals(200, probe.getStatusCode().value(), probe.getBody());
        String probeTrace = probe.getHeaders().getFirst("X-Gateway-Trace");
        assertTrue(probeTrace.contains("HALF_OPEN"), probeTrace);
        assertTrue(probeTrace.contains("(probe)"), probeTrace);

        // 随后流量恢复正常 CLOSED
        ResponseEntity<String> after = get("/api/a-only/rec/after");
        assertEquals(200, after.getStatusCode().value());
        assertTrue(after.getHeaders().getFirst("X-Gateway-Trace")
                .contains("breaker=CLOSED"));
    }

    @Test
    void halfOpenRejectsConcurrentRequestsBeyondProbePermit() throws Exception {
        rest.getForEntity(admin("/up-a/status?code=503"), String.class);
        for (int i = 0; i < 3; i++) {
            get("/api/a-only/cb/" + i);
        }
        // 让探测期间的上游响应慢一点，使多个请求在 HALF_OPEN 窗口内并发到达
        rest.getForEntity(admin("/up-a/delay?millis=400"), String.class);
        Thread.sleep(1400);

        int threads = 6;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<ResponseEntity<String>>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final int idx = i;
            futures.add(pool.submit(() -> {
                start.await();
                return get("/api/a-only/cb/half/" + idx);
            }));
        }
        start.countDown();

        int probes = 0;
        int openRejected = 0;
        for (Future<ResponseEntity<String>> f : futures) {
            ResponseEntity<String> r = f.get(30, TimeUnit.SECONDS);
            String trace = r.getHeaders().getFirst("X-Gateway-Trace");
            if (r.getStatusCode().is2xxSuccessful() && trace.contains("(probe)")) {
                probes++;
            } else if (r.getStatusCode().value() == 503
                    && trace.contains("breaker=HALF_OPEN")) {
                openRejected++;
            }
        }
        pool.shutdownNow();
        System.out.println("half-open concurrency: probes=" + probes
                + " rejected=" + openRejected);
        // halfOpenPermits=1：恰好 1 个探测，其余在 HALF_OPEN 被拒绝，绝无误放行
        assertEquals(1, probes, "exactly one probe must be admitted");
        assertTrue(openRejected >= 1, "surplus requests must be rejected");
        assertEquals(threads, probes + openRejected);
    }

    @Test
    void failedProbeReopensBreaker() throws Exception {
        rest.getForEntity(admin("/up-a/status?code=503"), String.class);
        for (int i = 0; i < 3; i++) {
            get("/api/a-only/fp/" + i);
        }
        Thread.sleep(1400);
        // 保持 503：HALF_OPEN 探测得到真实上游 503 并立即重新 OPEN；
        // 因熔断 OPEN 无法继续重试，对外保留真实上游业务状态 503
        ResponseEntity<String> probe = get("/api/a-only/fp/probe");
        assertEquals(503, probe.getStatusCode().value());
        String pTrace = probe.getHeaders().getFirst("X-Gateway-Trace");
        assertTrue(pTrace.contains("HALF_OPEN"), pTrace);
        assertTrue(pTrace.contains("business status=503"), pTrace);

        ResponseEntity<String> again = get("/api/a-only/fp/again");
        assertEquals(503, again.getStatusCode().value());
        assertTrue(again.getBody().contains("CIRCUIT_OPEN"), again.getBody());
    }
}
