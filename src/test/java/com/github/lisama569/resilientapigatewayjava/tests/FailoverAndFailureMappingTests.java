package com.github.lisama569.resilientapigatewayjava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 上游不可用故障转移、超时归一化、业务错误映射与错误原因可区分性。 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class FailoverAndFailureMappingTests extends AbstractGatewayTest {

    @Test
    void connectionFailureFailsOverToHealthyInstance() {
        // 故障转移路由顺序固定 [a,b,d]，游标每用例归零：
        // up-a 首次 503 -> 故障转移 up-b（首次 503）-> up-d 健康成功
        rest.getForEntity(admin("/up-a/fail-first-n?n=1"), String.class);
        rest.getForEntity(admin("/up-b/fail-first-n?n=1"), String.class);

        ResponseEntity<String> resp = get("/api/failover/77");
        assertEquals(200, resp.getStatusCode().value(), resp.getBody());
        String trace = resp.getHeaders().getFirst("X-Gateway-Trace");
        assertTrue(trace.contains("business status=503"), trace);
        assertTrue(trace.contains("success status=200"), trace);
        assertTrue(Integer.parseInt(
                resp.getHeaders().getFirst("X-Gateway-Retries")) >= 1);
    }

    @Test
    void connectionRefusedIsNormalizedToConnectKind() {
        // dead-only 路由仅含指向保留封闭端口的实例 up-dead：
        // 三次尝试全部连接被拒绝，最终归一化为 UPSTREAM_CONNECT，与超时/业务错误区分
        ResponseEntity<String> resp = get("/api/dead-only/1");
        assertEquals(502, resp.getStatusCode().value(), resp.getBody());
        String body = resp.getBody();
        assertTrue(body.contains("UPSTREAM_CONNECT"), body);
        String trace = resp.getHeaders().getFirst("X-Gateway-Trace");
        assertTrue(trace.contains("kind=UPSTREAM_CONNECT"), trace);
        assertEquals("2", resp.getHeaders().getFirst("X-Gateway-Retries"));
    }

    @Test
    void timeoutIsNormalizedTo504DistinctFromConnect() {
        rest.getForEntity(admin("/up-c/timeout"), String.class);
        ResponseEntity<String> resp = get("/api/payment/9");
        assertEquals(504, resp.getStatusCode().value(), resp.getBody());
        assertTrue(resp.getBody().contains("UPSTREAM_TIMEOUT"), resp.getBody());
        String trace = resp.getHeaders().getFirst("X-Gateway-Trace");
        assertTrue(trace.contains("kind=UPSTREAM_TIMEOUT"), trace);
    }

    @Test
    void nonRetryableBusinessErrorIsReturnedImmediatelyAndDistinguished() {
        rest.getForEntity(admin("/up-a/status?code=500"), String.class);
        ResponseEntity<String> resp = post("/api/a-only/x", "biz-err-1");
        assertEquals(500, resp.getStatusCode().value());
        assertTrue(resp.getBody().contains("UPSTREAM_BUSINESS"), resp.getBody());
        // 500 不可重试：仅 1 次尝试，不应故障转移
        assertEquals("0", resp.getHeaders().getFirst("X-Gateway-Retries"));
        String trace = resp.getHeaders().getFirst("X-Gateway-Trace");
        assertTrue(trace.contains("business status=500"));
        assertTrue(trace.contains("[final]"), trace);
    }

    @Test
    void sideEffectNotDuplicatedAcrossFailover() {
        // a、b 首次返回 503（可重试），故障转移/重试后完成 POST；
        // 共享幂等键空间保证副作用仅一次
        rest.getForEntity(admin("/up-a/fail-first-n?n=1"), String.class);
        rest.getForEntity(admin("/up-b/fail-first-n?n=1"), String.class);
        ResponseEntity<String> resp = post("/api/failover/77", "order-failover-1");
        assertEquals(201, resp.getStatusCode().value(), resp.getBody());
        assertTrue(resp.getBody().contains("createdBy"), resp.getBody());
        assertTrue(Integer.parseInt(
                resp.getHeaders().getFirst("X-Gateway-Retries")) >= 1);
        assertEquals(1, mockState.effectCount());
    }
}
