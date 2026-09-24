package com.github.lisama569.resilientapigatewayjava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 路由分发与幂等（重复投递不放大副作用）。 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class RoutingAndIdempotencyTests extends AbstractGatewayTest {

    @Test
    void routesToHealthyInstanceAndSucceeds() {
        ResponseEntity<String> resp = get("/api/orders/123");
        assertEquals(200, resp.getStatusCode().value());
        assertNotNull(resp.getBody());
        assertTrue(resp.getBody().contains("\"ok\":true"), resp.getBody());
        String trace = resp.getHeaders().getFirst("X-Gateway-Trace");
        assertTrue(trace.contains("matched rule=orders-route"));
        assertTrue(trace.contains("breaker=CLOSED"));
        assertEquals("0", resp.getHeaders().getFirst("X-Gateway-Retries"));
    }

    @Test
    void unknownPathIsRouteFailureDistinctFromUpstreamFailure() {
        ResponseEntity<String> resp = get("/no/such/route");
        assertEquals(404, resp.getStatusCode().value());
        assertTrue(resp.getBody().contains("ROUTE_NOT_FOUND"), resp.getBody());
        String trace = resp.getHeaders().getFirst("X-Gateway-Trace");
        assertTrue(trace.contains("ROUTE_NOT_FOUND"));
        // 路由失败不应产生任何上游调用
        assertEquals(0, mockState.effectCount());
    }

    @Test
    void twoEntryPointsBehaveConsistently() {
        ResponseEntity<String> viaGw = get("/gw/orders/1");
        ResponseEntity<String> viaApi = get("/api/orders/1");
        assertEquals(200, viaGw.getStatusCode().value());
        assertEquals(200, viaApi.getStatusCode().value());
        assertTrue(viaApi.getHeaders().getFirst("X-Gateway-Trace")
                .contains("matched rule=orders-route"));
    }

    @Test
    void retriedDeliveryWithSameKeyProducesSingleSideEffect() throws Exception {
        // 固定顺序 [a,b,d]：up-a、up-b 首次 503，第三次 up-d 成功
        rest.getForEntity(admin("/up-a/fail-first-n?n=1"), String.class);
        rest.getForEntity(admin("/up-b/fail-first-n?n=1"), String.class);

        ResponseEntity<String> resp = post("/api/failover/1", "order-1001");
        // 重试后成功
        assertEquals(201, resp.getStatusCode().value(), resp.getBody());
        int retries = Integer.parseInt(
                resp.getHeaders().getFirst("X-Gateway-Retries"));
        assertTrue(retries >= 1, "expected at least one retry, got " + retries);

        // 再次用同一键投递：直接命中幂等，不产生第二个副作用
        ResponseEntity<String> replay = post("/api/orders", "order-1001");
        assertEquals(201, replay.getStatusCode().value());
        assertEquals(1, mockState.effectCount(),
                "side effect must happen exactly once despite retries+replay");
    }
}
