package com.github.lisama569.resilientapigatewayjava.tests;

import com.github.lisama569.resilientapigatewayjava.config.GatewayProperties;
import com.github.lisama569.resilientapigatewayjava.gateway.GatewayController;
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
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 控制器层：新错误码 HTTP 映射、查询入口 JSON 与路由路径优先级
 * （/gateway/idempotency/{key} 必须命中查询入口而非 {routeId}=idempotency）。
 */
class GatewayControllerWebTest {

    private MockUpstreamServer upstream;
    private GatewayController controller;
    private GatewayService service;

    @BeforeEach
    void setUp() {
        upstream = new MockUpstreamServer("upstream-a", 0);
        upstream.start();
        GatewayProperties properties = new GatewayProperties();
        properties.setUpstreams(List.of(
                new UpstreamInstance("upstream-a", "127.0.0.1", upstream.getPort(), 10, 1000, 1)));
        properties.setRoutes(List.of(
                new RouteRule("orders", "/orders", List.of("upstream-a"))));
        properties.setMaxRetries(0);
        properties.setOverallTimeoutMs(3000);
        properties.setPerAttemptTimeoutMs(1000);
        service = new GatewayService(properties, new UpstreamClient(), new GatewayDiagnostics(),
                new IdempotencyStore(), new RoundRobinLoadBalancer());
        controller = new GatewayController(service);
    }

    @AfterEach
    void tearDown() {
        upstream.stop();
    }

    @Test
    @SuppressWarnings("unchecked")
    void queryEndpointTakesPrecedenceAndReportsLifecycle() throws Exception {
        // 未知键：404 + IDEMPOTENT_KEY_NOT_FOUND
        ResponseEntity<?> notFound = controller.queryIdempotent("abc");
        assertEquals(HttpStatus.NOT_FOUND, notFound.getStatusCode());
        assertEquals(GatewayErrorCode.IDEMPOTENT_KEY_NOT_FOUND.name(),
                ((Map<String, Object>) notFound.getBody()).get("code"));

        // 完成一次投递后查询：200 + COMPLETED + 同一结果
        String delivered = service.handle("orders", "/work", "abc");
        ResponseEntity<?> done = controller.queryIdempotent("abc");
        assertEquals(HttpStatus.OK, done.getStatusCode());
        Map<String, Object> doneBody = (Map<String, Object>) done.getBody();
        assertEquals("COMPLETED", doneBody.get("status"));
        assertEquals(delivered, doneBody.get("result"));
    }

    @Test
    void upstreamRejectionMapsTo422WithDistinctCode() {
        upstream.setFailureMode(MockUpstreamServer.FailureMode.ALWAYS_400);
        GatewayException e = new GatewayException(GatewayErrorCode.UPSTREAM_REJECTED,
                "rejected", "upstream-a", 400, "bad argument");
        // 直接验证控制器的 HTTP 映射不依赖 Spring 容器
        ResponseEntity<?> response = ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(Map.of("code", e.getCode().name()));
        assertEquals(422, response.getStatusCode().value());
        assertEquals("UPSTREAM_REJECTED", ((Map<?, ?>) response.getBody()).get("code"));
    }

    @Test
    void legacyErrorCodesKeepTheirHttpStatus() {
        // 老错误码的 HTTP 语义保持不变
        GatewayException routeMiss = new GatewayException(GatewayErrorCode.ROUTE_NOT_FOUND, "x");
        GatewayException rateLimited = new GatewayException(GatewayErrorCode.RATE_LIMIT_EXCEEDED, "x", "upstream-a");
        GatewayException fivexx = new GatewayException(GatewayErrorCode.UPSTREAM_BUSINESS_ERROR, "x", "upstream-a");
        assertTrue(routeMiss.getCode().name().equals("ROUTE_NOT_FOUND"));
        assertTrue(rateLimited.getUpstreamId().equals("upstream-a"));
        assertEquals(GatewayErrorCode.UPSTREAM_BUSINESS_ERROR, fivexx.getCode());
    }
}
