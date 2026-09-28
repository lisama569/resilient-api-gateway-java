package com.github.lisama569.resilientapigatewayjava.tests;

import com.github.lisama569.resilientapigatewayjava.config.GatewayProperties;
import com.github.lisama569.resilientapigatewayjava.gateway.GatewayController;
import com.github.lisama569.resilientapigatewayjava.gateway.GatewayService;
import com.github.lisama569.resilientapigatewayjava.gateway.IdempotencyQueryController;
import com.github.lisama569.resilientapigatewayjava.gateway.error.GatewayErrorCode;
import com.github.lisama569.resilientapigatewayjava.gateway.error.GatewayException;
import com.github.lisama569.resilientapigatewayjava.idempotency.IdempotencySnapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 控制器层：新错误码到 HTTP 状态 / 错误体的映射，以及只读查询入口的 JSON 形态。
 * 使用 standalone MockMvc + 手写桩，不启动 Web 容器。
 */
class GatewayHttpMappingTest {

    private MockMvc mvc;
    private StubGatewayService stub;

    /** 手写桩，避免依赖字节码 mock（对新 JDK 版本更稳）。 */
    static class StubGatewayService extends GatewayService {
        volatile GatewayException forwardError;
        volatile String forwardResult = "ok";
        volatile IdempotencySnapshot snapshot;

        StubGatewayService() {
            super(new GatewayProperties(), null, null, null, null);
        }

        @Override
        public String handle(String routeId, String path, String idempotencyKey)
                throws GatewayException {
            if (forwardError != null) {
                throw forwardError;
            }
            return forwardResult;
        }

        @Override
        public IdempotencySnapshot queryStatus(String key) {
            return snapshot;
        }
    }

    @BeforeEach
    void setUp() {
        stub = new StubGatewayService();
        mvc = MockMvcBuilders.standaloneSetup(
                        new GatewayController(stub),
                        new IdempotencyQueryController(stub))
                .build();
    }

    @Test
    void upstreamRejectedMapsTo422WithStatusAndDetail() throws Exception {
        stub.forwardError = new GatewayException(GatewayErrorCode.UPSTREAM_REJECTED,
                "upstream upstream-a rejected the request as client error",
                "upstream-a", 400, "invalid request");

        mvc.perform(post("/gateway/orders/work").header("Idempotency-Key", "k1"))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.code").value("UPSTREAM_REJECTED"))
                .andExpect(jsonPath("$.upstreamId").value("upstream-a"))
                .andExpect(jsonPath("$.upstreamStatus").value(400))
                .andExpect(jsonPath("$.detail").value("invalid request"));
    }

    @Test
    void idempotencyInProgressMapsTo409WithDistinctCode() throws Exception {
        stub.forwardError = new GatewayException(GatewayErrorCode.IDEMPOTENCY_IN_PROGRESS,
                "request with idempotency key 'k2' is still being processed",
                null, 0, "k2");

        mvc.perform(post("/gateway/orders/work").header("Idempotency-Key", "k2"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("IDEMPOTENCY_IN_PROGRESS"))
                .andExpect(jsonPath("$.detail").value("k2"));
    }

    @Test
    void queryUnknownKeyMapsTo404() throws Exception {
        stub.snapshot = null;

        mvc.perform(get("/idempotency/keys/missing"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("IDEMPOTENCY_KEY_NOT_FOUND"));
    }

    @Test
    void queryInFlightKeyReportsProcessing() throws Exception {
        stub.snapshot = new IdempotencySnapshot(IdempotencySnapshot.Status.IN_FLIGHT, null);

        mvc.perform(get("/idempotency/keys/k3"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.key").value("k3"))
                .andExpect(jsonPath("$.status").value("IN_FLIGHT"));
    }

    @Test
    void queryCompletedKeyReturnsBody() throws Exception {
        stub.snapshot = new IdempotencySnapshot(IdempotencySnapshot.Status.COMPLETED,
                "upstream=upstream-a sideEffect=1");

        mvc.perform(get("/idempotency/keys/k4"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPLETED"))
                .andExpect(jsonPath("$.body").value("upstream=upstream-a sideEffect=1"));
    }
}
