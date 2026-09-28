package com.github.lisama569.resilientapigatewayjava.tests;

import com.github.lisama569.resilientapigatewayjava.config.GatewayProperties;
import com.github.lisama569.resilientapigatewayjava.gateway.GatewayController;
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
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 经 Spring MVC 路由表验证：/gateway/idempotency/{key} 命中查询入口，
 * 而不会被 /gateway/{routeId}/** 抢走。
 */
class GatewayControllerMvcTest {

    private MockUpstreamServer upstream;
    private MockMvc mvc;

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
        GatewayService service = new GatewayService(properties, new UpstreamClient(),
                new GatewayDiagnostics(), new IdempotencyStore(), new RoundRobinLoadBalancer());
        mvc = MockMvcBuilders.standaloneSetup(new GatewayController(service)).build();
    }

    @AfterEach
    void tearDown() {
        upstream.stop();
    }

    @Test
    void unknownIdempotencyKeyReturns404ViaDedicatedEndpoint() throws Exception {
        mvc.perform(get("/gateway/idempotency/xyz"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("IDEMPOTENT_KEY_NOT_FOUND"))
                .andExpect(jsonPath("$.key").value("xyz"));
    }
}
