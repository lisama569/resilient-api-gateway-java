package com.github.lisama569.resilientapigatewayjava.config;

import com.github.lisama569.resilientapigatewayjava.gateway.GatewayService;
import com.github.lisama569.resilientapigatewayjava.gateway.UpstreamClient;
import com.github.lisama569.resilientapigatewayjava.gateway.diag.GatewayDiagnostics;
import com.github.lisama569.resilientapigatewayjava.gateway.route.LoadBalancer;
import com.github.lisama569.resilientapigatewayjava.gateway.route.RoundRobinLoadBalancer;
import com.github.lisama569.resilientapigatewayjava.idempotency.IdempotencyStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 网关组件装配。
 */
@Configuration
public class GatewayConfig {

    @Bean
    public UpstreamClient upstreamClient() {
        return new UpstreamClient();
    }

    @Bean
    public GatewayDiagnostics gatewayDiagnostics() {
        return new GatewayDiagnostics();
    }

    @Bean
    public IdempotencyStore idempotencyStore() {
        return new IdempotencyStore();
    }

    @Bean
    public LoadBalancer loadBalancer() {
        return new RoundRobinLoadBalancer();
    }

    @Bean
    public GatewayService gatewayService(GatewayProperties properties,
                                         UpstreamClient client,
                                         GatewayDiagnostics diagnostics,
                                         IdempotencyStore idempotencyStore,
                                         LoadBalancer loadBalancer) {
        return new GatewayService(properties, client, diagnostics, idempotencyStore, loadBalancer);
    }
}
