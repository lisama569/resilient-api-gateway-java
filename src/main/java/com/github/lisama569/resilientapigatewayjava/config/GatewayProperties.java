package com.github.lisama569.resilientapigatewayjava.config;

import com.github.lisama569.resilientapigatewayjava.gateway.route.RouteRule;
import com.github.lisama569.resilientapigatewayjava.upstream.UpstreamInstance;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 网关配置：上游实例、路由规则、重试与超时预算、熔断参数。
 */
@Component
@ConfigurationProperties(prefix = "gateway")
public class GatewayProperties {

    private List<UpstreamInstance> upstreams = List.of();
    private List<RouteRule> routes = List.of();
    private int maxRetries = 2;
    private long overallTimeoutMs = 3000;
    private long perAttemptTimeoutMs = 1000;
    private int circuitFailureThreshold = 3;
    private long circuitOpenDurationMs = 2000;
    private int circuitHalfOpenPermits = 1;
    /** 晚到的同键请求等待先到请求结果的最长时间；超时后快速返回“处理中”。 */
    private long idempotencyWaitMs = 1000;

    public List<UpstreamInstance> getUpstreams() {
        return upstreams;
    }

    public void setUpstreams(List<UpstreamInstance> upstreams) {
        this.upstreams = upstreams;
    }

    public List<RouteRule> getRoutes() {
        return routes;
    }

    public void setRoutes(List<RouteRule> routes) {
        this.routes = routes;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public long getOverallTimeoutMs() {
        return overallTimeoutMs;
    }

    public void setOverallTimeoutMs(long overallTimeoutMs) {
        this.overallTimeoutMs = overallTimeoutMs;
    }

    public long getPerAttemptTimeoutMs() {
        return perAttemptTimeoutMs;
    }

    public void setPerAttemptTimeoutMs(long perAttemptTimeoutMs) {
        this.perAttemptTimeoutMs = perAttemptTimeoutMs;
    }

    public int getCircuitFailureThreshold() {
        return circuitFailureThreshold;
    }

    public void setCircuitFailureThreshold(int circuitFailureThreshold) {
        this.circuitFailureThreshold = circuitFailureThreshold;
    }

    public long getCircuitOpenDurationMs() {
        return circuitOpenDurationMs;
    }

    public void setCircuitOpenDurationMs(long circuitOpenDurationMs) {
        this.circuitOpenDurationMs = circuitOpenDurationMs;
    }

    public int getCircuitHalfOpenPermits() {
        return circuitHalfOpenPermits;
    }

    public void setCircuitHalfOpenPermits(int circuitHalfOpenPermits) {
        this.circuitHalfOpenPermits = circuitHalfOpenPermits;
    }

    public long getIdempotencyWaitMs() {
        return idempotencyWaitMs;
    }

    public void setIdempotencyWaitMs(long idempotencyWaitMs) {
        this.idempotencyWaitMs = idempotencyWaitMs;
    }
}
