package com.github.lisama569.resilientapigatewayjava.config;

import com.github.lisama569.resilientapigatewayjava.resilience.CircuitBreaker;
import com.github.lisama569.resilientapigatewayjava.resilience.RateLimiter;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 每上游实例独立持有一个 {@link RateLimiter} 和一个 {@link CircuitBreaker}，
 * 在启动时按配置一次性构建。限流与熔断均按实例维度统计，互不串扰。
 */
@Component
public class ResilienceRegistry {

    private final Map<String, RateLimiter> rateLimiters = new LinkedHashMap<>();
    private final Map<String, CircuitBreaker> breakers = new LinkedHashMap<>();
    private GatewayProperties properties;

    public ResilienceRegistry(GatewayProperties properties) {
        this.properties = properties;
        for (InstanceProps p : properties.getInstances()) {
            rateLimiters.put(p.getId(),
                    new RateLimiter(p.getMaxConcurrent(), p.getMaxPerSecond()));
            breakers.put(p.getId(),
                    new CircuitBreaker(properties.getBreakerFailureThreshold(),
                            properties.getBreakerOpenMillis(),
                            properties.getBreakerHalfOpenPermits()));
        }
    }

    public RateLimiter rateLimiter(String instanceId) {
        return rateLimiters.get(instanceId);
    }

    public CircuitBreaker breaker(String instanceId) {
        return breakers.get(instanceId);
    }

    /** 重新构建全部实例的限流器/熔断器（主要供测试隔离使用）。 */
    public void resetAll() {
        for (InstanceProps p : properties.getInstances()) {
            rateLimiters.put(p.getId(),
                    new RateLimiter(p.getMaxConcurrent(), p.getMaxPerSecond()));
            breakers.put(p.getId(),
                    new CircuitBreaker(properties.getBreakerFailureThreshold(),
                            properties.getBreakerOpenMillis(),
                            properties.getBreakerHalfOpenPermits()));
        }
    }
}
