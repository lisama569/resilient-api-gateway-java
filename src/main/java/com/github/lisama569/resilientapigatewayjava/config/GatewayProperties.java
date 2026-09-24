package com.github.lisama569.resilientapigatewayjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/** 网关配置：上游实例表 + 路由表 + 弹性参数。 */
@Component
@ConfigurationProperties(prefix = "gateway")
public class GatewayProperties {

    private List<InstanceProps> instances = new ArrayList<>();
    private List<RouteProps> routes = new ArrayList<>();
    private int breakerFailureThreshold = 3;
    private long breakerOpenMillis = 3000;
    private int breakerHalfOpenPermits = 1;
    private int retryMaxAttempts = 3;
    private long retryTotalTimeoutMillis = 4000;
    private long callTimeoutMillis = 800;

    public List<InstanceProps> getInstances() { return instances; }
    public void setInstances(List<InstanceProps> instances) { this.instances = instances; }
    public List<RouteProps> getRoutes() { return routes; }
    public void setRoutes(List<RouteProps> routes) { this.routes = routes; }
    public int getBreakerFailureThreshold() { return breakerFailureThreshold; }
    public void setBreakerFailureThreshold(int v) { this.breakerFailureThreshold = v; }
    public long getBreakerOpenMillis() { return breakerOpenMillis; }
    public void setBreakerOpenMillis(long v) { this.breakerOpenMillis = v; }
    public int getBreakerHalfOpenPermits() { return breakerHalfOpenPermits; }
    public void setBreakerHalfOpenPermits(int v) { this.breakerHalfOpenPermits = v; }
    public int getRetryMaxAttempts() { return retryMaxAttempts; }
    public void setRetryMaxAttempts(int v) { this.retryMaxAttempts = v; }
    public long getRetryTotalTimeoutMillis() { return retryTotalTimeoutMillis; }
    public void setRetryTotalTimeoutMillis(long v) { this.retryTotalTimeoutMillis = v; }
    public long getCallTimeoutMillis() { return callTimeoutMillis; }
    public void setCallTimeoutMillis(long v) { this.callTimeoutMillis = v; }
}
