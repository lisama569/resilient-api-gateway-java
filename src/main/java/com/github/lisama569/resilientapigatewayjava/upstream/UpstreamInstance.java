package com.github.lisama569.resilientapigatewayjava.upstream;

/**
 * 上游实例配置。
 */
public record UpstreamInstance(
        String id,
        String host,
        int port,
        int maxConcurrency,
        double rateLimitPerSecond,
        int weight) {

    public String baseUrl() {
        return "http://" + host + ":" + port;
    }
}
