package com.github.lisama569.resilientapigatewayjava.model;

/** 上游实例（逻辑标识 + 基础地址）。 */
public class UpstreamInstance {

    private final String id;
    private final String baseUrl;

    public UpstreamInstance(String id, String baseUrl) {
        this.id = id;
        this.baseUrl = baseUrl;
    }

    public String getId() {
        return id;
    }

    public String getBaseUrl() {
        return baseUrl;
    }
}
