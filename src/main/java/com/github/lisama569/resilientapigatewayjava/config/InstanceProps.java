package com.github.lisama569.resilientapigatewayjava.config;

/** 单个上游实例的配置。 */
public class InstanceProps {

    private String id;
    private String baseUrl;
    private int maxConcurrent = 4;
    private int maxPerSecond = 20;

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getBaseUrl() { return baseUrl; }
    public void setBaseUrl(String baseUrl) { this.baseUrl = baseUrl; }
    public int getMaxConcurrent() { return maxConcurrent; }
    public void setMaxConcurrent(int maxConcurrent) { this.maxConcurrent = maxConcurrent; }
    public int getMaxPerSecond() { return maxPerSecond; }
    public void setMaxPerSecond(int maxPerSecond) { this.maxPerSecond = maxPerSecond; }
}
