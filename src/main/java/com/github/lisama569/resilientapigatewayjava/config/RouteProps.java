package com.github.lisama569.resilientapigatewayjava.config;

import java.util.ArrayList;
import java.util.List;

/** 单条路由规则的配置。 */
public class RouteProps {

    private String id;
    private String pathPrefix;
    private List<String> instances = new ArrayList<>();

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getPathPrefix() { return pathPrefix; }
    public void setPathPrefix(String pathPrefix) { this.pathPrefix = pathPrefix; }
    public List<String> getInstances() { return instances; }
    public void setInstances(List<String> instances) { this.instances = instances; }
}
