package com.github.lisama569.resilientapigatewayjava.model;

import java.util.List;

/** 路由规则：路径前缀 -> 上游实例组。 */
public class RouteRule {

    private final String id;
    private final String pathPrefix;
    private final List<String> instanceIds;

    public RouteRule(String id, String pathPrefix, List<String> instanceIds) {
        this.id = id;
        this.pathPrefix = pathPrefix;
        this.instanceIds = List.copyOf(instanceIds);
    }

    public String getId() {
        return id;
    }

    public String getPathPrefix() {
        return pathPrefix;
    }

    public List<String> getInstanceIds() {
        return instanceIds;
    }
}
