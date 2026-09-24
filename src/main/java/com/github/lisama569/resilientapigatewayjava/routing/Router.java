package com.github.lisama569.resilientapigatewayjava.routing;

import com.github.lisama569.resilientapigatewayjava.config.GatewayProperties;
import com.github.lisama569.resilientapigatewayjava.config.InstanceProps;
import com.github.lisama569.resilientapigatewayjava.model.RouteRule;
import com.github.lisama569.resilientapigatewayjava.model.UpstreamInstance;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 基于路径前缀的路由器：从配置构建不可变路由表与实例表。
 * 匹配规则：选择“能匹配该路径的、前缀最长的”路由（最长前缀优先），
 * 未命中任何规则即为路由失败，由上层归一化为 ROUTE_NOT_FOUND ——
 * 路由阶段的失败与上游阶段的失败在此时点就被明确区分。
 */
@Component
public class Router {

    private final GatewayProperties properties;
    private final Map<String, UpstreamInstance> instances = new LinkedHashMap<>();
    private final List<RouteRule> rules;

    public Router(GatewayProperties properties) {
        this.properties = properties;
        for (InstanceProps p : properties.getInstances()) {
            instances.put(p.getId(), new UpstreamInstance(p.getId(), p.getBaseUrl()));
        }
        List<RouteRule> built = new ArrayList<>();
        for (var r : properties.getRoutes()) {
            built.add(new RouteRule(r.getId(), r.getPathPrefix(), r.getInstances()));
        }
        built.sort(Comparator.comparingInt(
                (RouteRule r) -> r.getPathPrefix().length()).reversed());
        this.rules = List.copyOf(built);
    }

    public RouteRule match(String path) {
        for (RouteRule rule : rules) {
            if (path.startsWith(rule.getPathPrefix())) {
                return rule;
            }
        }
        return null;
    }

    public List<UpstreamInstance> instancesOf(RouteRule rule) {
        List<UpstreamInstance> result = new ArrayList<>();
        for (String id : rule.getInstanceIds()) {
            UpstreamInstance instance = instances.get(id);
            if (instance != null) {
                result.add(instance);
            }
        }
        return result;
    }

    public GatewayProperties properties() {
        return properties;
    }
}
