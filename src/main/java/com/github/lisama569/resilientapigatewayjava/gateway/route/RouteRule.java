package com.github.lisama569.resilientapigatewayjava.gateway.route;

import java.util.List;

/**
 * 路由规则：路径前缀匹配到一组上游实例。
 */
public record RouteRule(String routeId, String pathPrefix, List<String> upstreamIds) {
}
