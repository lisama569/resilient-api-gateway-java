package com.github.lisama569.resilientapigatewayjava.gateway.route;

import com.github.lisama569.resilientapigatewayjava.upstream.UpstreamInstance;

import java.util.List;

/**
 * 负载均衡策略接口。
 */
public interface LoadBalancer {

    /** 从候选实例中选择一个，返回 null 表示无可用实例。 */
    UpstreamInstance choose(List<UpstreamInstance> candidates);
}
