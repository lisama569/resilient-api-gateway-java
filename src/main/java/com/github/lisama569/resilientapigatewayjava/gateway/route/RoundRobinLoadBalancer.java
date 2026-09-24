package com.github.lisama569.resilientapigatewayjava.gateway.route;

import com.github.lisama569.resilientapigatewayjava.upstream.UpstreamInstance;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 轮询负载均衡：在候选健康实例间均匀分发。
 */
public class RoundRobinLoadBalancer implements LoadBalancer {

    private final AtomicInteger cursor = new AtomicInteger();

    @Override
    public UpstreamInstance choose(List<UpstreamInstance> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            return null;
        }
        int index = Math.floorMod(cursor.getAndIncrement(), candidates.size());
        return candidates.get(index);
    }
}
