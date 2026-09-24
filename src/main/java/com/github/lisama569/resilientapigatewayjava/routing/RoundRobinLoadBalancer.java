package com.github.lisama569.resilientapigatewayjava.routing;

import com.github.lisama569.resilientapigatewayjava.model.UpstreamInstance;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 线程安全的轮询负载均衡，游标按路由键（路由 id）独立维护：
 * 不同路由各自从第一个实例开始公平轮转，互不干扰。
 * <p>
 * 单次请求做故障转移时，通过 excludeInstanceIds 排除本轮已尝试的实例，
 * 保证一次业务请求不会无谓地重复打回刚失败的实例；当所有实例都试过
 * 且预算仍允许时，调用方可再次调用本方法（此时清空排除集）重试同一实例。
 */
@Component
public class RoundRobinLoadBalancer {

    private final Map<String, AtomicInteger> cursors = new ConcurrentHashMap<>();

    public UpstreamInstance pick(String routeKey, List<UpstreamInstance> instances,
                                 Set<String> excludeInstanceIds) {
        if (instances == null || instances.isEmpty()) {
            return null;
        }
        AtomicInteger cursor =
                cursors.computeIfAbsent(routeKey, k -> new AtomicInteger(0));
        int start = Math.floorMod(cursor.getAndIncrement(), instances.size());
        for (int offset = 0; offset < instances.size(); offset++) {
            UpstreamInstance candidate =
                    instances.get((start + offset) % instances.size());
            if (excludeInstanceIds == null
                    || !excludeInstanceIds.contains(candidate.getId())) {
                return candidate;
            }
        }
        return null;
    }

    /** 所有路由游标归零（主要供测试隔离使用）。 */
    public void reset() {
        cursors.clear();
    }
}
