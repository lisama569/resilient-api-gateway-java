package com.github.lisama569.resilientapigatewayjava.gateway.diag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 网关诊断：记录路由决策、限流与熔断状态、重试次数。
 * 事件同时写入日志（测试可见）并保存在内存列表中供断言。
 */
public class GatewayDiagnostics {

    private static final Logger log = LoggerFactory.getLogger(GatewayDiagnostics.class);

    private final CopyOnWriteArrayList<String> events = new CopyOnWriteArrayList<>();

    public void record(String event) {
        events.add(event);
        log.info("[gateway-diag] {}", event);
    }

    public List<String> getEvents() {
        return List.copyOf(events);
    }

    public long countEventsContaining(String fragment) {
        return events.stream().filter(e -> e.contains(fragment)).count();
    }

    public void clear() {
        events.clear();
    }
}
