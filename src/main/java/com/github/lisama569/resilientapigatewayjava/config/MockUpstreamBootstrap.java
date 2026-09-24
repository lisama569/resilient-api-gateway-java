package com.github.lisama569.resilientapigatewayjava.config;

import com.github.lisama569.resilientapigatewayjava.upstream.MockUpstreamServer;
import com.github.lisama569.resilientapigatewayjava.upstream.UpstreamInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 本地开发用：按 gateway.upstreams 配置启动同端口的模拟上游，
 * 使服务在本地启动后即可直接体验路由 / 限流 / 熔断行为。
 */
@Component
@ConditionalOnProperty(prefix = "gateway.mock-upstreams", name = "enabled", havingValue = "true")
public class MockUpstreamBootstrap implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(MockUpstreamBootstrap.class);

    private final List<MockUpstreamServer> servers = new ArrayList<>();
    private final GatewayProperties properties;
    private volatile boolean running = false;

    public MockUpstreamBootstrap(GatewayProperties properties) {
        this.properties = properties;
    }

    @Override
    public void start() {
        for (UpstreamInstance upstream : properties.getUpstreams()) {
            MockUpstreamServer server = new MockUpstreamServer(upstream.id(), upstream.port());
            server.start();
            servers.add(server);
            log.info("mock upstream {} started on port {}", upstream.id(), server.getPort());
        }
        running = true;
    }

    @Override
    public void stop() {
        servers.forEach(MockUpstreamServer::stop);
        servers.clear();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
