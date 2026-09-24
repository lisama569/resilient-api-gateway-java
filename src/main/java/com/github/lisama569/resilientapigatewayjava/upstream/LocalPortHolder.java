package com.github.lisama569.resilientapigatewayjava.upstream;

import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

/**
 * 持有当前 Web 服务实际监听端口，供 mock:// 寻址解析使用。
 * 端口在首次使用时惰性读取——此时 Web 服务器必然已启动，
 * 避免在 Bean 构造阶段依赖尚未就绪的 WebServer。
 */
@Component
public class LocalPortHolder {

    private final ApplicationContext context;
    private volatile int cachedPort = -1;

    public LocalPortHolder(ApplicationContext context) {
        this.context = context;
    }

    public int getPort() {
        if (cachedPort < 0) {
            if (context instanceof org.springframework.boot.web.servlet.context
                    .ServletWebServerApplicationContext servletContext) {
                cachedPort = servletContext.getWebServer().getPort();
            } else {
                cachedPort = Integer.parseInt(
                        context.getEnvironment().getProperty("server.port", "8080"));
            }
        }
        return cachedPort;
    }
}
