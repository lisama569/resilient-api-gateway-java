package com.github.lisama569.resilientapigatewayjava.upstream;

import org.springframework.stereotype.Component;

/**
 * 上游基础地址解析：
 * <ul>
 *   <li>{@code mock://<instanceId>} -> 本进程内的模拟上游
 *       {@code http://127.0.0.1:<port>/__mock/<instanceId>}，
 *       从而用真实本地 HTTP 调用模拟多个上游实例；</li>
 *   <li>普通 {@code http(s)://} 地址原样返回，可对接真实上游；</li>
 *   <li>{@code dead://<name>} 解析到保留封闭端口（1），稳定模拟
 *       “连接被拒绝”的不可用实例。</li>
 * </ul>
 */
@Component
public class MockAddressResolver {

    private final LocalPortHolder portHolder;

    public MockAddressResolver(LocalPortHolder portHolder) {
        this.portHolder = portHolder;
    }

    public String resolve(String baseUrl) {
        if (baseUrl == null) {
            return baseUrl;
        }
        if (baseUrl.startsWith("mock://")) {
            String instanceId = baseUrl.substring("mock://".length());
            return "http://127.0.0.1:" + portHolder.getPort()
                    + "/__mock/" + instanceId;
        }
        if (baseUrl.startsWith("dead://")) {
            return "http://127.0.0.1:1";
        }
        return baseUrl;
    }
}
