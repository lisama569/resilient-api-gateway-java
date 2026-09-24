package com.github.lisama569.resilientapigatewayjava.model;

import java.util.List;

/** 网关统一对外响应：状态码 + 报文 + 决策轨迹 + 实际尝试次数。 */
public class GatewayResponse {

    private final int status;
    private final String body;
    private final List<String> trace;
    private final int attempts;

    public GatewayResponse(int status, String body, List<String> trace) {
        this(status, body, trace, 0);
    }

    public GatewayResponse(int status, String body, List<String> trace, int attempts) {
        this.status = status;
        this.body = body;
        this.trace = List.copyOf(trace);
        this.attempts = attempts;
    }

    public int getStatus() {
        return status;
    }

    public String getBody() {
        return body;
    }

    public List<String> getTrace() {
        return trace;
    }

    /** 真正向上游发起的调用次数（含首次）；重试次数 = attempts - 1。 */
    public int getAttempts() {
        return attempts;
    }
}
