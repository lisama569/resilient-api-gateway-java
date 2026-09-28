package com.github.lisama569.resilientapigatewayjava.gateway.error;

/**
 * 单次上游调用的归一化结果。
 */
public class UpstreamResult {

    public enum FailureKind {
        NONE, TIMEOUT, CONNECT_FAILURE, BUSINESS_ERROR, REJECTED
    }

    private final int status;
    private final String body;
    private final long latencyMs;
    private final FailureKind failureKind;

    private UpstreamResult(int status, String body, long latencyMs, FailureKind failureKind) {
        this.status = status;
        this.body = body;
        this.latencyMs = latencyMs;
        this.failureKind = failureKind;
    }

    public static UpstreamResult success(int status, String body, long latencyMs) {
        return new UpstreamResult(status, body, latencyMs, FailureKind.NONE);
    }

    public static UpstreamResult businessError(int status, String body, long latencyMs) {
        return new UpstreamResult(status, body, latencyMs, FailureKind.BUSINESS_ERROR);
    }

    /**
     * 上游在受理前因调用方原因拒绝（4xx）：不重试、不故障转移、不计入熔断失败。
     */
    public static UpstreamResult rejected(int status, String body, long latencyMs) {
        return new UpstreamResult(status, body, latencyMs, FailureKind.REJECTED);
    }

    public static UpstreamResult timeout(long latencyMs) {
        return new UpstreamResult(0, null, latencyMs, FailureKind.TIMEOUT);
    }

    public static UpstreamResult connectFailure(long latencyMs) {
        return new UpstreamResult(0, null, latencyMs, FailureKind.CONNECT_FAILURE);
    }

    public int getStatus() {
        return status;
    }

    public String getBody() {
        return body;
    }

    public long getLatencyMs() {
        return latencyMs;
    }

    public FailureKind getFailureKind() {
        return failureKind;
    }

    public boolean isSuccess() {
        return failureKind == FailureKind.NONE;
    }
}
