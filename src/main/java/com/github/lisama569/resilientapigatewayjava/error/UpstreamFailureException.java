package com.github.lisama569.resilientapigatewayjava.error;

/** 归一化的上游调用失败，携带区分性原因与目标实例。 */
public class UpstreamFailureException extends RuntimeException {

    private final FailureKind kind;
    private final String instanceId;
    private final int upstreamStatus;

    public UpstreamFailureException(FailureKind kind, String instanceId,
                                    int upstreamStatus, String message) {
        super(message);
        this.kind = kind;
        this.instanceId = instanceId;
        this.upstreamStatus = upstreamStatus;
    }

    public FailureKind getKind() {
        return kind;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public int getUpstreamStatus() {
        return upstreamStatus;
    }
}
