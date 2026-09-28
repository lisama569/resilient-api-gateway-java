package com.github.lisama569.resilientapigatewayjava.gateway.error;

/**
 * 网关统一异常，携带可区分错误码、相关上游实例以及（如适用）上游状态码与拒绝详情。
 */
public class GatewayException extends Exception {

    private final GatewayErrorCode code;
    private final String upstreamId;
    private final int upstreamStatus;
    private final String detail;

    public GatewayException(GatewayErrorCode code, String message) {
        this(code, message, null, 0, null);
    }

    public GatewayException(GatewayErrorCode code, String message, String upstreamId) {
        this(code, message, upstreamId, 0, null);
    }

    public GatewayException(GatewayErrorCode code, String message, String upstreamId,
                            int upstreamStatus, String detail) {
        super(message);
        this.code = code;
        this.upstreamId = upstreamId;
        this.upstreamStatus = upstreamStatus;
        this.detail = detail;
    }

    public GatewayErrorCode getCode() {
        return code;
    }

    public String getUpstreamId() {
        return upstreamId;
    }

    /** 相关上游 HTTP 状态码（如 4xx 拒绝）；不适用时为 0。 */
    public int getUpstreamStatus() {
        return upstreamStatus;
    }

    /** 上游返回的拒绝详情 / 错误体；不适用时为 null。 */
    public String getDetail() {
        return detail;
    }
}
