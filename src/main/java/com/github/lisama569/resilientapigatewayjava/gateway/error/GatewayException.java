package com.github.lisama569.resilientapigatewayjava.gateway.error;

/**
 * 网关统一异常，携带可区分错误码与相关上游实例。
 */
public class GatewayException extends Exception {

    private final GatewayErrorCode code;
    private final String upstreamId;

    public GatewayException(GatewayErrorCode code, String message) {
        this(code, message, null);
    }

    public GatewayException(GatewayErrorCode code, String message, String upstreamId) {
        super(message);
        this.code = code;
        this.upstreamId = upstreamId;
    }

    public GatewayErrorCode getCode() {
        return code;
    }

    public String getUpstreamId() {
        return upstreamId;
    }
}
