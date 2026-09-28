package com.github.lisama569.resilientapigatewayjava.gateway;

import com.github.lisama569.resilientapigatewayjava.gateway.error.GatewayErrorCode;
import com.github.lisama569.resilientapigatewayjava.gateway.error.GatewayException;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 网关 REST 入口：/gateway/{routeId}/** 转发到对应路由的上游组，
 * 归一化错误码映射为可区分的 HTTP 状态与错误体。
 */
@RestController
public class GatewayController {

    private final GatewayService gatewayService;

    public GatewayController(GatewayService gatewayService) {
        this.gatewayService = gatewayService;
    }

    @RequestMapping("/gateway/{routeId}/**")
    public ResponseEntity<?> forward(@PathVariable String routeId,
                                     @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
                                     HttpServletRequest request) {
        String prefix = "/gateway/" + routeId;
        String uri = request.getRequestURI();
        String path = uri.length() > prefix.length() ? uri.substring(prefix.length()) : "/work";
        try {
            String body = gatewayService.handle(routeId, path, idempotencyKey);
            return ResponseEntity.ok(body);
        } catch (GatewayException e) {
            return ResponseEntity.status(toHttpStatus(e.getCode())).body(errorBody(e));
        }
    }

    private HttpStatus toHttpStatus(GatewayErrorCode code) {
        return switch (code) {
            case ROUTE_NOT_FOUND, IDEMPOTENCY_KEY_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case NO_HEALTHY_UPSTREAM, CIRCUIT_OPEN -> HttpStatus.SERVICE_UNAVAILABLE;
            case CONCURRENCY_LIMIT_EXCEEDED, RATE_LIMIT_EXCEEDED -> HttpStatus.TOO_MANY_REQUESTS;
            case IDEMPOTENCY_IN_PROGRESS -> HttpStatus.CONFLICT;
            case UPSTREAM_REJECTED -> HttpStatus.UNPROCESSABLE_ENTITY;
            case UPSTREAM_TIMEOUT, RETRY_BUDGET_EXHAUSTED -> HttpStatus.GATEWAY_TIMEOUT;
            case UPSTREAM_CONNECT_FAILURE, UPSTREAM_BUSINESS_ERROR -> HttpStatus.BAD_GATEWAY;
            default -> HttpStatus.INTERNAL_SERVER_ERROR;
        };
    }

    private Map<String, Object> errorBody(GatewayException e) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("code", e.getCode().name());
        body.put("message", e.getMessage());
        if (e.getUpstreamId() != null) {
            body.put("upstreamId", e.getUpstreamId());
        }
        if (e.getUpstreamStatus() > 0) {
            body.put("upstreamStatus", e.getUpstreamStatus());
        }
        if (e.getDetail() != null) {
            body.put("detail", e.getDetail());
        }
        return body;
    }
}
