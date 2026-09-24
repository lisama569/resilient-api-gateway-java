package com.github.lisama569.resilientapigatewayjava.web;

import com.github.lisama569.resilientapigatewayjava.core.GatewayExecutor;
import com.github.lisama569.resilientapigatewayjava.model.GatewayResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 兜底入口：所有未被 /gw、/api、模拟上游管理路径匹配的请求也进入同一
 * {@link GatewayExecutor}，由路由层归一化返回 ROUTE_NOT_FOUND，
 * 从而保证“路由失败”是网关给出的、可区分的对外结论，而不是容器默认 404。
 */
@RestController
@Order(Ordered.LOWEST_PRECEDENCE)
public class CatchAllController {

    private final GatewayExecutor executor;

    public CatchAllController(GatewayExecutor executor) {
        this.executor = executor;
    }

    @RequestMapping("/**")
    public ResponseEntity<String> fallback(HttpServletRequest request,
                                           @RequestBody(required = false) String body) {
        GatewayResponse resp = executor.execute(
                request.getMethod(), request.getRequestURI(), body,
                request.getHeader("Idempotency-Key"));
        return ResponseEntity.status(resp.getStatus())
                .header(GatewayController.TRACE_HEADER,
                        String.join(" || ", resp.getTrace()))
                .body(resp.getBody());
    }
}
