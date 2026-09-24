package com.github.lisama569.resilientapigatewayjava.web;

import com.github.lisama569.resilientapigatewayjava.core.GatewayExecutor;
import com.github.lisama569.resilientapigatewayjava.model.GatewayResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 网关统一 HTTP 入口。{@code /gw/**} 与 {@code /api/**} 两个入口
 * 完全等价，都交给同一个 {@link GatewayExecutor} 处理，保证路由、限流、
 * 熔断与重试在各入口下行为一致。
 * <p>
 * 响应头 {@code X-Gateway-Trace} 以 {@code ||} 分隔打印每一步
 * 路由决策、限流/熔断状态与重试次数，方便解释与排障。
 */
@RestController
public class GatewayController {

    static final String TRACE_HEADER = "X-Gateway-Trace";
    static final String RETRIES_HEADER = "X-Gateway-Retries";

    private final GatewayExecutor executor;

    public GatewayController(GatewayExecutor executor) {
        this.executor = executor;
    }

    @RequestMapping({"/gw/**", "/api/**"})
    public ResponseEntity<String> forward(HttpServletRequest request,
                                          @RequestBody(required = false) String body) {
        GatewayResponse resp = executor.execute(
                request.getMethod(), request.getRequestURI(), body,
                request.getHeader("Idempotency-Key"));
        List<String> trace = resp.getTrace();
        return ResponseEntity.status(resp.getStatus())
                .header(TRACE_HEADER, String.join(" || ", trace))
                .header(RETRIES_HEADER,
                        String.valueOf(Math.max(0, resp.getAttempts() - 1)))
                .body(resp.getBody());
    }
}
