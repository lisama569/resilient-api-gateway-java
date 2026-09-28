package com.github.lisama569.resilientapigatewayjava.gateway;

import com.github.lisama569.resilientapigatewayjava.gateway.error.GatewayErrorCode;
import com.github.lisama569.resilientapigatewayjava.gateway.error.GatewayException;
import com.github.lisama569.resilientapigatewayjava.idempotency.IdempotencySnapshot;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 幂等键只读查询入口：GET /idempotency/keys/{key}
 * 调用方超时 / 断连后可先查询，而不是盲目重新投递：
 * <ul>
 *   <li>200 + {"status":"IN_FLIGHT"}：仍在处理，暂时没有结果（不硬报成功/失败）；</li>
 *   <li>200 + {"status":"COMPLETED","body":...}：已完成，返回首个结果；</li>
 *   <li>404 + IDEMPOTENCY_KEY_NOT_FOUND：该键在网关无任何记录。</li>
 * </ul>
 */
@RestController
public class IdempotencyQueryController {

    private final GatewayService gatewayService;

    public IdempotencyQueryController(GatewayService gatewayService) {
        this.gatewayService = gatewayService;
    }

    @GetMapping("/idempotency/keys/{key}")
    public ResponseEntity<?> query(@PathVariable String key) {
        IdempotencySnapshot snapshot = gatewayService.queryStatus(key);
        if (snapshot == null) {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("code", GatewayErrorCode.IDEMPOTENCY_KEY_NOT_FOUND.name());
            body.put("message", "no record for idempotency key '" + key + "'");
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(body);
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("key", key);
        body.put("status", snapshot.status().name());
        if (snapshot.status() == IdempotencySnapshot.Status.COMPLETED) {
            body.put("body", snapshot.body());
        }
        return ResponseEntity.ok(body);
    }
}
