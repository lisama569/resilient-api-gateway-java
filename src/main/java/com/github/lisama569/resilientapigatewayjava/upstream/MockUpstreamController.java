package com.github.lisama569.resilientapigatewayjava.upstream;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 本地可启动的模拟上游。
 * <p>
 * 业务入口：{@code /__mock/{instanceId}/**}（GET 只读回显；POST 产生一次
 * 带幂等保护的副作用）。网关用真实 HTTP 调用这些端点，实例行为由
 * {@link MockUpstreamState} 的进程内状态决定。
 * <p>
 * 管理入口：{@code /__mock-admin/**}，供测试在运行时注入
 * 延迟/超时/业务错误码/前 N 次失败，并读取副作用计数与重置状态。
 */
@RestController
public class MockUpstreamController {

    private final MockUpstreamState state;

    public MockUpstreamController(MockUpstreamState state) {
        this.state = state;
    }

    @RequestMapping("/__mock/{instanceId}/**")
    public ResponseEntity<String> mockCall(@PathVariable String instanceId,
                                           HttpServletRequest request) {
        state.recordHit(instanceId);
        MockUpstreamState.Behavior behavior = state.behavior(instanceId);
        String key = request.getHeader("Idempotency-Key");

        switch (behavior.mode) {
            case TIMEOUT -> sleepQuietly(10_000L);
            case DELAY -> sleepQuietly(behavior.delayMillis);
            case STATUS -> {
                return ResponseEntity.status(behavior.status)
                        .body("mock-upstream-error:" + behavior.status);
            }
            case FAIL_FIRST_N -> {
                if (behavior.failRemaining > 0) {
                    behavior.failRemaining--;
                    return ResponseEntity.status(503)
                            .body("mock-upstream-temporary-failure");
                }
            }
            case HEALTHY -> {
            }
        }

        // 行为异常时在任何副作用之前返回，保证“上游业务错误”不产生副作用
        if (!"POST".equalsIgnoreCase(request.getMethod())) {
            return ResponseEntity.ok("{\"instance\":\"" + instanceId + "\",\"ok\":true}");
        }
        Optional<String> duplicate = state.registerEffect(key, instanceId);
        String body = duplicate.isEmpty()
                ? "{\"createdBy\":\"" + instanceId + "\",\"order\":\"" + key + "\"}"
                : "{\"createdBy\":\"" + duplicate.get()
                        + "\",\"duplicate\":true,\"order\":\"" + key + "\"}";
        HttpHeaders headers = new HttpHeaders();
        if (duplicate.isPresent()) {
            headers.add("X-Upstream-Duplicate", "true");
        }
        return new ResponseEntity<>(body, headers, 201);
    }

    @RequestMapping("/__mock-admin/state")
    public Map<String, Object> snapshot() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("effectCount", state.effectCount());
        out.put("effectKeys", state.effectKeys());
        return out;
    }

    @RequestMapping("/__mock-admin/reset")
    public Map<String, Object> reset() {
        state.reset();
        return Map.of("reset", true);
    }

    @RequestMapping("/__mock-admin/{instanceId}/healthy")
    public Map<String, Object> healthy(@PathVariable String instanceId) {
        state.setHealthy(instanceId);
        return Map.of("instance", instanceId, "mode", "HEALTHY");
    }

    @RequestMapping("/__mock-admin/{instanceId}/delay")
    public Map<String, Object> delay(@PathVariable String instanceId,
                                     @RequestParam(defaultValue = "1200") long millis) {
        state.setDelay(instanceId, millis);
        return Map.of("instance", instanceId, "mode", "DELAY", "millis", millis);
    }

    @RequestMapping("/__mock-admin/{instanceId}/timeout")
    public Map<String, Object> timeout(@PathVariable String instanceId) {
        state.setTimeout(instanceId);
        return Map.of("instance", instanceId, "mode", "TIMEOUT");
    }

    @RequestMapping("/__mock-admin/{instanceId}/status")
    public Map<String, Object> status(@PathVariable String instanceId,
                                      @RequestParam(defaultValue = "500") int code) {
        state.setStatus(instanceId, code);
        return Map.of("instance", instanceId, "mode", "STATUS", "code", code);
    }

    @RequestMapping("/__mock-admin/{instanceId}/fail-first-n")
    public Map<String, Object> failFirstN(@PathVariable String instanceId,
                                          @RequestParam(defaultValue = "3") int n) {
        state.setFailFirstN(instanceId, n);
        return Map.of("instance", instanceId, "mode", "FAIL_FIRST_N", "n", n);
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
