package com.github.lisama569.resilientapigatewayjava.upstream;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 模拟上游的进程内状态（测试与本地验证的“真实调用”基础）：
 * <ul>
 *   <li>每个实例一种可注入行为：健康 / 延迟 / 超时 / 固定业务错误码 / 前 N 次失败；</li>
 *   <li>全实例共享的幂等键表：同一业务键无论被投递到哪个实例、投递几次，
 *       副作用只计一次（复刻真实带幂等表的上游集群，支撑故障转移不放大副作用）；</li>
 *   <li>管理接口可在运行时切换行为、读取副作用计数并重置。</li>
 * </ul>
 */
@Component
public class MockUpstreamState {

    public enum Mode { HEALTHY, DELAY, TIMEOUT, STATUS, FAIL_FIRST_N }

    public static final class Behavior {
        volatile Mode mode = Mode.HEALTHY;
        volatile long delayMillis = 0;
        volatile int status = 500;
        volatile int failRemaining = 0;
    }

    private record EffectRecord(String instanceId) {
    }

    private final Map<String, Behavior> behaviors = new ConcurrentHashMap<>();
    private final Map<String, EffectRecord> effects = new ConcurrentHashMap<>();
    private final Map<String, Integer> hitCounts = new ConcurrentHashMap<>();

    public Behavior behavior(String instanceId) {
        return behaviors.computeIfAbsent(instanceId, k -> new Behavior());
    }

    public void setHealthy(String instanceId) {
        Behavior b = behavior(instanceId);
        b.mode = Mode.HEALTHY;
    }

    public void setDelay(String instanceId, long delayMillis) {
        Behavior b = behavior(instanceId);
        b.mode = Mode.DELAY;
        b.delayMillis = delayMillis;
    }

    public void setTimeout(String instanceId) {
        behavior(instanceId).mode = Mode.TIMEOUT;
    }

    public void setStatus(String instanceId, int status) {
        Behavior b = behavior(instanceId);
        b.mode = Mode.STATUS;
        b.status = status;
    }

    /** 前 failFirstN 次请求返回 503，之后自动恢复健康（用于熔断恢复探测验证）。 */
    public void setFailFirstN(String instanceId, int failFirstN) {
        Behavior b = behavior(instanceId);
        b.mode = Mode.FAIL_FIRST_N;
        b.failRemaining = failFirstN;
    }

    /**
     * 幂等副作用登记。返回 Optional.empty() 表示这是首次投递（副作用已执行）；
     * 非空表示重复投递，返回值为首次承担该键的实例 id（本次不产生副作用）。
     */
    public Optional<String> registerEffect(String idempotencyKey, String instanceId) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            return Optional.empty();
        }
        EffectRecord previous =
                effects.putIfAbsent(idempotencyKey, new EffectRecord(instanceId));
        return previous == null ? Optional.empty()
                : Optional.of(previous.instanceId());
    }

    public int effectCount() {
        return effects.size();
    }

    public int hitCount(String instanceId) {
        return hitCounts.getOrDefault(instanceId, 0);
    }

    public void recordHit(String instanceId) {
        hitCounts.merge(instanceId, 1, Integer::sum);
    }

    public Set<String> effectKeys() {
        return effects.keySet();
    }

    public void reset() {
        behaviors.clear();
        effects.clear();
        hitCounts.clear();
    }
}
