package com.github.lisama569.resilientapigatewayjava.resilience;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 单实例熔断器，三态模型：
 * <pre>
 *   CLOSED  -- 连续失败达到阈值 --> OPEN
 *   OPEN    -- 等待 openMillis --> HALF_OPEN（仅放少量探测请求）
 *   HALF_OPEN -- 全部探测成功 --> CLOSED（逐步恢复全量）
 *             -- 任一探测失败 --> OPEN（重新计时）
 * </pre>
 * 并发正确性要点：
 * <ul>
 *   <li>所有状态迁移与“检查-占用”都在 synchronized 方法内原子完成；</li>
 *   <li>{@link #isOpen()} 无副作用，用于快速跳过 OPEN 请求（不占探测名额）；</li>
 *   <li>{@link #allowRequest()} 有副作用：必要时完成 OPEN->HALF_OPEN 迁移并
 *       原子发放有限探测名额，超出名额立即拒绝，HALF_OPEN 不会误放行；</li>
 *   <li>探测结果由持名额者通过 {@link #recordSuccess()}/{@link #recordFailure()}
 *       上报，非探测请求的结果不影响熔断恢复。</li>
 * </ul>
 */
public class CircuitBreaker {

    private static final class Snapshot {
        final BreakerState state;
        final long openedAt;
        final int consecutiveFailures;

        Snapshot(BreakerState state, long openedAt, int consecutiveFailures) {
            this.state = state;
            this.openedAt = openedAt;
            this.consecutiveFailures = consecutiveFailures;
        }
    }

    private final int failureThreshold;
    private final long openMillis;
    private final int halfOpenPermits;

    private final AtomicReference<Snapshot> snapshot =
            new AtomicReference<>(new Snapshot(BreakerState.CLOSED, 0L, 0));
    private final AtomicInteger halfOpenTaken = new AtomicInteger(0);
    private final AtomicInteger halfOpenSucceeded = new AtomicInteger(0);

    public CircuitBreaker(int failureThreshold, long openMillis, int halfOpenPermits) {
        this.failureThreshold = failureThreshold;
        this.openMillis = openMillis;
        this.halfOpenPermits = Math.max(1, halfOpenPermits);
    }

    /**
     * 无副作用的 OPEN 判定：OPEN 且恢复窗口未到 -> true（应快速跳过）。
     * 恢复窗口已到时不返回 true，留给 {@link #allowRequest()} 原子迁移。
     */
    public synchronized boolean isOpen() {
        Snapshot s = snapshot.get();
        return s.state == BreakerState.OPEN
                && System.currentTimeMillis() - s.openedAt < openMillis;
    }

    /**
     * 有副作用的放行判定。仅在 CLOSED 或“OPEN 窗口已到 / HALF_OPEN 有名额”
     * 时返回 true；进入 HALF_OPEN 的迁移与探测名额发放在此原子完成。
     * 返回 false 表示此刻应被快速拒绝（HALF_OPEN 探测名额已满）。
     */
    public synchronized boolean allowRequest() {
        Snapshot s = snapshot.get();
        long now = System.currentTimeMillis();
        if (s.state == BreakerState.OPEN && now - s.openedAt >= openMillis) {
            snapshot.set(new Snapshot(BreakerState.HALF_OPEN, s.openedAt,
                    s.consecutiveFailures));
            halfOpenTaken.set(0);
            halfOpenSucceeded.set(0);
            return halfOpenTaken.incrementAndGet() <= halfOpenPermits;
        }
        if (s.state == BreakerState.HALF_OPEN) {
            return halfOpenTaken.incrementAndGet() <= halfOpenPermits;
        }
        return true;
    }

    public BreakerState state() {
        return snapshot.get().state;
    }

    /** 上报一次成功（调用方持有调用名额）。 */
    public synchronized void recordSuccess() {
        Snapshot s = snapshot.get();
        if (s.state == BreakerState.HALF_OPEN) {
            if (halfOpenSucceeded.incrementAndGet() >= halfOpenPermits) {
                snapshot.set(new Snapshot(BreakerState.CLOSED, 0L, 0));
            }
        } else if (s.state == BreakerState.CLOSED && s.consecutiveFailures > 0) {
            snapshot.set(new Snapshot(BreakerState.CLOSED, 0L, 0));
        }
    }

    /**
     * 上报一次“既不算成功也不算失败”的结果（如 4xx 客户端错误）：
     * 不改变连续失败计数（CLOSED 下），避免与上游健康无关的请求影响熔断；
     * 但在 HALF_OPEN 下仍视为探测完成并成功（请求确实到达并得到了确定响应）。
     */
    public synchronized void recordNeutral() {
        Snapshot s = snapshot.get();
        if (s.state == BreakerState.HALF_OPEN) {
            if (halfOpenSucceeded.incrementAndGet() >= halfOpenPermits) {
                snapshot.set(new Snapshot(BreakerState.CLOSED, 0L, 0));
            }
        }
    }

    /** 上报一次失败。HALF_OPEN 下任意失败立即重新 OPEN；CLOSED 下累计阈值后 OPEN。 */
    public synchronized void recordFailure() {
        Snapshot s = snapshot.get();
        if (s.state == BreakerState.HALF_OPEN) {
            snapshot.set(new Snapshot(BreakerState.OPEN, System.currentTimeMillis(),
                    s.consecutiveFailures));
            return;
        }
        int failures = s.consecutiveFailures + 1;
        if (failures >= failureThreshold) {
            snapshot.set(new Snapshot(BreakerState.OPEN, System.currentTimeMillis(),
                    failures));
        } else {
            snapshot.set(new Snapshot(BreakerState.CLOSED, 0L, failures));
        }
    }

    public int halfOpenInflight() {
        return halfOpenTaken.get();
    }
}
