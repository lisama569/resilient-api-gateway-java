package com.github.lisama569.resilientapigatewayjava.gateway.circuit;

/**
 * 按上游维度的熔断器。
 * CLOSED：正常放行，连续失败达到阈值后熔断；
 * OPEN：快速拒绝，冷却期结束后进入 HALF_OPEN；
 * HALF_OPEN：仅放行有限个探测请求，探测成功则闭合，失败则重新熔断。
 * 所有状态迁移在 synchronized 内完成，并发下不会误放行或状态错乱。
 */
public class CircuitBreaker {

    private final String upstreamId;
    private final int failureThreshold;
    private final long openDurationNanos;
    private final int halfOpenPermits;

    private CircuitState state = CircuitState.CLOSED;
    private int consecutiveFailures = 0;
    private long openedAtNanos = 0;
    private int halfOpenInFlight = 0;

    public CircuitBreaker(String upstreamId, int failureThreshold, long openDurationMs, int halfOpenPermits) {
        this.upstreamId = upstreamId;
        this.failureThreshold = failureThreshold;
        this.openDurationNanos = openDurationMs * 1_000_000L;
        this.halfOpenPermits = halfOpenPermits;
    }

    /** 尝试获取调用许可（半开状态下为探测许可）。 */
    public synchronized boolean tryAcquirePermit() {
        maybeTransitionToHalfOpen();
        switch (state) {
            case CLOSED -> {
                return true;
            }
            case OPEN -> {
                return false;
            }
            case HALF_OPEN -> {
                if (halfOpenInFlight < halfOpenPermits) {
                    halfOpenInFlight++;
                    return true;
                }
                return false;
            }
        }
        return false;
    }

    public synchronized void onSuccess() {
        if (state == CircuitState.HALF_OPEN) {
            halfOpenInFlight = Math.max(0, halfOpenInFlight - 1);
            // 探测成功：闭合熔断器
            state = CircuitState.CLOSED;
            consecutiveFailures = 0;
        } else if (state == CircuitState.CLOSED) {
            consecutiveFailures = 0;
        }
    }

    public synchronized void onFailure() {
        if (state == CircuitState.HALF_OPEN) {
            halfOpenInFlight = Math.max(0, halfOpenInFlight - 1);
            // 探测失败：重新熔断，重新计时
            open();
        } else if (state == CircuitState.CLOSED) {
            consecutiveFailures++;
            if (consecutiveFailures >= failureThreshold) {
                open();
            }
        }
    }

    public synchronized CircuitState getState() {
        maybeTransitionToHalfOpen();
        return state;
    }

    public synchronized int getConsecutiveFailures() {
        return consecutiveFailures;
    }

    public String getUpstreamId() {
        return upstreamId;
    }

    private void maybeTransitionToHalfOpen() {
        if (state == CircuitState.OPEN
                && System.nanoTime() - openedAtNanos >= openDurationNanos) {
            state = CircuitState.HALF_OPEN;
            halfOpenInFlight = 0;
        }
    }

    private void open() {
        state = CircuitState.OPEN;
        openedAtNanos = System.nanoTime();
    }
}
