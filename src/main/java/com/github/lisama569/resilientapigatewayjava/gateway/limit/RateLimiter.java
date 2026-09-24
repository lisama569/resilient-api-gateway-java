package com.github.lisama569.resilientapigatewayjava.gateway.limit;

/**
 * 按上游维度的速率限制（令牌桶），超限快速拒绝。
 * 惰性补充令牌，synchronized 保证并发下不超发。
 */
public class RateLimiter {

    private final double permitsPerSecond;
    private final double maxTokens;
    private double tokens;
    private long lastRefillNanos;

    public RateLimiter(double permitsPerSecond) {
        this.permitsPerSecond = permitsPerSecond;
        // 桶容量允许 1 秒突发
        this.maxTokens = Math.max(1.0, permitsPerSecond);
        this.tokens = this.maxTokens;
        this.lastRefillNanos = System.nanoTime();
    }

    /** 尝试获取一个令牌，不等待。 */
    public synchronized boolean tryAcquire() {
        refill();
        if (tokens >= 1.0) {
            tokens -= 1.0;
            return true;
        }
        return false;
    }

    private void refill() {
        long now = System.nanoTime();
        long elapsed = now - lastRefillNanos;
        if (elapsed > 0) {
            tokens = Math.min(maxTokens, tokens + elapsed * permitsPerSecond / 1_000_000_000.0);
            lastRefillNanos = now;
        }
    }

    public double getPermitsPerSecond() {
        return permitsPerSecond;
    }

    /** 当前可用令牌数（测试与诊断用）。 */
    public synchronized double getAvailableTokens() {
        refill();
        return tokens;
    }
}
