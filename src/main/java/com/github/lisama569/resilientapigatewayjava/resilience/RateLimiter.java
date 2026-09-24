package com.github.lisama569.resilientapigatewayjava.resilience;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 单上游实例维度的入口限制器：
 * <ul>
 *   <li>并发闸：非阻塞信号量，超过 maxConcurrent 立即拒绝；</li>
 *   <li>速率限制：1 秒固定窗口计数，超过 maxPerSecond 立即拒绝。</li>
 * </ul>
 * 两类拒绝原因通过 {@link AcquireStatus} 区分，且绝不排队等待，
 * 因此不会把压力传导给上游，也不会占用重试预算去阻塞。
 */
public class RateLimiter {

    public enum AcquireStatus { ALLOWED, CONCURRENCY_EXHAUSTED, RATE_EXHAUSTED }

    private final int maxConcurrent;
    private final int maxPerSecond;
    private final Semaphore inflight;
    private final AtomicInteger windowCount = new AtomicInteger(0);
    private volatile long windowStartMillis = System.currentTimeMillis();

    public RateLimiter(int maxConcurrent, int maxPerSecond) {
        this.maxConcurrent = maxConcurrent;
        this.maxPerSecond = maxPerSecond;
        this.inflight = new Semaphore(maxConcurrent, true);
    }

    /** 非阻塞获取一次调用配额。 */
    public AcquireStatus tryAcquire() {
        if (!inflight.tryAcquire()) {
            return AcquireStatus.CONCURRENCY_EXHAUSTED;
        }
        long now = System.currentTimeMillis();
        if (now - windowStartMillis >= 1000L) {
            synchronized (this) {
                if (now - windowStartMillis >= 1000L) {
                    windowStartMillis = now;
                    windowCount.set(0);
                }
            }
        }
        if (windowCount.incrementAndGet() > maxPerSecond) {
            inflight.release();
            return AcquireStatus.RATE_EXHAUSTED;
        }
        return AcquireStatus.ALLOWED;
    }

    /** 调用结束后归还并发名额（速率名额不归还）。 */
    public void release() {
        inflight.release();
    }

    public int currentInflight() {
        return maxConcurrent - inflight.availablePermits();
    }

    public int currentWindowCount() {
        return windowCount.get();
    }
}
