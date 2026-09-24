package com.github.lisama569.resilientapigatewayjava.gateway.limit;

import java.util.concurrent.Semaphore;

/**
 * 按上游维度的并发限制（信号量），超限快速拒绝，不排队。
 */
public class ConcurrencyLimiter {

    private final Semaphore semaphore;
    private final int maxConcurrency;

    public ConcurrencyLimiter(int maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
        this.semaphore = new Semaphore(maxConcurrency, true);
    }

    /** 尝试获取许可，不等待。 */
    public boolean tryAcquire() {
        return semaphore.tryAcquire();
    }

    public void release() {
        semaphore.release();
    }

    public int getInFlight() {
        return maxConcurrency - semaphore.availablePermits();
    }

    public int getMaxConcurrency() {
        return maxConcurrency;
    }
}
