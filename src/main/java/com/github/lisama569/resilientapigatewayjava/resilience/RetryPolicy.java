package com.github.lisama569.resilientapigatewayjava.resilience;

import com.github.lisama569.resilientapigatewayjava.error.FailureKind;

/**
 * 重试策略与整体超时预算。
 * <ul>
 *   <li>可重试：连接失败（{@link FailureKind#UPSTREAM_CONNECT}）、
 *       超时（{@link FailureKind#UPSTREAM_TIMEOUT}）、
 *       以及上游 502/503/504 暂时性业务错误；</li>
 *   <li>不可重试：4xx、500 等确定性业务错误，直接归一化对外返回；</li>
 *   <li>所有重试共享一个总预算（次数 + 总耗时），预算用尽即
 *       {@link FailureKind#RETRY_BUDGET_EXHAUSTED}，避免无限放大。</li>
 * </ul>
 */
public class RetryPolicy {

    private final int maxAttempts;
    private final long totalTimeoutMillis;

    public RetryPolicy(int maxAttempts, long totalTimeoutMillis) {
        this.maxAttempts = maxAttempts;
        this.totalTimeoutMillis = totalTimeoutMillis;
    }

    public boolean isRetryable(FailureKind kind, int upstreamStatus) {
        return switch (kind) {
            case UPSTREAM_CONNECT, UPSTREAM_TIMEOUT -> true;
            case UPSTREAM_BUSINESS -> upstreamStatus == 502
                    || upstreamStatus == 503 || upstreamStatus == 504;
            default -> false;
        };
    }

    /** 是否还能再发起一次尝试：次数未超上限，且已用时间未超总预算。 */
    public boolean hasBudget(long elapsedMillis, int attemptsDone) {
        return attemptsDone < maxAttempts && elapsedMillis < totalTimeoutMillis;
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public long totalTimeoutMillis() {
        return totalTimeoutMillis;
    }
}
