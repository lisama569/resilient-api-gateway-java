package com.github.lisama569.resilientapigatewayjava.resilience;

/** 熔断器状态。 */
public enum BreakerState {
    CLOSED,
    OPEN,
    HALF_OPEN
}
