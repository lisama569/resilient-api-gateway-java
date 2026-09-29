package com.github.lisama569.resilientapigatewayjava.tests;

import com.github.lisama569.resilientapigatewayjava.gateway.circuit.CircuitBreaker;
import com.github.lisama569.resilientapigatewayjava.gateway.circuit.CircuitState;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 熔断器状态机单测（不依赖网络）：调用方拒绝（4xx）在各阶段都不计失败、不熔断；
 * HALF_OPEN 下只归还探测许可并保持半开，恢复探测成功仍可闭合。
 */
class CircuitBreakerClientRejectionTest {

    @Test
    void clientRejectionInClosedStateNeverCountsAsFailure() {
        CircuitBreaker breaker = new CircuitBreaker("upstream-a", 2, 10_000, 1);
        for (int i = 0; i < 10; i++) {
            assertTrue(breaker.tryAcquirePermit());
            breaker.onClientRejected();
            assertEquals(CircuitState.CLOSED, breaker.getState());
            assertEquals(0, breaker.getConsecutiveFailures());
        }
    }

    @Test
    void clientRejectionDuringHalfOpenReleasesPermitAndStaysHalfOpen() throws Exception {
        CircuitBreaker breaker = new CircuitBreaker("upstream-a", 1, 50, 1);

        // 一次内部故障即熔断
        assertTrue(breaker.tryAcquirePermit());
        breaker.onFailure();
        assertEquals(CircuitState.OPEN, breaker.getState());

        // 冷却结束进入半开，占用唯一探测许可
        Thread.sleep(80);
        assertTrue(breaker.tryAcquirePermit(), "冷却后应能拿到探测许可");
        assertEquals(CircuitState.HALF_OPEN, breaker.getState());
        assertFalse(breaker.tryAcquirePermit(), "唯一探测许可已被占用");

        // 试探结果是调用方拒绝：归还许可，但保持半开、不重新熔断、不新增失败
        breaker.onClientRejected();
        assertEquals(CircuitState.HALF_OPEN, breaker.getState());
        assertTrue(breaker.tryAcquirePermit(), "4xx 后探测许可必须可再次获取");

        // 上游真正恢复：探测成功闭合，失败计数清零
        breaker.onSuccess();
        assertEquals(CircuitState.CLOSED, breaker.getState());
        assertEquals(0, breaker.getConsecutiveFailures());
    }

    @Test
    void repeatedClientRejectionProbesDoNotBlockEventualRecovery() throws Exception {
        CircuitBreaker breaker = new CircuitBreaker("upstream-a", 1, 50, 2);
        assertTrue(breaker.tryAcquirePermit());
        breaker.onFailure();
        Thread.sleep(80);
        assertEquals(CircuitState.HALF_OPEN, breaker.getState());

        // 连续多轮“探测被 4xx 拒”，许可始终能循环使用，状态始终停留半开
        for (int round = 0; round < 5; round++) {
            assertTrue(breaker.tryAcquirePermit(), "round " + round);
            assertTrue(breaker.tryAcquirePermit(), "round " + round);
            breaker.onClientRejected();
            breaker.onClientRejected();
            assertEquals(CircuitState.HALF_OPEN, breaker.getState(), "round " + round);
        }

        // 最终一个成功探测即可闭合
        assertTrue(breaker.tryAcquirePermit());
        breaker.onSuccess();
        assertEquals(CircuitState.CLOSED, breaker.getState());
        assertEquals(0, breaker.getConsecutiveFailures());
    }

    @Test
    void internalFailureDuringHalfOpenStillReopens() throws Exception {
        CircuitBreaker breaker = new CircuitBreaker("upstream-a", 1, 50, 1);
        assertTrue(breaker.tryAcquirePermit());
        breaker.onFailure();
        Thread.sleep(80);
        assertTrue(breaker.tryAcquirePermit());
        assertEquals(CircuitState.HALF_OPEN, breaker.getState());

        // 4xx 不影响后续真正内部故障的语义：半开探测遇 5xx 仍重新熔断
        breaker.onClientRejected();
        assertTrue(breaker.tryAcquirePermit());
        breaker.onFailure();
        assertEquals(CircuitState.OPEN, breaker.getState());
    }
}
