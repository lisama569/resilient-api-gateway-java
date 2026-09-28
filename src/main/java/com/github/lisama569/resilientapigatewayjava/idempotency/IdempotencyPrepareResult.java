package com.github.lisama569.resilientapigatewayjava.idempotency;

/**
 * {@link IdempotencyStore#prepare} 的结果：
 * 成为处理方（leader）、等待后拿到已完成结果、或等待超时仍在处理。
 */
public class IdempotencyPrepareResult {

    public enum Outcome {
        LEADER, COMPLETED, STILL_IN_FLIGHT
    }

    private final Outcome outcome;
    private final IdempotencyLease lease;
    private final String body;

    private IdempotencyPrepareResult(Outcome outcome, IdempotencyLease lease, String body) {
        this.outcome = outcome;
        this.lease = lease;
        this.body = body;
    }

    public static IdempotencyPrepareResult leader(IdempotencyLease lease) {
        return new IdempotencyPrepareResult(Outcome.LEADER, lease, null);
    }

    public static IdempotencyPrepareResult completed(String body) {
        return new IdempotencyPrepareResult(Outcome.COMPLETED, null, body);
    }

    public static IdempotencyPrepareResult stillInFlight() {
        return new IdempotencyPrepareResult(Outcome.STILL_IN_FLIGHT, null, null);
    }

    public Outcome getOutcome() {
        return outcome;
    }

    public IdempotencyLease getLease() {
        return lease;
    }

    public String getBody() {
        return body;
    }

    public boolean isLeader() {
        return outcome == Outcome.LEADER;
    }

    public boolean isCompleted() {
        return outcome == Outcome.COMPLETED;
    }

    public boolean isStillInFlight() {
        return outcome == Outcome.STILL_IN_FLIGHT;
    }
}
