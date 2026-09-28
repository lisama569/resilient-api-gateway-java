package com.github.lisama569.resilientapigatewayjava.idempotency;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 幂等键内部状态：处理中 / 已完成 / 失败。
 * 失败态只在 leader 唤醒等待者的瞬间可见，随后条目被移除，等待方重新竞争处理权。
 */
enum KeyPhase {
    IN_FLIGHT, COMPLETED, FAILED
}

/**
 * 进程内幂等存储：
 * 同一幂等键同时只有一个处理方（leader）真正调用上游；晚到请求（follower）在条目上等待并
 * 复用先到请求的结果，或在等待超时后得到“仍在处理”由调用方快速返回。
 * leader 失败（超时 / 连接失败 / 5xx / 限流 / 熔断拒绝等）会唤醒全部等待者竞争成为新
 * leader，保证高并发 + 重试 + 故障转移下落地上游的真实处理仍然只有一次。
 */
public class IdempotencyStore {

    private final ConcurrentMap<String, Entry> entries = new ConcurrentHashMap<>();

    /**
     * 单个幂等键的在途条目。所有等待 / 状态迁移都在条目 monitor 上完成。
     */
    static class Entry {
        private KeyPhase phase = KeyPhase.IN_FLIGHT;
        private String body;
        private final IdempotencyLease lease = new IdempotencyLease(this);
    }

    /** 若键已有完成结果则返回，否则返回 empty。 */
    public Optional<String> getIfPresent(String key) {
        Entry entry = entries.get(key);
        if (entry == null) {
            return Optional.empty();
        }
        synchronized (entry) {
            return entry.phase == KeyPhase.COMPLETED
                    ? Optional.ofNullable(entry.body)
                    : Optional.empty();
        }
    }

    /** 仅当键不存在时写入完成结果，返回是否真正写入。 */
    public boolean store(String key, String responseBody) {
        Entry entry = new Entry();
        entry.phase = KeyPhase.COMPLETED;
        entry.body = responseBody;
        return entries.putIfAbsent(key, entry) == null;
    }

    /**
     * 尝试登记处理权：
     * <ul>
     *   <li>键无在途记录 / 上一轮已失败：成为 leader，返回令牌；</li>
     *   <li>等待期间 leader 完成：返回完成结果；</li>
     *   <li>等待期间 leader 失败：自动重新竞争成为新 leader，绝不各自打上游；</li>
     *   <li>等待超时仍在处理：返回 STILL_IN_FLIGHT。</li>
     * </ul>
     */
    public IdempotencyPrepareResult prepare(String key, long waitMs) {
        long remainingNanos = Math.max(0L, waitMs) * 1_000_000L;
        while (true) {
            Entry existing = entries.get(key);
            if (existing == null) {
                Entry candidate = new Entry();
                Entry actual = entries.putIfAbsent(key, candidate);
                if (actual == null) {
                    return IdempotencyPrepareResult.leader(candidate.lease);
                }
                existing = actual;
            }
            synchronized (existing) {
                // 等待锁期间条目可能已被失败的 leader 移除并替换
                if (entries.get(key) != existing) {
                    continue;
                }
                switch (existing.phase) {
                    case COMPLETED -> {
                        return IdempotencyPrepareResult.completed(existing.body);
                    }
                    case FAILED -> {
                        // 上一轮失败：竞争替换为新在途条目；输掉则外层循环重读
                        Entry candidate = new Entry();
                        if (entries.replace(key, existing, candidate)) {
                            return IdempotencyPrepareResult.leader(candidate.lease);
                        }
                    }
                    case IN_FLIGHT -> {
                        long deadlineNanos = System.nanoTime() + remainingNanos;
                        while (existing.phase == KeyPhase.IN_FLIGHT && remainingNanos > 0L) {
                            try {
                                existing.wait(remainingNanos / 1_000_000L,
                                        (int) (remainingNanos % 1_000_000L));
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return IdempotencyPrepareResult.stillInFlight();
                            }
                            remainingNanos = deadlineNanos - System.nanoTime();
                        }
                        if (existing.phase == KeyPhase.COMPLETED) {
                            return IdempotencyPrepareResult.completed(existing.body);
                        }
                        if (existing.phase == KeyPhase.FAILED) {
                            // leader 失败：条目将被移除，重新竞争处理权
                            continue;
                        }
                        return IdempotencyPrepareResult.stillInFlight();
                    }
                }
            }
        }
    }

    /** leader 发布成功结果并唤醒全部等待者；过期令牌无效。 */
    public void complete(IdempotencyLease lease, String responseBody) {
        Entry entry = lease.entry;
        synchronized (entry) {
            if (entry.phase != KeyPhase.IN_FLIGHT) {
                return;
            }
            entry.phase = KeyPhase.COMPLETED;
            entry.body = responseBody;
            entry.notifyAll();
        }
    }

    /** leader 标记本次处理失败并唤醒等待者，等待方竞争重投；过期令牌无效。 */
    public void fail(IdempotencyLease lease) {
        Entry entry = lease.entry;
        synchronized (entry) {
            if (entry.phase != KeyPhase.IN_FLIGHT) {
                return;
            }
            entry.phase = KeyPhase.FAILED;
            entry.body = null;
            entry.notifyAll();
        }
        // 失败条目不保留：等待者醒来后经 putIfAbsent 竞争唯一的新 leader
        for (var keys = entries.entrySet().iterator(); keys.hasNext(); ) {
            var e = keys.next();
            if (e.getValue() == entry) {
                entries.remove(e.getKey(), entry);
                break;
            }
        }
    }

    /** 查询键当前状态（只读）：无记录 / 处理中 / 已完成。 */
    public Optional<IdempotencySnapshot> statusOf(String key) {
        Entry entry = entries.get(key);
        if (entry == null) {
            return Optional.empty();
        }
        synchronized (entry) {
            return switch (entry.phase) {
                case COMPLETED -> Optional.of(
                        new IdempotencySnapshot(IdempotencySnapshot.Status.COMPLETED, entry.body));
                default -> Optional.of(
                        new IdempotencySnapshot(IdempotencySnapshot.Status.IN_FLIGHT, null));
            };
        }
    }

    /** 已缓存的完成结果数量。 */
    public int size() {
        int count = 0;
        for (Entry entry : entries.values()) {
            synchronized (entry) {
                if (entry.phase == KeyPhase.COMPLETED) {
                    count++;
                }
            }
        }
        return count;
    }
}
