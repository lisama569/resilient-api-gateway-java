package com.github.lisama569.resilientapigatewayjava.idempotency;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 进程内幂等存储（单飞表）。
 *
 * <p>同一幂等键在任意时刻只有一个处理者（owner）：
 * <ul>
 *   <li>{@link #acquire(String)} 原子占位，先到者成为 owner，晚到者拿到 PROCESSING 占用；</li>
 *   <li>owner 处理成功后 {@link #complete(String, String)}，所有等待者/后续投递直接拿到同一结果；</li>
 *   <li>owner 失败（超时、限流、熔断等可重试或不确定的失败）时 {@link #fail(Entry)} 移除占用，
 *       后续同键请求可重新发起（语义为“尚未确定处理完成”，允许安全重试）。</li>
 * </ul>
 *
 * <p>并发投递、故障转移、多轮重试下真实处理只发生一次（上游侧另维护幂等表兜底）。
 */
public class IdempotencyStore {

    /** 占位结果。 */
    public static final class Entry {
        private final CompletableFuture<String> future = new CompletableFuture<>();

        /** 阻塞等待处理结果。 */
        public AwaitOutcome await(long timeoutMs) throws InterruptedException {
            try {
                return AwaitOutcome.success(future.get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS));
            } catch (java.util.concurrent.TimeoutException e) {
                return AwaitOutcome.timeout();
            } catch (java.util.concurrent.ExecutionException e) {
                return AwaitOutcome.failed();
            }
        }
    }

    /** 等待 owner 结果的结局。 */
    public static final class AwaitOutcome {
        public enum Kind { SUCCESS, TIMEOUT, FAILED }

        private final Kind kind;
        private final String body;

        private AwaitOutcome(Kind kind, String body) {
            this.kind = kind;
            this.body = body;
        }

        static AwaitOutcome success(String body) {
            return new AwaitOutcome(Kind.SUCCESS, body);
        }

        static AwaitOutcome timeout() {
            return new AwaitOutcome(Kind.TIMEOUT, null);
        }

        static AwaitOutcome failed() {
            return new AwaitOutcome(Kind.FAILED, null);
        }

        public Kind getKind() {
            return kind;
        }

        public String getBody() {
            return body;
        }
    }

    /** 查询结果状态。 */
    public enum QueryStatus {
        COMPLETED,   // 已处理完成，携带结果
        PROCESSING,  // 正在处理，暂时没有结果
        NOT_FOUND    // 该键从未出现（或此前失败已清理）
    }

    /** 查询结果。 */
    public static final class QueryResult {
        private final QueryStatus status;
        private final String body;

        private QueryResult(QueryStatus status, String body) {
            this.status = status;
            this.body = body;
        }

        public static QueryResult completed(String body) {
            return new QueryResult(QueryStatus.COMPLETED, body);
        }

        public static QueryResult processing() {
            return new QueryResult(QueryStatus.PROCESSING, null);
        }

        public static QueryResult notFound() {
            return new QueryResult(QueryStatus.NOT_FOUND, null);
        }

        public QueryStatus getStatus() {
            return status;
        }

        public String getBody() {
            return body;
        }
    }

    private final ConcurrentMap<String, Entry> inflight = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String> completed = new ConcurrentHashMap<>();

    /**
     * 原子占位：
     * <ul>
     *   <li>键已有完成结果 -> 返回完成结果；</li>
     *   <li>键正在处理 -> 返回在途 Entry（调用方等待）；</li>
     *   <li>键不存在 -> 新建占位并返回，调用方成为 owner（{@code acquired=true}）。</li>
     * </ul>
     */
    public AcquireOutcome acquire(String key) {
        String done = completed.get(key);
        if (done != null) {
            return AcquireOutcome.completed(done);
        }
        Entry fresh = new Entry();
        while (true) {
            String nowDone = completed.get(key);
            if (nowDone != null) {
                return AcquireOutcome.completed(nowDone);
            }
            Entry existing = inflight.putIfAbsent(key, fresh);
            if (existing == null) {
                return AcquireOutcome.acquired(fresh);
            }
            // 已被其他线程占位：可能恰好在这一刻完成，复查完成表
            String maybeDone = completed.get(key);
            if (maybeDone != null) {
                return AcquireOutcome.completed(maybeDone);
            }
            Entry inflightEntry = inflight.get(key);
            if (inflightEntry != null) {
                return AcquireOutcome.processing(inflightEntry);
            }
            // 占位刚好被失败移除：自旋重新占位
        }
    }

    /** owner 成功：发布结果并清理在途占位。仅首个 owner 的写入生效。 */
    public void complete(String key, String responseBody) {
        completed.putIfAbsent(key, responseBody);
        Entry entry = inflight.remove(key);
        if (entry != null) {
            entry.future.complete(responseBody);
        }
    }

    /** owner 失败：移除在途占位并唤醒等待者（它们将收到 empty，按快速失败处理）。 */
    public void fail(String key, Entry entry) {
        if (inflight.remove(key, entry)) {
            entry.future.completeExceptionally(new IllegalStateException("processing failed: " + key));
        }
    }

    /** 只读查询某键的处理状态，绝不触发任何上游调用。 */
    public QueryResult query(String key) {
        String done = completed.get(key);
        if (done != null) {
            return QueryResult.completed(done);
        }
        if (inflight.containsKey(key)) {
            return QueryResult.processing();
        }
        // 完成与在途之间存在极小窗口，以完成表为准复查一次
        String recheck = completed.get(key);
        if (recheck != null) {
            return QueryResult.completed(recheck);
        }
        return QueryResult.notFound();
    }

    /** 兼容旧用法：完成结果命中。 */
    public Optional<String> getIfPresent(String key) {
        return Optional.ofNullable(completed.get(key));
    }

    public int size() {
        return completed.size();
    }

    /** acquire 的返回。 */
    public static final class AcquireOutcome {
        private final boolean acquired;
        private final boolean completedHit;
        private final Entry entry;
        private final String body;

        private AcquireOutcome(boolean acquired, boolean completedHit, Entry entry, String body) {
            this.acquired = acquired;
            this.completedHit = completedHit;
            this.entry = entry;
            this.body = body;
        }

        static AcquireOutcome acquired(Entry entry) {
            return new AcquireOutcome(true, false, entry, null);
        }

        static AcquireOutcome processing(Entry entry) {
            return new AcquireOutcome(false, false, entry, null);
        }

        static AcquireOutcome completed(String body) {
            return new AcquireOutcome(false, true, null, body);
        }

        /** 是否成功占位成为 owner。 */
        public boolean isAcquired() {
            return acquired;
        }

        /** 是否直接命中已完成结果。 */
        public boolean isCompletedHit() {
            return completedHit;
        }

        /** 在途占位（等待 owner 结果用），仅 PROCESSING 时非 null。 */
        public Entry getEntry() {
            return entry;
        }

        public String getBody() {
            return body;
        }
    }
}
