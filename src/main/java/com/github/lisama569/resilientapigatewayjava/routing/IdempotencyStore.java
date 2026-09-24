package com.github.lisama569.resilientapigatewayjava.routing;

import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 进程内幂等登记表（以幂等键标识一次业务请求）。
 * <p>
 * 同一次业务请求在重试 / 故障转移中会被多次投递。这里保证：
 * <ol>
 *   <li>只有第一个进入的投递能返回 true（真正向下游发请求）；</li>
 *   <li>首个投递执行期间，并发重复投递以 INFLIGHT 短路，避免并发双写；</li>
 *   <li>首个投递完成后，重复投递直接拿到缓存结果，不再访问任何上游。</li>
 * </ol>
 * 网关入口去重 + 上游按幂等键去重构成双保险，即使跨实例故障转移也不会
 * 放大副作用（模拟上游共享同一业务键空间，复刻真实带幂等表的上游集群）。
 */
@Component
public class IdempotencyStore {

    private record Entry(boolean inflight, int status, String body) {
    }

    private final ConcurrentHashMap<String, Entry> table = new ConcurrentHashMap<>();

    /** 首次登记：true 表示获得执行权；false 表示已有同键请求，调用方应查 lookup。 */
    public boolean firstAttempt(String key) {
        return table.putIfAbsent(key, new Entry(true, 0, null)) == null;
    }

    /** 登记完成结果，供后续重复投递直接复用。 */
    public void complete(String key, int status, String body) {
        table.put(key, new Entry(false, status, body));
    }

    public Optional<CachedOutcome> lookup(String key) {
        Entry e = table.get(key);
        if (e == null) {
            return Optional.empty();
        }
        return Optional.of(new CachedOutcome(e.status(), e.body(), e.inflight()));
    }

    /** 清空登记表（主要供测试隔离使用）。 */
    public void reset() {
        table.clear();
    }

    public record CachedOutcome(int status, String body, boolean inflight) {
    }
}
