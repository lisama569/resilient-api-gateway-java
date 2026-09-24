package com.github.lisama569.resilientapigatewayjava.idempotency;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 进程内幂等存储：同一幂等键的首个成功响应被缓存，
 * 重复投递（客户端重试 / 网关故障转移重放）直接返回缓存结果，避免重复副作用。
 */
public class IdempotencyStore {

    private final ConcurrentMap<String, String> store = new ConcurrentHashMap<>();

    /** 若键已有成功结果则返回，否则返回 empty。 */
    public Optional<String> getIfPresent(String key) {
        return Optional.ofNullable(store.get(key));
    }

    /** 仅当键不存在时写入，返回是否真正写入。 */
    public boolean store(String key, String responseBody) {
        return store.putIfAbsent(key, responseBody) == null;
    }

    public int size() {
        return store.size();
    }
}
