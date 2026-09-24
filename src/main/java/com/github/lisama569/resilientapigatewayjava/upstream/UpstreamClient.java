package com.github.lisama569.resilientapigatewayjava.upstream;

import com.github.lisama569.resilientapigatewayjava.model.UpstreamInstance;

/** 上游 HTTP 调用抽象，便于测试替换。 */
public interface UpstreamClient {

    UpstreamResult call(UpstreamInstance instance, String method, String path,
                        String idempotencyKey, long timeoutMillis);
}
