package com.github.lisama569.resilientapigatewayjava.gateway;

import com.github.lisama569.resilientapigatewayjava.gateway.error.UpstreamResult;
import com.github.lisama569.resilientapigatewayjava.upstream.UpstreamInstance;

import java.io.IOException;
import java.net.ConnectException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.net.URI;
import java.time.Duration;

/**
 * 上游 HTTP 调用客户端：单次尝试带独立超时，
 * 将调用方原因被拒（4xx）、上游内部故障（5xx）、超时、连接失败
 * 归一化为相互可区分的 UpstreamResult。
 */
public class UpstreamClient {

    private final HttpClient httpClient;

    public UpstreamClient() {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build();
    }

    /**
     * 调用上游 /work 端点。
     *
     * @param upstream       目标实例
     * @param path           请求路径（透传）
     * @param idempotencyKey 幂等键（同一业务请求的重试/故障转移共用）
     * @param timeoutMs      本次尝试的超时预算
     */
    public UpstreamResult call(UpstreamInstance upstream, String path, String idempotencyKey, long timeoutMs) {
        long start = System.nanoTime();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(upstream.baseUrl() + path))
                .timeout(Duration.ofMillis(Math.max(1, timeoutMs)))
                .header("Idempotency-Key", idempotencyKey)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            long latency = elapsedMs(start);
            int status = response.statusCode();
            if (status >= 500) {
                // 上游自身内部故障：可重试 / 可故障转移，计入熔断失败
                return UpstreamResult.businessError(status, response.body(), latency);
            }
            if (status >= 400) {
                // 上游在受理前因调用方原因拒绝：不重试、不故障转移、不计入熔断失败
                return UpstreamResult.rejected(status, response.body(), latency);
            }
            return UpstreamResult.success(status, response.body(), latency);
        } catch (HttpTimeoutException e) {
            // 含连接超时（HttpConnectTimeoutException 是其子类）
            return UpstreamResult.timeout(elapsedMs(start));
        } catch (ConnectException e) {
            return UpstreamResult.connectFailure(elapsedMs(start));
        } catch (IOException e) {
            // 连接被重置等 IO 失败，归为连接失败
            return UpstreamResult.connectFailure(elapsedMs(start));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return UpstreamResult.timeout(elapsedMs(start));
        }
    }

    private long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }
}
