package com.github.lisama569.resilientapigatewayjava.upstream;

import com.github.lisama569.resilientapigatewayjava.error.FailureKind;
import com.github.lisama569.resilientapigatewayjava.error.UpstreamFailureException;
import com.github.lisama569.resilientapigatewayjava.model.UpstreamInstance;
import org.springframework.stereotype.Component;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.net.http.HttpTimeoutException;

/**
 * 基于 JDK {@link HttpClient} 的上游调用实现。
 * <p>
 * 把底层失败在“调用边界”归一化为 {@link UpstreamFailureException}：
 * <ul>
 *   <li>连接被拒绝/无路由 -> {@link FailureKind#UPSTREAM_CONNECT}；</li>
 *   <li>响应超时 -> {@link FailureKind#UPSTREAM_TIMEOUT}；</li>
 *   <li>有 HTTP 响应但状态码异常 -> {@link FailureKind#UPSTREAM_BUSINESS}
 *       （保留上游原始状态码）。</li>
 * </ul>
 * 2xx/3xx 视为成功。幂等键以 Idempotency-Key 头透传给上游，
 * 由上游保证同一键只产生一次副作用。
 */
@Component
public class HttpUpstreamClient implements UpstreamClient {

    private final MockAddressResolver resolver;
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(500))
            .build();

    public HttpUpstreamClient(MockAddressResolver resolver) {
        this.resolver = resolver;
    }

    @Override
    public UpstreamResult call(UpstreamInstance instance, String method, String path,
                               String idempotencyKey, long timeoutMillis) {
        String base = resolver.resolve(instance.getBaseUrl());
        String normalizedPath = path.startsWith("/") ? path : "/" + path;
        URI uri = URI.create(base + normalizedPath);

        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofMillis(timeoutMillis))
                .header("Idempotency-Key",
                        idempotencyKey == null ? "" : idempotencyKey);
        builder.method(method, HttpRequest.BodyPublishers.noBody());

        HttpResponse<String> response;
        try {
            response = httpClient.send(builder.build(),
                    HttpResponse.BodyHandlers.ofString());
        } catch (HttpTimeoutException | SocketTimeoutException e) {
            throw new UpstreamFailureException(FailureKind.UPSTREAM_TIMEOUT,
                    instance.getId(), 0,
                    "upstream timeout after " + timeoutMillis + "ms: " + e.getMessage());
        } catch (ConnectException e) {
            throw new UpstreamFailureException(FailureKind.UPSTREAM_CONNECT,
                    instance.getId(), 0,
                    "upstream connect failed to " + uri + ": "
                            + describe(e));
        } catch (Exception e) {
            Throwable cause = e.getCause();
            if (cause instanceof ConnectException) {
                throw new UpstreamFailureException(FailureKind.UPSTREAM_CONNECT,
                        instance.getId(), 0,
                        "upstream connect failed to " + uri + ": "
                                + describe(cause));
            }
            if (cause instanceof HttpTimeoutException || cause instanceof SocketTimeoutException) {
                throw new UpstreamFailureException(FailureKind.UPSTREAM_TIMEOUT,
                        instance.getId(), 0,
                        "upstream timeout: " + describe(cause));
            }
            throw new UpstreamFailureException(FailureKind.UPSTREAM_CONNECT,
                    instance.getId(), 0,
                    "upstream call failed: " + describe(e));
        }

        boolean duplicate = "true".equalsIgnoreCase(
                response.headers().firstValue("X-Upstream-Duplicate").orElse("false"));
        return new UpstreamResult(response.statusCode(), response.body(), duplicate);
    }

    private static String describe(Throwable t) {
        String msg = t.getMessage();
        return (msg == null || msg.isBlank())
                ? t.getClass().getSimpleName()
                : t.getClass().getSimpleName() + ": " + msg;
    }
}
