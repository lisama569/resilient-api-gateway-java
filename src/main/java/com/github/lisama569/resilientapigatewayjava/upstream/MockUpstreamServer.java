package com.github.lisama569.resilientapigatewayjava.upstream;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * 基于 JDK HttpServer 的模拟上游，可在本地启动。
 * 支持可控延迟、失败模式；进程内维护幂等表与副作用计数，
 * 同一幂等键重复投递不会产生重复副作用。
 */
public class MockUpstreamServer {

    private final String id;
    private final int requestedPort;
    private final AtomicInteger sideEffectCount = new AtomicInteger();
    private final AtomicInteger totalRequestCount = new AtomicInteger();
    private final Map<String, String> idempotencyTable = new ConcurrentHashMap<>();

    private volatile long latencyMs = 0;
    private volatile FailureMode failureMode = FailureMode.NONE;
    private volatile Supplier<FailureMode> failureModeSupplier;
    private volatile boolean running = false;
    private HttpServer server;

    public MockUpstreamServer(String id, int port) {
        this.id = id;
        this.requestedPort = port;
    }

    public void start() {
        try {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", requestedPort), 0);
        } catch (IOException e) {
            throw new IllegalStateException("mock upstream " + id + " start failed", e);
        }
        server.setExecutor(Executors.newCachedThreadPool());
        server.createContext("/work", this::handleWork);
        server.createContext("/health", exchange -> respond(exchange, 200, "OK"));
        server.start();
        running = true;
    }

    public void stop() {
        running = false;
        if (server != null) {
            server.stop(0);
        }
    }

    public int getPort() {
        return server == null ? requestedPort : server.getAddress().getPort();
    }

    public String getId() {
        return id;
    }

    public void setLatencyMs(long latencyMs) {
        this.latencyMs = latencyMs;
    }

    public void setFailureMode(FailureMode mode) {
        this.failureModeSupplier = null;
        this.failureMode = mode;
    }

    /** 设置动态失败模式：每次请求实时取模式（如“前 N 次失败后恢复”）。 */
    public void setFailureMode(Supplier<FailureMode> supplier) {
        this.failureModeSupplier = supplier;
    }

    private FailureMode currentFailureMode() {
        Supplier<FailureMode> supplier = failureModeSupplier;
        return supplier != null ? supplier.get() : failureMode;
    }

    /** 已发生的真实副作用次数（按幂等键去重后）。 */
    public int getSideEffectCount() {
        return sideEffectCount.get();
    }

    /** 收到的请求总数（含重复投递）。 */
    public int getTotalRequestCount() {
        return totalRequestCount.get();
    }

    private void handleWork(com.sun.net.httpserver.HttpExchange exchange) throws IOException {
        totalRequestCount.incrementAndGet();
        try {
            switch (currentFailureMode()) {
                case RESET -> {
                    // 不发送任何响应头直接关闭，模拟连接被重置
                    exchange.close();
                    return;
                }
                case HANG -> {
                    // 永不响应，直到服务停止，触发客户端超时
                    while (running) {
                        Thread.sleep(50);
                    }
                    exchange.close();
                    return;
                }
                case ALWAYS_500 -> {
                    respond(exchange, 500, "upstream business error from " + id);
                    return;
                }
                case ALWAYS_400 -> {
                    // 上游因调用方自身原因拒绝（参数不合法 / 业务规则不允许）
                    respond(exchange, 400, "client request rejected by " + id
                            + ": invalid argument or business rule violation");
                    return;
                }
                case NONE -> {
                    // fall through
                }
            }
            sleepQuietly(latencyMs);
            String idempotencyKey = exchange.getRequestHeaders().getFirst("Idempotency-Key");
            if (idempotencyKey != null) {
                String existing = idempotencyTable.get(idempotencyKey);
                if (existing != null) {
                    // 重复投递：返回首个结果，不产生新副作用
                    respond(exchange, 200, existing);
                    return;
                }
            }
            int seq = sideEffectCount.incrementAndGet();
            String body = "upstream=" + id + " sideEffect=" + seq;
            if (idempotencyKey != null) {
                idempotencyTable.putIfAbsent(idempotencyKey, body);
            }
            respond(exchange, 200, body);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            exchange.close();
        }
    }

    private void respond(com.sun.net.httpserver.HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private void sleepQuietly(long ms) {
        if (ms <= 0) {
            return;
        }
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public enum FailureMode {
        NONE,           // 正常
        ALWAYS_500,     // 上游内部错误（5xx）
        ALWAYS_400,     // 调用方原因被拒（4xx）
        HANG,           // 永不响应（触发超时）
        RESET           // 连接被重置（IO 失败）
    }
}
