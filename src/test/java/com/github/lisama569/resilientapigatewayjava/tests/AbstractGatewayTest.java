package com.github.lisama569.resilientapigatewayjava.tests;

import com.github.lisama569.resilientapigatewayjava.config.ResilienceRegistry;
import com.github.lisama569.resilientapigatewayjava.routing.IdempotencyStore;
import com.github.lisama569.resilientapigatewayjava.routing.RoundRobinLoadBalancer;
import com.github.lisama569.resilientapigatewayjava.upstream.MockUpstreamState;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

/** 测试基类：随机端口启动完整服务，每个用例前重置模拟上游/熔断/限流/幂等状态。 */
public abstract class AbstractGatewayTest {

    @LocalServerPort
    protected int port;

    @Autowired
    protected TestRestTemplate rest;

    @Autowired
    protected MockUpstreamState mockState;

    @Autowired
    protected ResilienceRegistry registry;

    @Autowired
    protected IdempotencyStore idempotencyStore;

    @Autowired
    protected RoundRobinLoadBalancer loadBalancer;

    @BeforeEach
    void resetState() {
        rest.getForEntity(admin("/reset"), String.class);
        registry.resetAll();
        idempotencyStore.reset();
        loadBalancer.reset();
    }

    protected String url(String path) {
        return "http://127.0.0.1:" + port + path;
    }

    protected String admin(String path) {
        return url("/__mock-admin" + path);
    }

    protected ResponseEntity<String> get(String path) {
        return print(rest.getForEntity(url(path), String.class), "GET " + path);
    }

    protected ResponseEntity<String> post(String path, String idempotencyKey) {
        HttpHeaders headers = new HttpHeaders();
        if (idempotencyKey != null) {
            headers.set("Idempotency-Key", idempotencyKey);
        }
        HttpEntity<Void> entity = new HttpEntity<>(headers);
        return print(rest.exchange(url(path), HttpMethod.POST, entity, String.class),
                "POST " + path + " key=" + idempotencyKey);
    }

    protected ResponseEntity<String> print(ResponseEntity<String> resp, String label) {
        System.out.println("---- " + label + " -> HTTP " + resp.getStatusCode().value());
        System.out.println("TRACE: " + resp.getHeaders().getFirst("X-Gateway-Trace"));
        System.out.println("RETRIES: " + resp.getHeaders().getFirst("X-Gateway-Retries"));
        System.out.println("BODY: " + resp.getBody());
        return resp;
    }
}
