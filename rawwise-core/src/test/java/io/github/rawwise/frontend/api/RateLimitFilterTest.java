package io.github.rawwise.frontend.api;

import static org.assertj.core.api.Assertions.assertThat;

import io.javalin.Javalin;
import io.javalin.testtools.JavalinTest;
import org.junit.jupiter.api.Test;

class RateLimitFilterTest {

    @Test
    void allowsRequestUnderLimit() {
        RateLimitFilter filter = new RateLimitFilter(100, 100);
        Javalin app = Javalin.create().before(filter).get("/api/test", ctx -> ctx.result("ok"));

        JavalinTest.test(
                app,
                (server, client) -> {
                    var response = client.get("/api/test");
                    assertThat(response.code()).isEqualTo(200);
                });
    }

    @Test
    void rejectsRequestWhenGlobalLimitExceeded() {
        RateLimitFilter filter = new RateLimitFilter(1, 100);
        Javalin app = Javalin.create().before(filter).get("/api/test", ctx -> ctx.result("ok"));

        JavalinTest.test(
                app,
                (server, client) -> {
                    client.get("/api/test");
                    var response = client.get("/api/test");
                    assertThat(response.code()).isEqualTo(429);
                    String body = response.body().string();
                    assertThat(body).contains("global");
                });
    }

    @Test
    void rejectsRequestWhenPerIpLimitExceeded() {
        RateLimitFilter filter = new RateLimitFilter(1000, 1);
        Javalin app = Javalin.create().before(filter).get("/api/test", ctx -> ctx.result("ok"));

        JavalinTest.test(
                app,
                (server, client) -> {
                    client.get("/api/test");
                    var response = client.get("/api/test");
                    assertThat(response.code()).isEqualTo(429);
                    String body = response.body().string();
                    assertThat(body).contains("per-ip");
                });
    }

    @Test
    void skipsAdminPaths() {
        RateLimitFilter filter = new RateLimitFilter(1, 1);
        Javalin app =
                Javalin.create()
                        .before(filter)
                        .get("/api/admin/security-status", ctx -> ctx.result("ok"))
                        .get("/api/test", ctx -> ctx.result("ok"));

        JavalinTest.test(
                app,
                (server, client) -> {
                    var adminResponse = client.get("/api/admin/security-status");
                    assertThat(adminResponse.code()).isEqualTo(200);
                });
    }

    @Test
    void returnsJsonErrorResponse() {
        RateLimitFilter filter = new RateLimitFilter(1, 100);
        Javalin app = Javalin.create().before(filter).get("/api/test", ctx -> ctx.result("ok"));

        JavalinTest.test(
                app,
                (server, client) -> {
                    client.get("/api/test");
                    var response = client.get("/api/test");
                    assertThat(response.code()).isEqualTo(429);
                    String body = response.body().string();
                    assertThat(body).contains("error");
                    assertThat(body).contains("Too Many Requests");
                });
    }
}
