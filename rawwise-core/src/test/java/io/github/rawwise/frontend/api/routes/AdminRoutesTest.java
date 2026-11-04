package io.github.rawwise.frontend.api.routes;

import static org.junit.jupiter.api.Assertions.*;

import io.javalin.Javalin;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AdminRoutesTest {

    private Javalin app;
    private HttpClient client;
    private int port;

    @BeforeEach
    void setUp() {
        app = Javalin.create();
        AdminRoutes.register(app);
        app.start(0);
        port = app.port();
        client = HttpClient.newHttpClient();
    }

    @AfterEach
    void tearDown() {
        app.stop();
    }

    @Test
    void configEndpointReturnsRawwiseProperties() throws Exception {
        var response =
                client.send(
                        HttpRequest.newBuilder(
                                        URI.create(
                                                "http://localhost:" + port + "/api/admin/config"))
                                .GET()
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("rawwise."));
    }

    @Test
    void threadsEndpointReturnsThreadInfo() throws Exception {
        var response =
                client.send(
                        HttpRequest.newBuilder(
                                        URI.create(
                                                "http://localhost:" + port + "/api/admin/threads"))
                                .GET()
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("threadCount"));
    }

    @Test
    void healthDetailEndpointReturnsHeapInfo() throws Exception {
        var response =
                client.send(
                        HttpRequest.newBuilder(
                                        URI.create(
                                                "http://localhost:"
                                                        + port
                                                        + "/api/admin/health/detail"))
                                .GET()
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("heap"));
        assertTrue(response.body().contains("processors"));
    }

    @Test
    void cacheEndpointReturnsMessage() throws Exception {
        var response =
                client.send(
                        HttpRequest.newBuilder(
                                        URI.create("http://localhost:" + port + "/api/admin/cache"))
                                .GET()
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
    }

    @Test
    void sessionsEndpointReturnsMessage() throws Exception {
        var response =
                client.send(
                        HttpRequest.newBuilder(
                                        URI.create(
                                                "http://localhost:" + port + "/api/admin/sessions"))
                                .GET()
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
    }

    @Test
    void configDesensitizesSensitiveKeys() throws Exception {
        System.setProperty("rawwise.test.password", "secret123");
        System.setProperty("rawwise.test.token", "tok123");
        System.setProperty("rawwise.test.key", "key123");
        System.setProperty("rawwise.test.normal", "visible");

        try {
            var response =
                    client.send(
                            HttpRequest.newBuilder(
                                            URI.create(
                                                    "http://localhost:"
                                                            + port
                                                            + "/api/admin/config"))
                                    .GET()
                                    .build(),
                            HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            assertTrue(response.body().contains("********"));
            assertTrue(response.body().contains("visible"));
            assertFalse(response.body().contains("secret123"));
            assertFalse(response.body().contains("tok123"));
            assertFalse(response.body().contains("key123"));
        } finally {
            System.clearProperty("rawwise.test.password");
            System.clearProperty("rawwise.test.token");
            System.clearProperty("rawwise.test.key");
            System.clearProperty("rawwise.test.normal");
        }
    }
}
