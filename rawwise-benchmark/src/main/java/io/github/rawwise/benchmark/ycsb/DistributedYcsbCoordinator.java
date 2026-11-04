package io.github.rawwise.benchmark.ycsb;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class DistributedYcsbCoordinator {

    private final String frontendHost;
    private final int frontendPort;
    private final int signalTimeoutSeconds;
    private final HttpClient httpClient;

    public DistributedYcsbCoordinator(
            String frontendHost, int frontendPort, int signalTimeoutSeconds) {
        this.frontendHost = frontendHost;
        this.frontendPort = frontendPort;
        this.signalTimeoutSeconds = signalTimeoutSeconds;
        this.httpClient = HttpClient.newHttpClient();
    }

    public boolean waitForReady() {
        long deadline = System.currentTimeMillis() + signalTimeoutSeconds * 1000L;
        while (System.currentTimeMillis() < deadline) {
            try {
                var request =
                        HttpRequest.newBuilder()
                                .uri(
                                        URI.create(
                                                "http://"
                                                        + frontendHost
                                                        + ":"
                                                        + frontendPort
                                                        + "/ready"))
                                .timeout(Duration.ofSeconds(5))
                                .GET()
                                .build();
                var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    System.out.println("  Signal received: Frontend is ready");
                    return true;
                }
            } catch (IOException | InterruptedException e) {
                // Frontend not ready yet, retry after sleep
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        System.out.println(
                "  Signal timeout after " + signalTimeoutSeconds + "s, proceeding anyway");
        return false;
    }

    public String getFrontendHost() {
        return frontendHost;
    }

    public int getFrontendPort() {
        return frontendPort;
    }
}
