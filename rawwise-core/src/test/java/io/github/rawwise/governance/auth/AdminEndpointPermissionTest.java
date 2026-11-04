package io.github.rawwise.governance.auth;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.metastore.InMemoryMetastore;
import io.javalin.Javalin;
import java.io.IOException;
import java.util.Map;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("P2-5: Admin Endpoint Permission Hardening")
class AdminEndpointPermissionTest {

    private DefaultAuthMetastore authMetastore;
    private Javalin app;
    private OkHttpClient client = new OkHttpClient();
    private int port;
    private String superAdminToken;
    private String user1Token;

    @BeforeEach
    void setUp() throws IOException {
        InMemoryMetastore store = new InMemoryMetastore();
        authMetastore = new DefaultAuthMetastore(store);
        PermissionCache permissionCache = new PermissionCache(100, 30);
        TokenCache tokenCache = new TokenCache(100, 300);
        AuthAuditLogger auditLogger = new AuthAuditLogger(true);

        AuthFilter authFilter =
                new AuthFilter(authMetastore, permissionCache, tokenCache, null, auditLogger, true);

        Principal superAdmin = new Principal.UserPrincipal("superadmin", "Super Admin");
        superAdminToken = AuthCrypto.generateSecureToken();
        authMetastore.createToken(superAdminToken, superAdmin);
        authMetastore.setSuperAdmin(superAdmin);

        Principal regularUser = new Principal.UserPrincipal("user1", "Regular User");
        user1Token = AuthCrypto.generateSecureToken();
        authMetastore.createToken(user1Token, regularUser);

        app =
                Javalin.create()
                        .before(authFilter.authenticate)
                        .before(authFilter.authorize)
                        .get("/api/admin/test", ctx -> ctx.json(Map.of("ok", true)))
                        .get("/api/query/test", ctx -> ctx.json(Map.of("ok", true)));

        port = 0;
        app.start(0);
        port = app.port();
    }

    @AfterEach
    void tearDown() {
        if (app != null) app.stop();
    }

    @Test
    @DisplayName("Unauthenticated request to /api/admin/ returns 401")
    void unauthenticatedAdminReturns401() throws IOException {
        Request request =
                new Request.Builder().url("http://localhost:" + port + "/api/admin/test").build();
        try (Response response = client.newCall(request).execute()) {
            assertThat(response.code()).isEqualTo(401);
        }
    }

    @Test
    @DisplayName("Non-admin request to /api/admin/ returns 403")
    void nonAdminAdminReturns403() throws IOException {
        Request request =
                new Request.Builder()
                        .url("http://localhost:" + port + "/api/admin/test")
                        .header("Authorization", "Bearer " + user1Token)
                        .build();
        try (Response response = client.newCall(request).execute()) {
            assertThat(response.code()).isEqualTo(403);
        }
    }

    @Test
    @DisplayName("Super admin request to /api/admin/ returns 200")
    void superAdminAdminReturns200() throws IOException {
        Request request =
                new Request.Builder()
                        .url("http://localhost:" + port + "/api/admin/test")
                        .header("Authorization", "Bearer " + superAdminToken)
                        .build();
        try (Response response = client.newCall(request).execute()) {
            assertThat(response.code()).isEqualTo(200);
        }
    }
}
