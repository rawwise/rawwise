package io.github.rawwise.governance.auth;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.metastore.InMemoryMetastore;
import io.javalin.Javalin;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("RBAC E2E - permission full-chain business scenario test")
class RbacE2ETest {

    private DefaultAuthMetastore authMetastore;

    @BeforeEach
    void setUp() throws IOException {
        InMemoryMetastore metastore = new InMemoryMetastore();
        authMetastore = new DefaultAuthMetastore(metastore);
    }

    private String origin(Javalin server) {
        return "http://localhost:" + server.port();
    }

    private Javalin createApp(
            PermissionCache permCache,
            TokenCache tokenCache,
            AuthAuditLogger auditLogger,
            boolean unauthenticatedDeny) {
        var filter =
                new AuthFilter(
                        authMetastore,
                        permCache,
                        tokenCache,
                        null,
                        auditLogger,
                        unauthenticatedDeny);
        var routes = new AuthRoutes(authMetastore, permCache, tokenCache, auditLogger);

        return Javalin.create()
                .before(filter.authenticate)
                .before(filter.authorize)
                .get("/api/tables/{table}", ctx -> ctx.json(Map.of("name", ctx.pathParam("table"))))
                .post(
                        "/api/query",
                        ctx -> ctx.json(Map.of("jobId", "test-123", "state", "SUCCEEDED")))
                .post("/api/v1/auth/roles", routes.createRole)
                .delete("/api/v1/auth/roles/{roleId}", routes.deleteRole)
                .get("/api/v1/auth/roles", routes.listRoles)
                .get("/api/v1/auth/roles/{roleId}", routes.getRole)
                .post("/api/v1/auth/grant", routes.grantRole)
                .post("/api/v1/auth/revoke", routes.revokeRole)
                .get("/api/v1/auth/permissions", routes.getPermissions)
                .post("/api/v1/auth/tokens", routes.createToken)
                .post("/api/v1/auth/agents", routes.createAgent);
    }

    private Javalin createAppNoRoutes(
            PermissionCache permCache,
            TokenCache tokenCache,
            AuthAuditLogger auditLogger,
            boolean unauthenticatedDeny) {
        var filter =
                new AuthFilter(
                        authMetastore,
                        permCache,
                        tokenCache,
                        null,
                        auditLogger,
                        unauthenticatedDeny);
        return Javalin.create()
                .before(filter.authenticate)
                .before(filter.authorize)
                .get(
                        "/api/tables/{table}",
                        ctx -> ctx.json(Map.of("name", ctx.pathParam("table"))));
    }

    private Request.Builder bearerGet(String origin, String path, String token) {
        return new Request.Builder().url(origin + path).header("Authorization", "Bearer " + token);
    }

    private Request.Builder bearerPost(String origin, String path, String token, String json) {
        return new Request.Builder()
                .url(origin + path)
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "application/json")
                .post(RequestBody.create(json, MediaType.parse("application/json")));
    }

    private Request.Builder noAuthGet(String origin, String path) {
        return new Request.Builder().url(origin + path);
    }

    private void bootstrapSuperAdmin() throws IOException {
        Principal superAdmin = new Principal.UserPrincipal("superadmin", "Super Admin");
        authMetastore.createToken("nbl-super-token", superAdmin);
        authMetastore.setSuperAdmin(superAdmin);
    }

    private void createOrderReaderRole() throws IOException {
        var readPerm =
                new Permission.TablePermission("db.orders", Set.of(Permission.TableOp.SELECT));
        authMetastore.createRole(
                new Role(
                        "order-reader",
                        "Read orders",
                        Set.of(readPerm),
                        System.currentTimeMillis(),
                        System.currentTimeMillis()));
    }

    private void createOrderWriterRole() throws IOException {
        var writePerm =
                new Permission.TablePermission(
                        "db.orders", Set.of(Permission.TableOp.INSERT, Permission.TableOp.UPDATE));
        authMetastore.createRole(
                new Role(
                        "order-writer",
                        "Write orders",
                        Set.of(writePerm),
                        System.currentTimeMillis(),
                        System.currentTimeMillis()));
    }

    @Nested
    @DisplayName("S1: Happy Path")
    class HappyPath {

        @Test
        @DisplayName(
                "RBAClifecycle: createToken->createrole->authorization->access table->verification passed")
        void testFullRbacLifecycle() throws IOException {
            var permCache = new PermissionCache(100, 30);
            var tokenCache = new TokenCache(300);
            var auditLogger = new AuthAuditLogger(true);

            Principal user = new Principal.UserPrincipal("user-001", "Zhang San");
            authMetastore.createToken("nbl-user-token", user);
            createOrderReaderRole();
            authMetastore.grantRole(user, "order-reader", "superadmin");

            Javalin app = createApp(permCache, tokenCache, auditLogger, true);

            io.javalin.testtools.JavalinTest.test(
                    app,
                    (server, client) -> {
                        var response =
                                client.request(
                                        bearerGet(
                                                        origin(server),
                                                        "/api/tables/db.orders",
                                                        "nbl-user-token")
                                                .build());
                        assertThat(response.code()).isEqualTo(200);
                    });
        }

        @Test
        @DisplayName(
                "Agentdelegation scenario: registerAgent->authorized role->Agentaccess table->verification passed")
        void testAgentDelegationLifecycle() throws IOException {
            var permCache = new PermissionCache(100, 30);
            var tokenCache = new TokenCache(300);
            var auditLogger = new AuthAuditLogger(true);

            bootstrapSuperAdmin();
            createOrderReaderRole();

            Principal agent =
                    new Principal.AgentPrincipal("order-agent-001", "user-zhangsan", "superadmin");
            authMetastore.createToken("nbl-agent-token", agent);
            authMetastore.grantRole(agent, "order-reader", "superadmin");

            Javalin app = createApp(permCache, tokenCache, auditLogger, true);

            io.javalin.testtools.JavalinTest.test(
                    app,
                    (server, client) -> {
                        var agentResp =
                                client.request(
                                        bearerGet(
                                                        origin(server),
                                                        "/api/tables/db.orders",
                                                        "nbl-agent-token")
                                                .build());
                        assertThat(agentResp.code()).isEqualTo(200);
                    });
        }

        @Test
        @DisplayName("role afterpermission : authorization-> via-> ->cache -> denied")
        void testRevokeRoleDeniesAccess() throws IOException {
            var permCache = new PermissionCache(100, 30);
            var tokenCache = new TokenCache(300);
            var auditLogger = new AuthAuditLogger(true);

            Principal user = new Principal.UserPrincipal("user-001", "Zhang San");
            authMetastore.createToken("nbl-user-token", user);
            createOrderReaderRole();
            authMetastore.grantRole(user, "order-reader", "superadmin");

            Javalin app = createApp(permCache, tokenCache, auditLogger, true);

            io.javalin.testtools.JavalinTest.test(
                    app,
                    (server, client) -> {
                        var beforeRevoke =
                                client.request(
                                        bearerGet(
                                                        origin(server),
                                                        "/api/tables/db.orders",
                                                        "nbl-user-token")
                                                .build());
                        assertThat(beforeRevoke.code()).isEqualTo(200);

                        try {
                            authMetastore.revokeRole(user, "order-reader");
                            permCache.invalidate("user-001");
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        var afterRevoke =
                                client.request(
                                        bearerGet(
                                                        origin(server),
                                                        "/api/tables/db.orders",
                                                        "nbl-user-token")
                                                .build());
                        assertThat(afterRevoke.code()).isEqualTo(403);
                    });
        }

        @Test
        @DisplayName("multiroleaggregatepermission: readrole+writerole->verifyreadwrite via")
        void testMultipleRolesAggregate() throws IOException {
            Principal user = new Principal.UserPrincipal("user-001", "Zhang San");
            authMetastore.createToken("nbl-user-token", user);

            createOrderReaderRole();
            createOrderWriterRole();
            authMetastore.grantRole(user, "order-reader", "superadmin");
            authMetastore.grantRole(user, "order-writer", "superadmin");

            ResolvedPermissions perms = authMetastore.resolvePermissions("user-001");
            assertThat(perms.hasPermission("db.orders", Permission.TableOp.SELECT)).isTrue();
            assertThat(perms.hasPermission("db.orders", Permission.TableOp.INSERT)).isTrue();
            assertThat(perms.hasPermission("db.orders", Permission.TableOp.UPDATE)).isTrue();
        }

        @Test
        @DisplayName("AuthRoutes API: createrole->list roles->deleterole")
        void testRoleManagementViaRoutes() throws IOException {
            var permCache = new PermissionCache(100, 30);
            var tokenCache = new TokenCache(300);
            var auditLogger = new AuthAuditLogger(true);
            bootstrapSuperAdmin();

            Javalin app = createApp(permCache, tokenCache, auditLogger, true);

            io.javalin.testtools.JavalinTest.test(
                    app,
                    (server, client) -> {
                        String createBody =
                                """
                    {"roleId":"analyst","description":"Data analyst","permissions":[{"@type":"table","tableId":"db.reports","allowedOps":["SELECT"]}]}
                    """;
                        var createResp =
                                client.request(
                                        bearerPost(
                                                        origin(server),
                                                        "/api/v1/auth/roles",
                                                        "nbl-super-token",
                                                        createBody)
                                                .build());
                        assertThat(createResp.code()).isEqualTo(201);

                        var listResp =
                                client.request(
                                        bearerGet(
                                                        origin(server),
                                                        "/api/v1/auth/roles",
                                                        "nbl-super-token")
                                                .build());
                        assertThat(listResp.code()).isEqualTo(200);
                        assertThat(listResp.body().string()).contains("analyst");

                        var deleteResp =
                                client.request(
                                        new Request.Builder()
                                                .url(origin(server) + "/api/v1/auth/roles/analyst")
                                                .header("Authorization", "Bearer nbl-super-token")
                                                .delete()
                                                .build());
                        assertThat(deleteResp.code()).isEqualTo(200);
                    });
        }

        @Test
        @DisplayName("AuthRoutes API: createAgent->Agentautomatically obtaininitial role")
        void testAgentRegistrationWithInitialRoles() throws IOException {
            var permCache = new PermissionCache(100, 30);
            var tokenCache = new TokenCache(300);
            var auditLogger = new AuthAuditLogger(true);
            bootstrapSuperAdmin();
            createOrderReaderRole();

            Javalin app = createApp(permCache, tokenCache, auditLogger, true);

            io.javalin.testtools.JavalinTest.test(
                    app,
                    (server, client) -> {
                        String agentBody =
                                """
                    {"agentId":"report-agent","userId":"user-lisi","delegatedBy":"superadmin","initialRoles":["order-reader"]}
                    """;
                        var agentResp =
                                client.request(
                                        bearerPost(
                                                        origin(server),
                                                        "/api/v1/auth/agents",
                                                        "nbl-super-token",
                                                        agentBody)
                                                .build());
                        assertThat(agentResp.code()).isEqualTo(200);
                        var body = agentResp.body().string();
                        assertThat(body).contains("report-agent");
                        assertThat(body).contains("nbl_");
                    });
        }
    }

    @Nested
    @DisplayName("S2: Concurrency")
    class Concurrency {

        @Test
        @DisplayName("concurrentauthorizationsamerole multiplePrincipal->allPrincipal permission")
        void testConcurrentGrantSameRole() throws Exception {
            createOrderReaderRole();

            int threadCount = 5;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch latch = new CountDownLatch(threadCount);
            AtomicInteger successCount = new AtomicInteger(0);

            for (int i = 0; i < threadCount; i++) {
                final int idx = i;
                executor.submit(
                        () -> {
                            try {
                                Principal user =
                                        new Principal.UserPrincipal(
                                                "user-concurrent-" + idx, "User " + idx);
                                authMetastore.createToken("nbl-concurrent-token-" + idx, user);
                                authMetastore.grantRole(user, "order-reader", "superadmin");

                                ResolvedPermissions perms =
                                        authMetastore.resolvePermissions("user-concurrent-" + idx);
                                if (perms.hasPermission("db.orders", Permission.TableOp.SELECT)) {
                                    successCount.incrementAndGet();
                                }
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            } finally {
                                latch.countDown();
                            }
                        });
            }

            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(successCount.get()).isEqualTo(threadCount);
            executor.shutdown();
        }

        @Test
        @DisplayName("concurrentpermissioncheck->cacheconsistency->allcheck correct")
        void testConcurrentPermissionCheck() throws Exception {
            Principal user = new Principal.UserPrincipal("user-perm-check", "Perm Check User");
            authMetastore.createToken("nbl-perm-check-token", user);
            createOrderReaderRole();
            authMetastore.grantRole(user, "order-reader", "superadmin");

            var permCache = new PermissionCache(100, 30);

            int threadCount = 10;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch latch = new CountDownLatch(threadCount);
            AtomicInteger allowedCount = new AtomicInteger(0);

            for (int i = 0; i < threadCount; i++) {
                executor.submit(
                        () -> {
                            try {
                                ResolvedPermissions perms =
                                        permCache.get("user-perm-check", authMetastore);
                                if (perms.hasPermission("db.orders", Permission.TableOp.SELECT)) {
                                    allowedCount.incrementAndGet();
                                }
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            } finally {
                                latch.countDown();
                            }
                        });
            }

            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(allowedCount.get()).isEqualTo(threadCount);
            executor.shutdown();
        }

        @Test
        @DisplayName("concurrentTokenparse->TokenCacheconsistency->allparse correct")
        void testConcurrentTokenResolution() throws Exception {
            Principal user =
                    new Principal.UserPrincipal("user-token-resolve", "Token Resolve User");
            authMetastore.createToken("nbl-resolve-token", user);

            var tokenCache = new TokenCache(300);

            int threadCount = 10;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch latch = new CountDownLatch(threadCount);
            AtomicInteger correctCount = new AtomicInteger(0);

            for (int i = 0; i < threadCount; i++) {
                executor.submit(
                        () -> {
                            try {
                                Principal resolved =
                                        tokenCache.get("nbl-resolve-token", authMetastore);
                                if (resolved != null
                                        && "user-token-resolve".equals(resolved.id())) {
                                    correctCount.incrementAndGet();
                                }
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            } finally {
                                latch.countDown();
                            }
                        });
            }

            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(correctCount.get()).isEqualTo(threadCount);
            executor.shutdown();
        }
    }

    @Nested
    @DisplayName("S3: Fault Recovery")
    class FaultRecovery {

        @Test
        @DisplayName("PermissionCache after-> fromMetastoreload->permissioncorrectly recovers")
        void testPermissionCacheRecoveryAfterInvalidation() throws IOException {
            var permCache = new PermissionCache(100, 30);

            Principal user = new Principal.UserPrincipal("user-recovery", "Recovery User");
            authMetastore.createToken("nbl-recovery-token", user);
            createOrderReaderRole();
            authMetastore.grantRole(user, "order-reader", "superadmin");

            ResolvedPermissions permsBefore = permCache.get("user-recovery", authMetastore);
            assertThat(permsBefore.hasPermission("db.orders", Permission.TableOp.SELECT)).isTrue();

            authMetastore.revokeRole(user, "order-reader");
            permCache.invalidate("user-recovery");

            ResolvedPermissions permsAfter = permCache.get("user-recovery", authMetastore);
            assertThat(permsAfter.hasPermission("db.orders", Permission.TableOp.SELECT)).isFalse();
        }

        @Test
        @DisplayName("TokenCache after-> fromMetastoreload-> correctly recovers")
        void testTokenCacheRecoveryAfterInvalidation() throws IOException {
            var tokenCache = new TokenCache(300);

            Principal user =
                    new Principal.UserPrincipal("user-token-recovery", "Token Recovery User");
            authMetastore.createToken("nbl-token-recovery", user);

            Principal resolved1 = tokenCache.get("nbl-token-recovery", authMetastore);
            assertThat(resolved1).isNotNull();
            assertThat(resolved1.id()).isEqualTo("user-token-recovery");

            tokenCache.invalidate("nbl-token-recovery");

            Principal resolved2 = tokenCache.get("nbl-token-recovery", authMetastore);
            assertThat(resolved2).isNotNull();
            assertThat(resolved2.id()).isEqualTo("user-token-recovery");
        }

        @Test
        @DisplayName("roleupdateafter->PermissionCache -> checkacquire permission")
        void testRoleUpdateTriggersCacheInvalidation() throws IOException {
            var permCache = new PermissionCache(100, 30);

            Principal user = new Principal.UserPrincipal("user-role-update", "Role Update User");
            authMetastore.createToken("nbl-role-update-token", user);

            createOrderReaderRole();
            authMetastore.grantRole(user, "order-reader", "superadmin");

            ResolvedPermissions permsBefore = permCache.get("user-role-update", authMetastore);
            assertThat(permsBefore.hasPermission("db.orders", Permission.TableOp.SELECT)).isTrue();
            assertThat(permsBefore.hasPermission("db.orders", Permission.TableOp.INSERT)).isFalse();

            var writePerm =
                    new Permission.TablePermission(
                            "db.orders",
                            Set.of(Permission.TableOp.INSERT, Permission.TableOp.SELECT));
            authMetastore.createRole(
                    new Role(
                            "order-writer",
                            "Write orders",
                            Set.of(writePerm),
                            System.currentTimeMillis(),
                            System.currentTimeMillis()));
            authMetastore.grantRole(user, "order-writer", "superadmin");
            permCache.invalidate("user-role-update");

            ResolvedPermissions permsAfter = permCache.get("user-role-update", authMetastore);
            assertThat(permsAfter.hasPermission("db.orders", Permission.TableOp.INSERT)).isTrue();
        }

        @Test
        @DisplayName("roledeleteafter->PermissionCache ->HTTP 403")
        void testRoleDeletePropagatesToPermissionCheck() throws IOException {
            var permCache = new PermissionCache(100, 30);
            var tokenCache = new TokenCache(300);
            var auditLogger = new AuthAuditLogger(true);

            Principal user = new Principal.UserPrincipal("user-role-delete", "Role Delete User");
            authMetastore.createToken("nbl-role-delete-token", user);
            createOrderReaderRole();
            authMetastore.grantRole(user, "order-reader", "superadmin");

            Javalin app = createAppNoRoutes(permCache, tokenCache, auditLogger, true);

            io.javalin.testtools.JavalinTest.test(
                    app,
                    (server, client) -> {
                        var beforeDelete =
                                client.request(
                                        bearerGet(
                                                        origin(server),
                                                        "/api/tables/db.orders",
                                                        "nbl-role-delete-token")
                                                .build());
                        assertThat(beforeDelete.code()).isEqualTo(200);

                        try {
                            authMetastore.deleteRole("order-reader");
                            permCache.invalidate("user-role-delete");
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        var afterDelete =
                                client.request(
                                        bearerGet(
                                                        origin(server),
                                                        "/api/tables/db.orders",
                                                        "nbl-role-delete-token")
                                                .build());
                        assertThat(afterDelete.code()).isEqualTo(403);
                    });
        }
    }

    @Nested
    @DisplayName("S4: Boundary Conditions")
    class BoundaryConditions {

        @Test
        @DisplayName("SuperAdmin allpermissioncheck-> table via")
        void testSuperAdminBypassAllChecks() throws IOException {
            var permCache = new PermissionCache(100, 30);
            var tokenCache = new TokenCache(300);
            var auditLogger = new AuthAuditLogger(true);
            bootstrapSuperAdmin();

            Javalin app = createAppNoRoutes(permCache, tokenCache, auditLogger, true);

            io.javalin.testtools.JavalinTest.test(
                    app,
                    (server, client) -> {
                        var response =
                                client.request(
                                        bearerGet(
                                                        origin(server),
                                                        "/api/tables/any-table",
                                                        "nbl-super-token")
                                                .build());
                        assertThat(response.code()).isEqualTo(200);
                    });
        }

        @Test
        @DisplayName("no Token->authenticationfail 401")
        void testInvalidTokenReturns401() throws IOException {
            var permCache = new PermissionCache(100, 30);
            var tokenCache = new TokenCache(300);
            var auditLogger = new AuthAuditLogger(true);

            Javalin app = createAppNoRoutes(permCache, tokenCache, auditLogger, true);

            io.javalin.testtools.JavalinTest.test(
                    app,
                    (server, client) -> {
                        var response =
                                client.request(
                                        bearerGet(
                                                        origin(server),
                                                        "/api/tables/orders",
                                                        "invalid-token-xxx")
                                                .build());
                        assertThat(response.code()).isEqualTo(401);
                    });
        }

        @Test
        @DisplayName("noToken->authenticationfail 401")
        void testMissingTokenReturns401() throws IOException {
            var permCache = new PermissionCache(100, 30);
            var tokenCache = new TokenCache(300);
            var auditLogger = new AuthAuditLogger(true);

            Javalin app = createAppNoRoutes(permCache, tokenCache, auditLogger, true);

            io.javalin.testtools.JavalinTest.test(
                    app,
                    (server, client) -> {
                        var response =
                                client.request(
                                        noAuthGet(origin(server), "/api/tables/orders").build());
                        assertThat(response.code()).isEqualTo(401);
                    });
        }

        @Test
        @DisplayName("has Token nopermission->authorizationfail 403")
        void testValidTokenNoPermissionReturns403() throws IOException {
            var permCache = new PermissionCache(100, 30);
            var tokenCache = new TokenCache(300);
            var auditLogger = new AuthAuditLogger(true);

            Principal user = new Principal.UserPrincipal("user-no-perm", "No Perm User");
            authMetastore.createToken("nbl-no-perm-token", user);

            Javalin app = createAppNoRoutes(permCache, tokenCache, auditLogger, true);

            io.javalin.testtools.JavalinTest.test(
                    app,
                    (server, client) -> {
                        var response =
                                client.request(
                                        bearerGet(
                                                        origin(server),
                                                        "/api/tables/orders",
                                                        "nbl-no-perm-token")
                                                .build());
                        assertThat(response.code()).isEqualTo(403);
                    });
        }

        @Test
        @DisplayName("Authdisabled mode->all requestspasses directlyno authentication required")
        void testAuthDisabledAllRequestsPass() throws IOException {
            var permCache = new PermissionCache(100, 30);
            var tokenCache = new TokenCache(300);
            var auditLogger = new AuthAuditLogger(true);

            Javalin app = createAppNoRoutes(permCache, tokenCache, auditLogger, false);

            io.javalin.testtools.JavalinTest.test(
                    app,
                    (server, client) -> {
                        var response =
                                client.request(
                                        noAuthGet(origin(server), "/api/tables/orders").build());
                        assertThat(response.code()).isEqualTo(200);
                    });
        }

        @Test
        @DisplayName("OWNERSHIPpermission-> hasalloperationspermission")
        void testOwnershipGrantsAllOps() throws IOException {
            var ownerPerm =
                    new Permission.TablePermission(
                            "db.orders", Set.of(Permission.TableOp.OWNERSHIP));
            authMetastore.createRole(
                    new Role(
                            "owner",
                            "Table Owner",
                            Set.of(ownerPerm),
                            System.currentTimeMillis(),
                            System.currentTimeMillis()));

            Principal user = new Principal.UserPrincipal("user-owner", "Owner User");
            authMetastore.createToken("nbl-owner-token", user);
            authMetastore.grantRole(user, "owner", "superadmin");

            ResolvedPermissions perms = authMetastore.resolvePermissions("user-owner");
            assertThat(perms.hasPermission("db.orders", Permission.TableOp.SELECT)).isTrue();
            assertThat(perms.hasPermission("db.orders", Permission.TableOp.INSERT)).isTrue();
            assertThat(perms.hasPermission("db.orders", Permission.TableOp.DELETE)).isTrue();
            assertThat(perms.hasPermission("db.orders", Permission.TableOp.DROP)).isTrue();
            assertThat(perms.hasPermission("db.orders", Permission.TableOp.ALTER)).isTrue();
        }

        @Test
        @DisplayName("rolepermission ->allpermissioncheck denied")
        void testEmptyRolePermissionsDeniesAll() throws IOException {
            authMetastore.createRole(
                    new Role(
                            "empty-role",
                            "No Permissions",
                            Set.of(),
                            System.currentTimeMillis(),
                            System.currentTimeMillis()));

            Principal user = new Principal.UserPrincipal("user-empty-role", "Empty Role User");
            authMetastore.createToken("nbl-empty-role-token", user);
            authMetastore.grantRole(user, "empty-role", "superadmin");

            ResolvedPermissions perms = authMetastore.resolvePermissions("user-empty-role");
            assertThat(perms.hasPermission("db.orders", Permission.TableOp.SELECT)).isFalse();
            assertThat(perms.tablePermissions()).isEmpty();
        }

        @Test
        @DisplayName("non-existent table->permission check denied")
        void testNonExistentTablePermissionDenied() throws IOException {
            createOrderReaderRole();

            Principal user = new Principal.UserPrincipal("user-wrong-table", "Wrong Table User");
            authMetastore.createToken("nbl-wrong-table-token", user);
            authMetastore.grantRole(user, "order-reader", "superadmin");

            ResolvedPermissions perms = authMetastore.resolvePermissions("user-wrong-table");
            assertThat(perms.hasPermission("db.nonexistent", Permission.TableOp.SELECT)).isFalse();
        }

        @Test
        @DisplayName("Token after->TokenCache ->sameTokenno authentication")
        void testTokenRevocation() throws IOException {
            var permCache = new PermissionCache(100, 30);
            var tokenCache = new TokenCache(300);
            var auditLogger = new AuthAuditLogger(true);

            Principal user = new Principal.UserPrincipal("user-revoke-token", "Revoke Token User");
            authMetastore.createToken("nbl-revoke-token", user);
            createOrderReaderRole();
            authMetastore.grantRole(user, "order-reader", "superadmin");

            Javalin app = createAppNoRoutes(permCache, tokenCache, auditLogger, true);

            io.javalin.testtools.JavalinTest.test(
                    app,
                    (server, client) -> {
                        var beforeRevoke =
                                client.request(
                                        bearerGet(
                                                        origin(server),
                                                        "/api/tables/db.orders",
                                                        "nbl-revoke-token")
                                                .build());
                        assertThat(beforeRevoke.code()).isEqualTo(200);

                        try {
                            authMetastore.revokeToken("user-revoke-token");
                            tokenCache.invalidate("nbl-revoke-token");
                            permCache.invalidate("user-revoke-token");
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        var afterRevoke =
                                client.request(
                                        bearerGet(
                                                        origin(server),
                                                        "/api/tables/db.orders",
                                                        "nbl-revoke-token")
                                                .build());
                        assertThat(afterRevoke.code()).isEqualTo(401);
                    });
        }
    }
}
