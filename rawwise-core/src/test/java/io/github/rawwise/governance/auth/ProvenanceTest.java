package io.github.rawwise.governance.auth;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.metastore.InMemoryMetastore;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("Provenance test")
class ProvenanceTest {

    private InMemoryMetastore metastore;
    private DefaultProvenanceStore provenanceStore;
    private DefaultAuthMetastore authMetastore;

    @BeforeEach
    void setUp() throws IOException {
        metastore = new InMemoryMetastore();
        provenanceStore = new DefaultProvenanceStore(metastore);
        authMetastore = new DefaultAuthMetastore(metastore);
    }

    private ProvenanceEvent createEvent(
            String principalId,
            String action,
            String targetId,
            String targetType,
            boolean granted,
            long timestamp) {
        return new ProvenanceEvent(
                UUID.randomUUID().toString(),
                principalId,
                "user",
                action,
                targetId,
                targetType,
                granted,
                Map.of(),
                timestamp);
    }

    private ProvenanceEvent createEventWithContext(
            String principalId,
            String action,
            String targetId,
            String targetType,
            boolean granted,
            long timestamp,
            Map<String, String> context) {
        return new ProvenanceEvent(
                UUID.randomUUID().toString(),
                principalId,
                "agent",
                action,
                targetId,
                targetType,
                granted,
                context,
                timestamp);
    }

    @Nested
    @DisplayName("DefaultProvenanceStore persistedandquery")
    class StorePersistence {

        @Test
        @DisplayName("record: singleitem persistedaftercan principal query")
        void testRecordAndQueryByPrincipal() throws IOException {
            long ts = System.currentTimeMillis();
            ProvenanceEvent event = createEvent("user-1", "ACCESS", "db.orders", "TABLE", true, ts);
            provenanceStore.record(event);

            List<ProvenanceEvent> results = provenanceStore.queryByPrincipal("user-1", 0, ts + 1);
            assertThat(results).hasSize(1);
            assertThat(results.getFirst().principalId()).isEqualTo("user-1");
            assertThat(results.getFirst().action()).isEqualTo("ACCESS");
            assertThat(results.getFirst().targetId()).isEqualTo("db.orders");
        }

        @Test
        @DisplayName("record: multiitem persistedafter principal filtercorrect")
        void testQueryByPrincipalFiltersCorrectly() throws IOException {
            long ts = System.currentTimeMillis();
            provenanceStore.record(createEvent("user-1", "ACCESS", "db.orders", "TABLE", true, ts));
            provenanceStore.record(createEvent("user-2", "ACCESS", "db.orders", "TABLE", true, ts));
            provenanceStore.record(createEvent("user-1", "GRANT", "role-reader", "ROLE", true, ts));

            List<ProvenanceEvent> user1Events =
                    provenanceStore.queryByPrincipal("user-1", 0, ts + 1);
            assertThat(user1Events).hasSize(2);
            assertThat(user1Events).allMatch(e -> e.principalId().equals("user-1"));
        }

        @Test
        @DisplayName("queryByTarget: type+ID query")
        void testQueryByTarget() throws IOException {
            long ts = System.currentTimeMillis();
            provenanceStore.record(createEvent("user-1", "ACCESS", "db.orders", "TABLE", true, ts));
            provenanceStore.record(
                    createEvent("user-2", "ACCESS", "db.orders", "TABLE", false, ts));
            provenanceStore.record(createEvent("user-1", "ACCESS", "db.users", "TABLE", true, ts));

            List<ProvenanceEvent> orderEvents =
                    provenanceStore.queryByTarget("TABLE", "db.orders", 0, ts + 1);
            assertThat(orderEvents).hasSize(2);
            assertThat(orderEvents).allMatch(e -> e.targetId().equals("db.orders"));
        }

        @Test
        @DisplayName("queryByAction: actiontypequery")
        void testQueryByAction() throws IOException {
            long ts = System.currentTimeMillis();
            provenanceStore.record(createEvent("user-1", "GRANT", "role-reader", "ROLE", true, ts));
            provenanceStore.record(createEvent("user-1", "ACCESS", "db.orders", "TABLE", true, ts));
            provenanceStore.record(
                    createEvent("admin-1", "GRANT", "role-writer", "ROLE", true, ts));

            List<ProvenanceEvent> grantEvents = provenanceStore.queryByAction("GRANT", 0, ts + 1);
            assertThat(grantEvents).hasSize(2);
            assertThat(grantEvents).allMatch(e -> e.action().equals("GRANT"));
        }

        @Test
        @DisplayName("queryByTimeRange: filter")
        void testQueryByTimeRange() throws IOException {
            long t1 = 1000L;
            long t2 = 2000L;
            long t3 = 3000L;

            provenanceStore.record(createEvent("user-1", "ACCESS", "db.orders", "TABLE", true, t1));
            provenanceStore.record(createEvent("user-1", "GRANT", "role-reader", "ROLE", true, t2));
            provenanceStore.record(createEvent("user-1", "ACCESS", "db.users", "TABLE", true, t3));

            List<ProvenanceEvent> midRange = provenanceStore.queryByTimeRange(t1, t2);
            assertThat(midRange).hasSize(2);

            List<ProvenanceEvent> allRange = provenanceStore.queryByTimeRange(0, t3 + 1);
            assertThat(allRange).hasSize(3);

            List<ProvenanceEvent> narrowRange = provenanceStore.queryByTimeRange(t2, t2);
            assertThat(narrowRange).hasSize(1);
            assertThat(narrowRange.get(0).action()).isEqualTo("GRANT");
        }

        @Test
        @DisplayName("queryByTimeRange:")
        void testQueryByTimeRangeEmpty() throws IOException {
            long ts = System.currentTimeMillis();
            provenanceStore.record(createEvent("user-1", "ACCESS", "db.orders", "TABLE", true, ts));

            List<ProvenanceEvent> results = provenanceStore.queryByTimeRange(0, ts - 1000);
            assertThat(results).isEmpty();
        }

        @Test
        @DisplayName("queryByPrincipal: filter")
        void testQueryByPrincipalWithTimeRange() throws IOException {
            long t1 = 1000L;
            long t2 = 2000L;
            long t3 = 3000L;

            provenanceStore.record(createEvent("user-1", "ACCESS", "db.orders", "TABLE", true, t1));
            provenanceStore.record(createEvent("user-1", "GRANT", "role-reader", "ROLE", true, t2));
            provenanceStore.record(createEvent("user-1", "ACCESS", "db.users", "TABLE", true, t3));

            List<ProvenanceEvent> results = provenanceStore.queryByPrincipal("user-1", t2, t3);
            assertThat(results).hasSize(2);
        }

        @Test
        @DisplayName("record: Agent type persistedandquery")
        void testAgentEventPersistence() throws IOException {
            long ts = System.currentTimeMillis();
            ProvenanceEvent agentEvent =
                    createEventWithContext(
                            "agent-001",
                            "ACCESS",
                            "db.orders",
                            "TABLE",
                            true,
                            ts,
                            Map.of("delegatedBy", "user-1", "ops", "[\"SELECT\"]"));
            provenanceStore.record(agentEvent);

            List<ProvenanceEvent> results =
                    provenanceStore.queryByPrincipal("agent-001", 0, ts + 1);
            assertThat(results).hasSize(1);
            assertThat(results.get(0).principalType()).isEqualTo("agent");
            assertThat(results.get(0).context()).containsEntry("delegatedBy", "user-1");
        }

        @Test
        @DisplayName("record: persistedandquery can")
        void testBulkRecordAndQuery() throws IOException {
            long baseTs = System.currentTimeMillis();
            for (int i = 0; i < 100; i++) {
                provenanceStore.record(
                        createEvent(
                                "user-" + (i % 5),
                                "ACCESS",
                                "db.table-" + (i % 3),
                                "TABLE",
                                i % 2 == 0,
                                baseTs + i));
            }

            List<ProvenanceEvent> user0Events =
                    provenanceStore.queryByPrincipal("user-0", 0, baseTs + 100);
            assertThat(user0Events).hasSize(20);

            List<ProvenanceEvent> allEvents = provenanceStore.queryByTimeRange(0, baseTs + 100);
            assertThat(allEvents).hasSize(100);
        }

        @Test
        @DisplayName("record: CAS duplicatewrite same eventId")
        void testCasPreventsDuplicateEventId() throws IOException {
            long ts = System.currentTimeMillis();
            String eventId = UUID.randomUUID().toString();
            ProvenanceEvent event1 =
                    new ProvenanceEvent(
                            eventId,
                            "user-1",
                            "user",
                            "ACCESS",
                            "db.orders",
                            "TABLE",
                            true,
                            Map.of(),
                            ts);
            ProvenanceEvent event2 =
                    new ProvenanceEvent(
                            eventId,
                            "user-1",
                            "user",
                            "ACCESS",
                            "db.orders",
                            "TABLE",
                            false,
                            Map.of(),
                            ts);

            provenanceStore.record(event1);
            provenanceStore.record(event2);

            List<ProvenanceEvent> results = provenanceStore.queryByTimeRange(ts, ts + 1);
            assertThat(results).hasSize(1);
            assertThat(results.getFirst().granted()).isTrue();
        }
    }

    @Nested
    @DisplayName("ProvenanceAuditor")
    class AuditorIntegration {

        @Test
        @DisplayName("ProvenanceAuditor: logAccess same write and ProvenanceStore")
        void testLogAccessPersistsToStore() throws IOException {
            ProvenanceAuditor auditor = new ProvenanceAuditor(true, provenanceStore, true);
            Principal user = new Principal.UserPrincipal("user-1", "Test User");
            long before = System.currentTimeMillis();

            auditor.logAccess(
                    user,
                    Set.of("db.orders"),
                    Set.of(Permission.TableOp.SELECT),
                    true,
                    "/api/query");

            long after = System.currentTimeMillis();
            List<ProvenanceEvent> events =
                    provenanceStore.queryByPrincipal("user-1", before, after + 1);
            assertThat(events).hasSize(1);
            assertThat(events.getFirst().action()).isEqualTo("ACCESS");
            assertThat(events.getFirst().targetId()).isEqualTo("db.orders");
            assertThat(events.getFirst().granted()).isTrue();
            assertThat(events.getFirst().context()).containsEntry("ops", "[\"SELECT\"]");
        }

        @Test
        @DisplayName("ProvenanceAuditor: logAccess multi-tablegeneratemultiitem")
        void testLogAccessMultipleTables() throws IOException {
            ProvenanceAuditor auditor = new ProvenanceAuditor(true, provenanceStore, true);
            Principal user = new Principal.UserPrincipal("user-1", "Test User");
            long before = System.currentTimeMillis();

            auditor.logAccess(
                    user,
                    Set.of("db.orders", "db.users"),
                    Set.of(Permission.TableOp.SELECT),
                    true,
                    "/api/query");

            long after = System.currentTimeMillis();
            List<ProvenanceEvent> events =
                    provenanceStore.queryByPrincipal("user-1", before, after + 1);
            assertThat(events).hasSize(2);
            Set<String> targetIds =
                    events.stream()
                            .map(ProvenanceEvent::targetId)
                            .collect(java.util.stream.Collectors.toSet());
            assertThat(targetIds).containsExactlyInAnyOrder("db.orders", "db.users");
        }

        @Test
        @DisplayName("ProvenanceAuditor: logGrant recordauthorization")
        void testLogGrantPersistsToStore() throws IOException {
            ProvenanceAuditor auditor = new ProvenanceAuditor(true, provenanceStore, true);
            Principal user = new Principal.UserPrincipal("user-1", "Test User");
            long before = System.currentTimeMillis();

            auditor.logGrant(user, "role-reader", "superadmin");

            long after = System.currentTimeMillis();
            List<ProvenanceEvent> events =
                    provenanceStore.queryByPrincipal("user-1", before, after + 1);
            assertThat(events).hasSize(1);
            assertThat(events.get(0).action()).isEqualTo("GRANT");
            assertThat(events.get(0).targetId()).isEqualTo("role-reader");
            assertThat(events.get(0).targetType()).isEqualTo("ROLE");
            assertThat(events.get(0).context()).containsEntry("grantedBy", "superadmin");
        }

        @Test
        @DisplayName("ProvenanceAuditor: logRevoke record")
        void testLogRevokePersistsToStore() throws IOException {
            ProvenanceAuditor auditor = new ProvenanceAuditor(true, provenanceStore, true);
            Principal user = new Principal.UserPrincipal("user-1", "Test User");
            long before = System.currentTimeMillis();

            auditor.logRevoke(user, "role-reader", "superadmin");

            long after = System.currentTimeMillis();
            List<ProvenanceEvent> events =
                    provenanceStore.queryByAction("REVOKE", before, after + 1);
            assertThat(events).hasSize(1);
            assertThat(events.get(0).context()).containsEntry("revokedBy", "superadmin");
        }

        @Test
        @DisplayName("ProvenanceAuditor: logRoleCreate recordrolecreate")
        void testLogRoleCreatePersistsToStore() throws IOException {
            ProvenanceAuditor auditor = new ProvenanceAuditor(true, provenanceStore, true);
            Principal admin = new Principal.UserPrincipal("admin", "Admin");
            long before = System.currentTimeMillis();

            auditor.logRoleCreate(admin, "role-analyst");

            long after = System.currentTimeMillis();
            List<ProvenanceEvent> events =
                    provenanceStore.queryByTarget("ROLE", "role-analyst", before, after + 1);
            assertThat(events).hasSize(1);
            assertThat(events.get(0).action()).isEqualTo("ROLE_CREATE");
        }

        @Test
        @DisplayName("ProvenanceAuditor: logRoleDelete recordroledelete")
        void testLogRoleDeletePersistsToStore() throws IOException {
            ProvenanceAuditor auditor = new ProvenanceAuditor(true, provenanceStore, true);
            Principal admin = new Principal.UserPrincipal("admin", "Admin");
            long before = System.currentTimeMillis();

            auditor.logRoleDelete(admin, "role-analyst");

            long after = System.currentTimeMillis();
            List<ProvenanceEvent> events =
                    provenanceStore.queryByAction("ROLE_DELETE", before, after + 1);
            assertThat(events).hasSize(1);
            assertThat(events.get(0).targetId()).isEqualTo("role-analyst");
        }

        @Test
        @DisplayName("ProvenanceAuditor: logTokenCreate record create")
        void testLogTokenCreatePersistsToStore() throws IOException {
            ProvenanceAuditor auditor = new ProvenanceAuditor(true, provenanceStore, true);
            Principal admin = new Principal.UserPrincipal("admin", "Admin");
            long before = System.currentTimeMillis();

            auditor.logTokenCreate(admin, "user-1");

            long after = System.currentTimeMillis();
            List<ProvenanceEvent> events =
                    provenanceStore.queryByTarget("TOKEN", "user-1", before, after + 1);
            assertThat(events).hasSize(1);
            assertThat(events.get(0).action()).isEqualTo("TOKEN_CREATE");
        }

        @Test
        @DisplayName("ProvenanceAuditor: logTokenRevoke record")
        void testLogTokenRevokePersistsToStore() throws IOException {
            ProvenanceAuditor auditor = new ProvenanceAuditor(true, provenanceStore, true);
            Principal admin = new Principal.UserPrincipal("admin", "Admin");
            long before = System.currentTimeMillis();

            auditor.logTokenRevoke(admin, "user-1");

            long after = System.currentTimeMillis();
            List<ProvenanceEvent> events =
                    provenanceStore.queryByAction("TOKEN_REVOKE", before, after + 1);
            assertThat(events).hasSize(1);
        }

        @Test
        @DisplayName("ProvenanceAuditor: logAgentCreate record Agent create")
        void testLogAgentCreatePersistsToStore() throws IOException {
            ProvenanceAuditor auditor = new ProvenanceAuditor(true, provenanceStore, true);
            Principal admin = new Principal.UserPrincipal("admin", "Admin");
            long before = System.currentTimeMillis();

            auditor.logAgentCreate(admin, "agent-001");

            long after = System.currentTimeMillis();
            List<ProvenanceEvent> events =
                    provenanceStore.queryByTarget("AGENT", "agent-001", before, after + 1);
            assertThat(events).hasSize(1);
            assertThat(events.get(0).action()).isEqualTo("AGENT_CREATE");
        }

        @Test
        @DisplayName("ProvenanceAuditor: provenanceEnabled=false write ProvenanceStore")
        void testProvenanceDisabledSkipsStore() throws IOException {
            ProvenanceAuditor auditor = new ProvenanceAuditor(true, provenanceStore, false);
            Principal user = new Principal.UserPrincipal("user-1", "Test User");
            long before = System.currentTimeMillis();

            auditor.logAccess(
                    user,
                    Set.of("db.orders"),
                    Set.of(Permission.TableOp.SELECT),
                    true,
                    "/api/query");
            auditor.logGrant(user, "role-reader", "admin");

            long after = System.currentTimeMillis();
            List<ProvenanceEvent> events = provenanceStore.queryByTimeRange(before, after + 1);
            assertThat(events).isEmpty();
        }

        @Test
        @DisplayName("ProvenanceAuditor: Agent type principal of principalType is agent")
        void testAgentPrincipalType() throws IOException {
            ProvenanceAuditor auditor = new ProvenanceAuditor(true, provenanceStore, true);
            Principal agent = new Principal.AgentPrincipal("agent-001", "user-1", "admin");
            long before = System.currentTimeMillis();

            auditor.logAccess(
                    agent,
                    Set.of("db.orders"),
                    Set.of(Permission.TableOp.SELECT),
                    true,
                    "/api/query");

            long after = System.currentTimeMillis();
            List<ProvenanceEvent> events =
                    provenanceStore.queryByPrincipal("agent-001", before, after + 1);
            assertThat(events).hasSize(1);
            assertThat(events.get(0).principalType()).isEqualTo("agent");
        }

        @Test
        @DisplayName("ProvenanceAuditor: denied granted=false")
        void testDeniedAccessEvent() throws IOException {
            ProvenanceAuditor auditor = new ProvenanceAuditor(true, provenanceStore, true);
            Principal user = new Principal.UserPrincipal("user-1", "Test User");
            long before = System.currentTimeMillis();

            auditor.logAccess(
                    user,
                    Set.of("db.orders"),
                    Set.of(Permission.TableOp.SELECT),
                    false,
                    "/api/query");

            long after = System.currentTimeMillis();
            List<ProvenanceEvent> events =
                    provenanceStore.queryByPrincipal("user-1", before, after + 1);
            assertThat(events).hasSize(1);
            assertThat(events.get(0).granted()).isFalse();
        }
    }

    @Nested
    @DisplayName("RBAC operations")
    class RbacProvenanceChain {

        @Test
        @DisplayName("RBAC lifecycle : createToken->createrole->authorization-> -> ->deleterole")
        void testFullRbacProvenanceChain() throws IOException {
            ProvenanceAuditor auditor = new ProvenanceAuditor(true, provenanceStore, true);
            Principal admin = new Principal.UserPrincipal("admin", "Admin");
            Principal user = new Principal.UserPrincipal("user-1", "Zhang San");
            long baseTs = System.currentTimeMillis();

            auditor.logTokenCreate(admin, "user-1");
            auditor.logRoleCreate(admin, "order-reader");
            auditor.logGrant(user, "order-reader", "admin");
            auditor.logAccess(
                    user,
                    Set.of("db.orders"),
                    Set.of(Permission.TableOp.SELECT),
                    true,
                    "/api/tables/db.orders");
            auditor.logRevoke(user, "order-reader", "admin");
            auditor.logAccess(
                    user,
                    Set.of("db.orders"),
                    Set.of(Permission.TableOp.SELECT),
                    false,
                    "/api/tables/db.orders");
            auditor.logRoleDelete(admin, "order-reader");

            long endTs = System.currentTimeMillis() + 1;

            List<ProvenanceEvent> allEvents = provenanceStore.queryByTimeRange(baseTs, endTs);
            assertThat(allEvents).hasSize(7);

            List<ProvenanceEvent> userEvents =
                    provenanceStore.queryByPrincipal("user-1", baseTs, endTs);
            assertThat(userEvents).hasSize(4);
            List<String> userActions =
                    userEvents.stream()
                            .map(ProvenanceEvent::action)
                            .collect(java.util.stream.Collectors.toList());
            assertThat(userActions)
                    .containsExactlyInAnyOrder("GRANT", "REVOKE", "ACCESS", "ACCESS");

            List<ProvenanceEvent> roleEvents =
                    provenanceStore.queryByTarget("ROLE", "order-reader", baseTs, endTs);
            assertThat(roleEvents).hasSize(4);
            assertThat(
                            roleEvents.stream()
                                    .map(ProvenanceEvent::action)
                                    .collect(java.util.stream.Collectors.toList()))
                    .containsExactlyInAnyOrder("ROLE_CREATE", "GRANT", "REVOKE", "ROLE_DELETE");

            List<ProvenanceEvent> accessEvents =
                    provenanceStore.queryByAction("ACCESS", baseTs, endTs);
            assertThat(accessEvents).hasSize(2);

            long grantedCount = accessEvents.stream().filter(ProvenanceEvent::granted).count();
            long deniedCount = accessEvents.stream().filter(e -> !e.granted()).count();
            assertThat(grantedCount).isEqualTo(1);
            assertThat(deniedCount).isEqualTo(1);
        }

        @Test
        @DisplayName("Agent delegation chain : createAgent->Agent -> delegation source")
        void testAgentDelegationProvenance() throws IOException {
            ProvenanceAuditor auditor = new ProvenanceAuditor(true, provenanceStore, true);
            Principal admin = new Principal.UserPrincipal("admin", "Admin");
            Principal agent = new Principal.AgentPrincipal("agent-001", "user-zhangsan", "admin");
            long baseTs = System.currentTimeMillis();

            auditor.logAgentCreate(admin, "agent-001");
            auditor.logGrant(agent, "order-reader", "admin");
            auditor.logAccess(
                    agent,
                    Set.of("db.orders"),
                    Set.of(Permission.TableOp.SELECT),
                    true,
                    "/api/query");

            long endTs = System.currentTimeMillis() + 1;

            List<ProvenanceEvent> agentEvents =
                    provenanceStore.queryByPrincipal("agent-001", baseTs, endTs);
            assertThat(agentEvents).hasSize(2);

            List<ProvenanceEvent> createEvents =
                    provenanceStore.queryByAction("AGENT_CREATE", baseTs, endTs);
            assertThat(createEvents).hasSize(1);
            assertThat(createEvents.get(0).principalType()).isEqualTo("user");
            assertThat(createEvents.get(0).targetId()).isEqualTo("agent-001");
            assertThat(createEvents.get(0).targetType()).isEqualTo("AGENT");

            List<ProvenanceEvent> accessEvents =
                    provenanceStore.queryByAction("ACCESS", baseTs, endTs);
            assertThat(accessEvents).hasSize(1);
            assertThat(accessEvents.get(0).principalType()).isEqualTo("agent");
        }

        @Test
        @DisplayName("multiuseroperations : differentuserofoperationscanindependentquery")
        void testMultiUserProvenanceIsolation() throws IOException {
            ProvenanceAuditor auditor = new ProvenanceAuditor(true, provenanceStore, true);
            Principal user1 = new Principal.UserPrincipal("user-1", "Zhang San");
            Principal user2 = new Principal.UserPrincipal("user-2", "Li Si");
            long baseTs = System.currentTimeMillis();

            auditor.logAccess(
                    user1,
                    Set.of("db.orders"),
                    Set.of(Permission.TableOp.SELECT),
                    true,
                    "/api/tables/db.orders");
            auditor.logAccess(
                    user2,
                    Set.of("db.orders"),
                    Set.of(Permission.TableOp.SELECT),
                    false,
                    "/api/tables/db.orders");
            auditor.logGrant(user1, "role-reader", "admin");

            long endTs = System.currentTimeMillis() + 1;

            List<ProvenanceEvent> user1Events =
                    provenanceStore.queryByPrincipal("user-1", baseTs, endTs);
            assertThat(user1Events).hasSize(2);

            List<ProvenanceEvent> user2Events =
                    provenanceStore.queryByPrincipal("user-2", baseTs, endTs);
            assertThat(user2Events).hasSize(1);
            assertThat(user2Events.get(0).granted()).isFalse();
        }

        @Test
        @DisplayName("same of operationshistory: query originaloperations")
        void testTargetOperationHistory() throws IOException {
            ProvenanceAuditor auditor = new ProvenanceAuditor(true, provenanceStore, true);
            Principal admin = new Principal.UserPrincipal("admin", "Admin");
            Principal user = new Principal.UserPrincipal("user-1", "Zhang San");
            long baseTs = System.currentTimeMillis();

            auditor.logRoleCreate(admin, "order-reader");
            auditor.logGrant(user, "order-reader", "admin");
            auditor.logRevoke(user, "order-reader", "admin");
            auditor.logRoleDelete(admin, "order-reader");

            long endTs = System.currentTimeMillis() + 1;

            List<ProvenanceEvent> roleHistory =
                    provenanceStore.queryByTarget("ROLE", "order-reader", baseTs, endTs);
            assertThat(roleHistory).hasSize(4);

            List<String> actions =
                    roleHistory.stream()
                            .map(ProvenanceEvent::action)
                            .collect(java.util.stream.Collectors.toList());
            assertThat(actions)
                    .containsExactlyInAnyOrder("ROLE_CREATE", "GRANT", "REVOKE", "ROLE_DELETE");
        }
    }

    @Nested
    @DisplayName("ProvenanceStore and AuthMetastore coexist")
    class StoreCoexistence {

        @Test
        @DisplayName("ProvenanceStore and AuthMetastore same Metastore conflict")
        void testProvenanceAndAuthMetastoreCoexistence() throws IOException {
            Principal user = new Principal.UserPrincipal("user-1", "Test User");
            authMetastore.createToken("nbl-test-token", user);
            authMetastore.setSuperAdmin(user);

            var readPerm =
                    new Permission.TablePermission("db.orders", Set.of(Permission.TableOp.SELECT));
            authMetastore.createRole(
                    new Role(
                            "order-reader",
                            "Read orders",
                            Set.of(readPerm),
                            System.currentTimeMillis(),
                            System.currentTimeMillis()));
            authMetastore.grantRole(user, "order-reader", "admin");

            long ts = System.currentTimeMillis();
            provenanceStore.record(createEvent("user-1", "ACCESS", "db.orders", "TABLE", true, ts));

            assertThat(authMetastore.isSuperAdmin(user)).isTrue();
            assertThat(authMetastore.getRole("order-reader")).isPresent();

            List<ProvenanceEvent> events = provenanceStore.queryByPrincipal("user-1", ts, ts + 1);
            assertThat(events).hasSize(1);
        }

        @Test
        @DisplayName("ProvenanceStore key before and AuthMetastore conflict")
        void testKeyPrefixIsolation() throws IOException {
            Principal user = new Principal.UserPrincipal("user-1", "Test User");
            authMetastore.createToken("nbl-test-token", user);

            long ts = System.currentTimeMillis();
            provenanceStore.record(createEvent("user-1", "ACCESS", "db.orders", "TABLE", true, ts));

            ResolvedPermissions perms = authMetastore.resolvePermissions("user-1");
            assertThat(perms).isNotNull();

            List<ProvenanceEvent> events = provenanceStore.queryByTimeRange(ts, ts + 1);
            assertThat(events).hasSize(1);
        }
    }
}
