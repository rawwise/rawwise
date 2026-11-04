package io.github.rawwise.frontend.config;

import io.github.rawwise.governance.config.ConfigOption;
import io.github.rawwise.governance.config.RawWiseOptions;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class FrontendOptions {

    public static final ConfigOption<String> FRONTEND_DEPLOYMENT_MODE =
            ConfigOption.string("frontend.deployment.mode", "standalone");

    public enum DeploymentMode {
        STANDALONE,
        SIDECAR
    }

    private final int port;
    private final DeploymentMode deploymentMode;
    private final String grpcBackendHost;
    private final int grpcBackendPort;
    private final long queryTimeoutSeconds;
    private final boolean authEnabled;
    private final int authCacheMaxSize;
    private final int authCacheExpireSeconds;
    private final int authTokenCacheExpireSeconds;
    private final boolean authUnauthenticatedDeny;
    private final boolean authAuditEnabled;
    private final boolean contextEnabled;
    private final long contextSessionTtlMs;
    private final int contextSessionHistoryVersions;
    private final long contextSessionCleanupIntervalMs;
    private final int contextKnowledgeMaxEntries;
    private final boolean executionEnabled;
    private final int executionPlanMaxSteps;
    private final int executionRetryDefaultMaxAttempts;
    private final boolean axiomEnabled;
    private final boolean axiomGraphEnabled;
    private final boolean axiomCoordEnabled;
    private final boolean axiomOrchestratorEnabled;
    private final int axiomMaxGraphNodes;
    private final int axiomMaxGraphEdges;
    private final int axiomOrchestratorMaxWorkflowSteps;
    private final int axiomOrchestratorMaxConcurrentInstances;
    private final int axiomOrchestratorMaxConcurrentInstancesPerAgent;
    private final int axiomOrchestratorMaxStepInputBytes;
    private final int axiomOrchestratorMaxStepOutputBytes;
    private final int axiomOrchestratorMaxWorkflowDepth;
    private final int axiomOrchestratorMaxWorkflowFanOut;
    private final long axiomOrchestratorTimeoutCheckIntervalMs;
    private final long axiomOrchestratorRecoveryScanIntervalMs;
    private final int axiomOrchestratorPipelineMaxPendingMessages;
    private final boolean grpcTlsEnabled;
    private final String grpcTlsTruststorePath;
    private final String grpcTlsTruststorePassword;
    private final String grpcTlsTruststoreType;
    private final String grpcTlsKeystorePath;
    private final String grpcTlsKeystorePassword;
    private final String grpcTlsKeystoreType;
    private final boolean grpcMtlsEnabled;
    private final String grpcTlsMinProtocol;
    private final boolean grpcTlsHostnameVerification;
    private final String grpcTlsCipherSuites;
    private final int maxResultRows;
    private final int maxResultBytes;
    private final int maxTrainingResultBytes;
    private final long maxTotalResultBytes;
    private final int routingRefreshSeconds;
    private final String grpcBackendGroups;

    public FrontendOptions(Map<String, Object> config) {
        this.port = getInt(config, RawWiseOptions.FRONTEND_PORT);
        String mode = getString(config, FRONTEND_DEPLOYMENT_MODE);
        this.deploymentMode = parseDeploymentMode(mode);
        this.grpcBackendHost = getString(config, RawWiseOptions.FRONTEND_GRPC_BACKEND_HOST);
        this.grpcBackendPort = getInt(config, RawWiseOptions.FRONTEND_GRPC_BACKEND_PORT);
        this.queryTimeoutSeconds = getLong(config, RawWiseOptions.FRONTEND_QUERY_TIMEOUT_SECONDS);
        this.authEnabled = resolveAuthEnabled(config);
        this.authCacheMaxSize = getInt(config, RawWiseOptions.AUTH_CACHE_MAX_SIZE);
        this.authCacheExpireSeconds = getInt(config, RawWiseOptions.AUTH_CACHE_EXPIRE_SECONDS);
        this.authTokenCacheExpireSeconds =
                getInt(config, RawWiseOptions.AUTH_TOKEN_CACHE_EXPIRE_SECONDS);
        this.authUnauthenticatedDeny = getBoolean(config, RawWiseOptions.AUTH_UNAUTHENTICATED_DENY);
        this.authAuditEnabled = getBoolean(config, RawWiseOptions.AUTH_AUDIT_ENABLED);
        this.contextEnabled = getBoolean(config, RawWiseOptions.CONTEXT_ENABLED);
        this.contextSessionTtlMs = getLong(config, RawWiseOptions.CONTEXT_SESSION_TTL_MS);
        this.contextSessionHistoryVersions =
                getInt(config, RawWiseOptions.CONTEXT_SESSION_HISTORY_VERSIONS);
        this.contextSessionCleanupIntervalMs =
                getLong(config, RawWiseOptions.CONTEXT_SESSION_CLEANUP_INTERVAL_MS);
        this.contextKnowledgeMaxEntries =
                getInt(config, RawWiseOptions.CONTEXT_KNOWLEDGE_MAX_ENTRIES);
        this.executionEnabled = getBoolean(config, RawWiseOptions.EXECUTION_ENABLED);
        this.executionPlanMaxSteps = getInt(config, RawWiseOptions.EXECUTION_PLAN_MAX_STEPS);
        this.executionRetryDefaultMaxAttempts =
                getInt(config, RawWiseOptions.EXECUTION_RETRY_DEFAULT_MAX_ATTEMPTS);
        this.axiomEnabled = getBoolean(config, RawWiseOptions.AXIOM_ENABLED);
        this.axiomGraphEnabled = getBoolean(config, RawWiseOptions.AXIOM_GRAPH_ENABLED);
        this.axiomCoordEnabled = getBoolean(config, RawWiseOptions.AXIOM_ORCHESTRATOR_ENABLED);
        this.axiomOrchestratorEnabled =
                getBoolean(config, RawWiseOptions.AXIOM_ORCHESTRATOR_ENABLED);
        this.axiomMaxGraphNodes = getInt(config, RawWiseOptions.AXIOM_MAX_GRAPH_NODES);
        this.axiomMaxGraphEdges = getInt(config, RawWiseOptions.AXIOM_MAX_GRAPH_EDGES);
        this.axiomOrchestratorMaxWorkflowSteps =
                getInt(config, RawWiseOptions.AXIOM_ORCHESTRATOR_MAX_WORKFLOW_STEPS);
        this.axiomOrchestratorMaxConcurrentInstances =
                getInt(config, RawWiseOptions.AXIOM_ORCHESTRATOR_MAX_CONCURRENT_INSTANCES);
        this.axiomOrchestratorMaxConcurrentInstancesPerAgent =
                getInt(
                        config,
                        RawWiseOptions.AXIOM_ORCHESTRATOR_MAX_CONCURRENT_INSTANCES_PER_AGENT);
        this.axiomOrchestratorMaxStepInputBytes =
                getInt(config, RawWiseOptions.AXIOM_ORCHESTRATOR_MAX_STEP_INPUT_BYTES);
        this.axiomOrchestratorMaxStepOutputBytes =
                getInt(config, RawWiseOptions.AXIOM_ORCHESTRATOR_MAX_STEP_OUTPUT_BYTES);
        this.axiomOrchestratorMaxWorkflowDepth =
                getInt(config, RawWiseOptions.AXIOM_ORCHESTRATOR_MAX_WORKFLOW_DEPTH);
        this.axiomOrchestratorMaxWorkflowFanOut =
                getInt(config, RawWiseOptions.AXIOM_ORCHESTRATOR_MAX_WORKFLOW_FAN_OUT);
        this.axiomOrchestratorTimeoutCheckIntervalMs =
                getLong(config, RawWiseOptions.AXIOM_ORCHESTRATOR_TIMEOUT_CHECK_INTERVAL_MS);
        this.axiomOrchestratorRecoveryScanIntervalMs =
                getLong(config, RawWiseOptions.AXIOM_ORCHESTRATOR_RECOVERY_SCAN_INTERVAL_MS);
        this.axiomOrchestratorPipelineMaxPendingMessages =
                getInt(config, RawWiseOptions.AXIOM_ORCHESTRATOR_PIPELINE_MAX_PENDING_MESSAGES);
        this.grpcTlsEnabled = getBoolean(config, RawWiseOptions.FRONTEND_GRPC_TLS_ENABLED);
        this.grpcTlsTruststorePath =
                resolveSecret(getString(config, RawWiseOptions.FRONTEND_GRPC_TLS_TRUSTSTORE_PATH));
        this.grpcTlsTruststorePassword =
                resolveSecret(
                        getString(config, RawWiseOptions.FRONTEND_GRPC_TLS_TRUSTSTORE_PASSWORD));
        this.grpcTlsTruststoreType =
                getString(config, RawWiseOptions.FRONTEND_GRPC_TLS_TRUSTSTORE_TYPE);
        this.grpcTlsKeystorePath =
                resolveSecret(getString(config, RawWiseOptions.FRONTEND_GRPC_TLS_KEYSTORE_PATH));
        this.grpcTlsKeystorePassword =
                resolveSecret(
                        getString(config, RawWiseOptions.FRONTEND_GRPC_TLS_KEYSTORE_PASSWORD));
        this.grpcTlsKeystoreType =
                getString(config, RawWiseOptions.FRONTEND_GRPC_TLS_KEYSTORE_TYPE);
        this.grpcMtlsEnabled = getBoolean(config, RawWiseOptions.FRONTEND_GRPC_MTLS_ENABLED);
        this.grpcTlsMinProtocol = getString(config, RawWiseOptions.FRONTEND_GRPC_TLS_MIN_PROTOCOL);
        this.grpcTlsHostnameVerification =
                getBoolean(config, RawWiseOptions.FRONTEND_GRPC_TLS_HOSTNAME_VERIFICATION);
        this.grpcTlsCipherSuites =
                getString(config, RawWiseOptions.FRONTEND_GRPC_TLS_CIPHER_SUITES);
        this.maxResultRows = getInt(config, RawWiseOptions.FRONTEND_MAX_RESULT_ROWS);
        this.maxResultBytes = getInt(config, RawWiseOptions.FRONTEND_MAX_RESULT_BYTES);
        this.maxTrainingResultBytes =
                getInt(config, RawWiseOptions.FRONTEND_MAX_TRAINING_RESULT_BYTES);
        this.maxTotalResultBytes = getLong(config, RawWiseOptions.FRONTEND_MAX_TOTAL_RESULT_BYTES);
        this.routingRefreshSeconds =
                getInt(config, RawWiseOptions.FRONTEND_ROUTING_REFRESH_SECONDS);
        this.grpcBackendGroups = getString(config, RawWiseOptions.FRONTEND_GRPC_BACKEND_GROUPS);

        if (contextEnabled && !authEnabled) {
            log.warn(
                    "context.enabled=true with auth.enabled=false; "
                            + "AuthFilter will inject anonymous ADMIN principal for dev mode");
        }
        if (executionEnabled && !contextEnabled) {
            throw new IllegalArgumentException(
                    "execution.enabled requires context.enabled to be true");
        }
        if (axiomOrchestratorEnabled && !axiomEnabled) {
            log.warn(
                    "axiom.orchestrator.enabled=true with axiom.enabled=false; orchestrator will not be activated");
        }
    }

    private boolean resolveAuthEnabled(Map<String, Object> config) {
        boolean configured = getBoolean(config, RawWiseOptions.AUTH_ENABLED);
        if (deploymentMode == DeploymentMode.STANDALONE) {
            if (!configured) {
                boolean explicitlyDisabled =
                        Boolean.parseBoolean(
                                System.getProperty(
                                        "rawwise.auth.explicitly-unsafe-disable", "false"));
                if (!explicitlyDisabled) {
                    log.warn(
                            "STANDALONE mode requires auth.enabled=true; overriding to true for safety");
                    return true;
                }
                log.warn("╔══════════════════════════════════════════════════════╗");
                log.warn("║  AUTHENTICATION EXPLICITLY DISABLED IN STANDALONE   ║");
                log.warn("║  THIS IS UNSAFE - DO NOT USE IN PRODUCTION          ║");
                log.warn("╚══════════════════════════════════════════════════════╝");
            }
        }
        return configured;
    }

    private String resolveSecret(String value) {
        if (value != null && value.startsWith("${env:")) {
            if (!value.endsWith("}")) {
                throw new IllegalArgumentException("Invalid secret reference format: " + value);
            }
            String envVar = value.substring(6, value.length() - 1);
            if (!envVar.startsWith("RAWWISE_SECRET_")) {
                throw new IllegalArgumentException(
                        "Secret environment variable must start with 'RAWWISE_SECRET_', got: "
                                + envVar);
            }
            String resolved = System.getenv(envVar);
            if (resolved == null) {
                throw new IllegalArgumentException(
                        "Secret environment variable not found: " + envVar);
            }
            return resolved;
        }
        if (value != null && !value.isEmpty()) {
            log.warn(
                    "Secret value provided directly without environment variable reference - use ${env:RAWWISE_SECRET_XXX} for production deployments");
        }
        return value;
    }

    private DeploymentMode parseDeploymentMode(String mode) {
        try {
            return DeploymentMode.valueOf(mode.toUpperCase());
        } catch (IllegalArgumentException e) {
            log.warn("Invalid deployment mode '{}', using STANDALONE", mode);
            return DeploymentMode.STANDALONE;
        }
    }

    private boolean getBoolean(Map<String, Object> config, ConfigOption<Boolean> option) {
        Object value = config.get(option.key());
        if (value == null) {
            value = config.get("rawwise." + option.key());
        }
        if (value == null) {
            return option.defaultValue();
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return Boolean.parseBoolean(value.toString());
    }

    private int getInt(Map<String, Object> config, ConfigOption<Integer> option) {
        Object value = config.get(option.key());
        if (value == null) {
            value = config.get("rawwise." + option.key());
        }
        if (value == null) {
            return option.defaultValue();
        }
        if (value instanceof Integer) {
            return (Integer) value;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return option.defaultValue();
    }

    private long getLong(Map<String, Object> config, ConfigOption<Long> option) {
        Object value = config.get(option.key());
        if (value == null) {
            value = config.get("rawwise." + option.key());
        }
        if (value == null) {
            return option.defaultValue();
        }
        if (value instanceof Long) {
            return (Long) value;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return option.defaultValue();
    }

    private String getString(Map<String, Object> config, ConfigOption<String> option) {
        Object value = config.get(option.key());
        if (value == null) {
            value = config.get("rawwise." + option.key());
        }
        if (value == null) {
            return option.defaultValue();
        }
        return value.toString();
    }
}
