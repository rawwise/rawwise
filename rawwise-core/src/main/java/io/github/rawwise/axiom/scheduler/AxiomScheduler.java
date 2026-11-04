package io.github.rawwise.axiom.scheduler;

import io.github.rawwise.axiom.memory.MemoryFacade;
import io.github.rawwise.axiom.memory.MemoryTier;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AxiomScheduler implements AutoCloseable {

    private static final Duration DEFAULT_DECAY_AGE = Duration.ofMillis(86_400_000L);
    private static final MemoryTier CONSOL_FROM_TIER = MemoryTier.WORKING;
    private static final MemoryTier CONSOL_TO_TIER = MemoryTier.EPISODIC;

    private final MemoryFacade memoryFacade;
    private final AxiomSchedulerConfig config;
    private final ScheduledExecutorService executor;
    private volatile boolean started = false;

    public AxiomScheduler(MemoryFacade memoryFacade, AxiomSchedulerConfig config) {
        this.memoryFacade = memoryFacade;
        this.config = config;
        int parallelism = Math.max(1, config.parallelism());
        AtomicInteger seq = new AtomicInteger(0);
        ScheduledThreadPoolExecutor pool =
                new ScheduledThreadPoolExecutor(
                        parallelism,
                        r -> {
                            Thread t =
                                    new Thread(
                                            r,
                                            String.format(
                                                    "axiom-scheduler-%d", seq.getAndIncrement()));
                            t.setDaemon(true);
                            return t;
                        });
        pool.setRemoveOnCancelPolicy(true);
        this.executor = pool;
    }

    public void start() {
        if (!config.enabled()) {
            log.info("AxiomScheduler disabled (enabled=false), skipping start");
            return;
        }
        if (!memoryFacade.supportsScheduledLifecycle()) {
            log.warn(
                    "AxiomScheduler enabled but memory facade has no-op decay/consolidate "
                            + "(no local ViewMetadataStore); skipping scheduled lifecycle");
            return;
        }
        if (started) {
            log.warn("AxiomScheduler already started, ignoring start()");
            return;
        }
        started = true;

        if (config.warmupOnStart()) {
            log.info("AxiomScheduler warmup-on-start: running initial tick");
            if (config.decayEnabled()) runDecayCycle();
            if (config.consolidateEnabled()) runConsolidateCycle();
        }

        if (config.decayEnabled()) {
            executor.scheduleAtFixedRate(
                    this::runDecayCycle,
                    config.decayIntervalMs(),
                    config.decayIntervalMs(),
                    TimeUnit.MILLISECONDS);
        }
        if (config.consolidateEnabled()) {
            executor.scheduleAtFixedRate(
                    this::runConsolidateCycle,
                    config.consolidateIntervalMs(),
                    config.consolidateIntervalMs(),
                    TimeUnit.MILLISECONDS);
        }

        log.info(
                "AxiomScheduler started: parallelism={}, agents={}, decay={}ms (enabled={}), consolidate={}ms (enabled={}), warmup={}",
                config.parallelism(),
                config.agents(),
                config.decayIntervalMs(),
                config.decayEnabled(),
                config.consolidateIntervalMs(),
                config.consolidateEnabled(),
                config.warmupOnStart());
    }

    private void runDecayCycle() {
        List<String> agents = config.agents();
        if (agents == null || agents.isEmpty()) {
            log.debug("Decay cycle skipped: no agents configured");
            return;
        }
        for (String agentId : agents) {
            try {
                memoryFacade.decay(agentId, DEFAULT_DECAY_AGE);
                log.debug("Decay tick complete agent={}", agentId);
            } catch (Exception e) {
                log.warn("Decay failed for agent {}", agentId, e);
            }
        }
    }

    private void runConsolidateCycle() {
        List<String> agents = config.agents();
        if (agents == null || agents.isEmpty()) {
            log.debug("Consolidate cycle skipped: no agents configured");
            return;
        }
        for (String agentId : agents) {
            try {
                memoryFacade.consolidate(agentId, CONSOL_FROM_TIER, CONSOL_TO_TIER);
                log.debug("Consolidate tick complete agent={}", agentId);
            } catch (Exception e) {
                log.warn("Consolidation failed for agent {}", agentId, e);
            }
        }
    }

    @Override
    public void close() {
        executor.shutdown();
        try {
            long timeoutSec = Math.max(1L, config.shutdownTimeoutSec());
            if (!executor.awaitTermination(timeoutSec, TimeUnit.SECONDS)) {
                log.warn(
                        "AxiomScheduler did not terminate gracefully within {}s, forcing shutdown",
                        timeoutSec);
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("AxiomScheduler stopped");
    }
}
