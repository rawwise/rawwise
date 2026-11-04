package io.github.rawwise.frontend.ir.routing;

import io.github.rawwise.frontend.backend.BackendEndpoint;
import io.github.rawwise.metastore.KeyValue;
import io.github.rawwise.metastore.Metastore;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RoutingTableManager {

    private final Metastore metastore;
    private final Map<String, RoutingTable> cache = new ConcurrentHashMap<>();
    private final ScheduledExecutorService refreshExecutor;

    public RoutingTableManager(Metastore metastore, long refreshIntervalSeconds) {
        this.metastore = metastore;
        if (refreshIntervalSeconds > 0) {
            this.refreshExecutor =
                    Executors.newSingleThreadScheduledExecutor(
                            r -> {
                                var t = new Thread(r, "routing-table-refresh");
                                t.setDaemon(true);
                                return t;
                            });
            refreshExecutor.scheduleWithFixedDelay(
                    this::refreshAll,
                    refreshIntervalSeconds,
                    refreshIntervalSeconds,
                    TimeUnit.SECONDS);
        } else {
            this.refreshExecutor = null;
        }
    }

    public RoutingTable getRoutingTable(String tableId) {
        RoutingTable cached = cache.get(tableId);
        if (cached != null) return cached;
        return load(tableId);
    }

    public void refreshAll() {
        for (String tableId : cache.keySet()) {
            try {
                byte[] currentVersion = metastore.get(RoutingKeys.versionKey(tableId)).orElse(null);
                if (currentVersion == null) continue;
                long remoteVersion = RoutingKeys.bytesToLong(currentVersion);
                RoutingTable cached = cache.get(tableId);
                if (cached != null && remoteVersion > cached.version()) {
                    cache.remove(tableId);
                }
            } catch (IOException e) {
                // refresh failure should not crash; stale cache is acceptable
            }
        }
    }

    public void shutdown() {
        if (refreshExecutor != null) {
            refreshExecutor.shutdown();
        }
    }

    private RoutingTable load(String tableId) {
        try {
            byte[] versionBytes = metastore.get(RoutingKeys.versionKey(tableId)).orElse(null);
            if (versionBytes == null) return null;

            long version = RoutingKeys.bytesToLong(versionBytes);

            List<RoutingEntry> entries = new ArrayList<>();
            try (var it = metastore.scan(RoutingKeys.entryScanPrefix(tableId))) {
                while (it.hasNext()) {
                    KeyValue kv = it.next();
                    byte[] v = kv.value();
                    long start = RoutingKeys.bytesToLong(v, 0);
                    long end = RoutingKeys.bytesToLong(v, 8);
                    int groupIdLen = RoutingKeys.bytesToInt(v, 16);
                    String groupId =
                            new String(v, 20, groupIdLen, java.nio.charset.StandardCharsets.UTF_8);
                    entries.add(new RoutingEntry(new KeyRange(start, end), groupId));
                }
            }

            byte[] defaultBytes =
                    metastore
                            .get(RoutingKeys.defaultKey(tableId))
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "Routing table missing default backend"));
            byte[] db = defaultBytes;
            int gl = RoutingKeys.bytesToInt(db, 0);
            String gId = new String(db, 4, gl, java.nio.charset.StandardCharsets.UTF_8);
            int hl = RoutingKeys.bytesToInt(db, 4 + gl);
            String h = new String(db, 4 + gl + 4, hl, java.nio.charset.StandardCharsets.UTF_8);
            int p = RoutingKeys.bytesToInt(db, 4 + gl + 4 + hl);
            BackendEndpoint defaultBackend = new BackendEndpoint(gId, h, p);

            RoutingTable rt = new RoutingTable(tableId, version, entries, defaultBackend);
            cache.put(tableId, rt);
            return rt;
        } catch (IOException e) {
            throw new RuntimeException("Failed to load routing table for " + tableId, e);
        }
    }
}
