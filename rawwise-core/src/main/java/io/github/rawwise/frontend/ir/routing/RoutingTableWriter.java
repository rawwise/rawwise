package io.github.rawwise.frontend.ir.routing;

import io.github.rawwise.frontend.backend.BackendEndpoint;
import io.github.rawwise.metastore.KeyValue;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class RoutingTableWriter {

    private static final int MAX_CAS_RETRIES = 100;

    private final Metastore store;

    public RoutingTableWriter(Metastore store) {
        this.store = store;
    }

    public boolean initialize(
            String tableId, List<RoutingEntry> entries, BackendEndpoint defaultBackend)
            throws IOException {
        byte[] versionKey = RoutingKeys.versionKey(tableId);
        byte[] defaultKey = RoutingKeys.defaultKey(tableId);

        List<Precondition> preconditions = new ArrayList<>();
        preconditions.add(new Precondition.NotExists(versionKey));

        List<Mutation> mutations = new ArrayList<>();
        mutations.add(new Mutation.Put(versionKey, RoutingKeys.longToBytes(1L)));
        mutations.add(new Mutation.Put(defaultKey, encodeEndpoint(defaultBackend)));

        for (int i = 0; i < entries.size(); i++) {
            byte[] entryKey = RoutingKeys.entryKey(tableId, i);
            mutations.add(new Mutation.Put(entryKey, encodeEntry(entries.get(i))));
        }

        return store.compareAndSwap(preconditions, mutations);
    }

    public boolean addEntry(String tableId, RoutingEntry newEntry) throws IOException {
        for (int retry = 0; retry < MAX_CAS_RETRIES; retry++) {
            long currentVersion = readVersion(tableId);
            byte[] versionKey = RoutingKeys.versionKey(tableId);

            List<KeyValue> existingEntries = new ArrayList<>();
            try (var it = store.scan(RoutingKeys.entryScanPrefix(tableId))) {
                while (it.hasNext()) {
                    existingEntries.add(it.next());
                }
            }
            int newSeq = existingEntries.size();
            byte[] newEntryKey = RoutingKeys.entryKey(tableId, newSeq);

            List<Precondition> preconditions = new ArrayList<>();
            preconditions.add(
                    new Precondition.ValueEquals(
                            versionKey, RoutingKeys.longToBytes(currentVersion)));
            preconditions.add(new Precondition.NotExists(newEntryKey));

            List<Mutation> mutations = new ArrayList<>();
            mutations.add(
                    new Mutation.Put(versionKey, RoutingKeys.longToBytes(currentVersion + 1)));
            mutations.add(new Mutation.Put(newEntryKey, encodeEntry(newEntry)));

            if (store.compareAndSwap(preconditions, mutations)) {
                return true;
            }
        }
        throw new IOException("Failed to add routing entry after " + MAX_CAS_RETRIES + " retries");
    }

    public boolean updateEntry(String tableId, int seq, RoutingEntry updatedEntry)
            throws IOException {
        byte[] entryKey = RoutingKeys.entryKey(tableId, seq);

        for (int retry = 0; retry < MAX_CAS_RETRIES; retry++) {
            long currentVersion = readVersion(tableId);
            byte[] versionKey = RoutingKeys.versionKey(tableId);

            Optional<byte[]> currentBytes = store.get(entryKey);
            if (currentBytes.isEmpty()) {
                return false;
            }

            List<Precondition> preconditions =
                    List.of(
                            new Precondition.ValueEquals(
                                    versionKey, RoutingKeys.longToBytes(currentVersion)),
                            new Precondition.ValueEquals(entryKey, currentBytes.get()));

            List<Mutation> mutations =
                    List.of(
                            new Mutation.Put(
                                    versionKey, RoutingKeys.longToBytes(currentVersion + 1)),
                            new Mutation.Put(entryKey, encodeEntry(updatedEntry)));

            if (store.compareAndSwap(preconditions, mutations)) {
                return true;
            }
        }
        throw new IOException(
                "Failed to update routing entry after " + MAX_CAS_RETRIES + " retries");
    }

    public boolean removeEntry(String tableId, int seq) throws IOException {
        byte[] entryKey = RoutingKeys.entryKey(tableId, seq);

        for (int retry = 0; retry < MAX_CAS_RETRIES; retry++) {
            long currentVersion = readVersion(tableId);
            byte[] versionKey = RoutingKeys.versionKey(tableId);

            Optional<byte[]> currentEntry = store.get(entryKey);
            if (currentEntry.isEmpty()) {
                return false;
            }

            List<Precondition> preconditions =
                    List.of(
                            new Precondition.ValueEquals(
                                    versionKey, RoutingKeys.longToBytes(currentVersion)),
                            new Precondition.Exists(entryKey));

            List<Mutation> mutations =
                    List.of(
                            new Mutation.Put(
                                    versionKey, RoutingKeys.longToBytes(currentVersion + 1)),
                            new Mutation.Delete(entryKey));

            if (store.compareAndSwap(preconditions, mutations)) {
                return true;
            }
        }
        throw new IOException(
                "Failed to remove routing entry after " + MAX_CAS_RETRIES + " retries");
    }

    public boolean updateDefaultBackend(String tableId, BackendEndpoint newDefault)
            throws IOException {
        byte[] defaultKey = RoutingKeys.defaultKey(tableId);

        for (int retry = 0; retry < MAX_CAS_RETRIES; retry++) {
            long currentVersion = readVersion(tableId);
            byte[] versionKey = RoutingKeys.versionKey(tableId);

            Optional<byte[]> currentDefault = store.get(defaultKey);

            List<Precondition> preconditions = new ArrayList<>();
            preconditions.add(
                    new Precondition.ValueEquals(
                            versionKey, RoutingKeys.longToBytes(currentVersion)));
            currentDefault.ifPresent(
                    bytes -> preconditions.add(new Precondition.ValueEquals(defaultKey, bytes)));

            List<Mutation> mutations =
                    List.of(
                            new Mutation.Put(
                                    versionKey, RoutingKeys.longToBytes(currentVersion + 1)),
                            new Mutation.Put(defaultKey, encodeEndpoint(newDefault)));

            if (store.compareAndSwap(preconditions, mutations)) {
                return true;
            }
        }
        throw new IOException(
                "Failed to update default backend after " + MAX_CAS_RETRIES + " retries");
    }

    public void dropRoutingTable(String tableId) throws IOException {
        byte[] versionKey = RoutingKeys.versionKey(tableId);

        for (int retry = 0; retry < MAX_CAS_RETRIES; retry++) {
            Optional<byte[]> current = store.get(versionKey);
            if (current.isEmpty()) return;

            boolean ok =
                    store.compareAndSwap(
                            List.of(new Precondition.ValueEquals(versionKey, current.get())),
                            List.of(new Mutation.Delete(versionKey)));
            if (ok) break;
        }

        List<Mutation> cleanMutations = new ArrayList<>();
        cleanMutations.add(new Mutation.Delete(RoutingKeys.defaultKey(tableId)));
        try (var it = store.scan(RoutingKeys.entryScanPrefix(tableId))) {
            while (it.hasNext()) {
                cleanMutations.add(new Mutation.Delete(it.next().key()));
            }
        }
        store.batchWrite(cleanMutations);
    }

    private long readVersion(String tableId) throws IOException {
        return store.get(RoutingKeys.versionKey(tableId)).map(RoutingKeys::bytesToLong).orElse(0L);
    }

    private static byte[] encodeEndpoint(BackendEndpoint ep) {
        byte[] groupId = ep.groupId().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] host = ep.host().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] encoded = new byte[4 + groupId.length + 4 + host.length + 4];
        int pos = 0;
        System.arraycopy(RoutingKeys.intToBytes(groupId.length), 0, encoded, pos, 4);
        pos += 4;
        System.arraycopy(groupId, 0, encoded, pos, groupId.length);
        pos += groupId.length;
        System.arraycopy(RoutingKeys.intToBytes(host.length), 0, encoded, pos, 4);
        pos += 4;
        System.arraycopy(host, 0, encoded, pos, host.length);
        pos += host.length;
        System.arraycopy(RoutingKeys.intToBytes(ep.port()), 0, encoded, pos, 4);
        return encoded;
    }

    private static byte[] encodeEntry(RoutingEntry entry) {
        byte[] backendGroupId =
                entry.backendGroupId().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] encoded = new byte[8 + 8 + 4 + backendGroupId.length];
        int pos = 0;
        System.arraycopy(
                RoutingKeys.longToBytes(entry.keyRange().startInclusive()), 0, encoded, pos, 8);
        pos += 8;
        System.arraycopy(
                RoutingKeys.longToBytes(entry.keyRange().endExclusive()), 0, encoded, pos, 8);
        pos += 8;
        System.arraycopy(RoutingKeys.intToBytes(backendGroupId.length), 0, encoded, pos, 4);
        pos += 4;
        System.arraycopy(backendGroupId, 0, encoded, pos, backendGroupId.length);
        return encoded;
    }
}
