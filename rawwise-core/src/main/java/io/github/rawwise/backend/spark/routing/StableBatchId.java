package io.github.rawwise.backend.spark.routing;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.Map;

public final class StableBatchId {
    private StableBatchId() {}

    public static String forPendingBatches(
            String tableIdentifier,
            int partitionId,
            long flushSequence,
            Map<Integer, ? extends BatchLike> pending) {
        MessageDigest digest = newSha256();
        updateString(digest, tableIdentifier);
        updateInt(digest, partitionId);
        updateLong(digest, flushSequence);
        pending.entrySet().stream()
                .sorted(Map.Entry.comparingByKey(Comparator.naturalOrder()))
                .forEach(
                        entry -> {
                            updateInt(digest, entry.getKey());
                            BatchLike batch = entry.getValue();
                            updateInt(digest, batch.size());
                            for (int i = 0; i < batch.size(); i++) {
                                updateBytes(digest, batch.keyAt(i));
                                updateNullableBytes(digest, batch.valueAt(i));
                            }
                        });
        return "nb-" + partitionId + "-" + flushSequence + "-" + toHex(digest.digest());
    }

    public static String forRecords(
            String tableIdentifier,
            int partitionId,
            long flushSequence,
            int[] shardIds,
            byte[][] keys,
            byte[][] values) {
        if (shardIds.length != keys.length || shardIds.length != values.length) {
            throw new IllegalArgumentException("shardIds/keys/values length mismatch");
        }
        MessageDigest digest = newSha256();
        updateString(digest, tableIdentifier);
        updateInt(digest, partitionId);
        updateLong(digest, flushSequence);
        for (int i = 0; i < shardIds.length; i++) {
            updateInt(digest, shardIds[i]);
            updateBytes(digest, keys[i]);
            updateNullableBytes(digest, values[i]);
        }
        return "nb-" + partitionId + "-" + flushSequence + "-" + toHex(digest.digest());
    }

    public interface BatchLike {
        int size();

        byte[] keyAt(int index);

        byte[] valueAt(int index);
    }

    private static MessageDigest newSha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static void updateString(MessageDigest digest, String value) {
        updateBytes(digest, value == null ? null : value.getBytes(StandardCharsets.UTF_8));
    }

    private static void updateInt(MessageDigest digest, int value) {
        digest.update((byte) (value >>> 24));
        digest.update((byte) (value >>> 16));
        digest.update((byte) (value >>> 8));
        digest.update((byte) value);
    }

    private static void updateLong(MessageDigest digest, long value) {
        digest.update((byte) (value >>> 56));
        digest.update((byte) (value >>> 48));
        digest.update((byte) (value >>> 40));
        digest.update((byte) (value >>> 32));
        digest.update((byte) (value >>> 24));
        digest.update((byte) (value >>> 16));
        digest.update((byte) (value >>> 8));
        digest.update((byte) value);
    }

    private static void updateBytes(MessageDigest digest, byte[] value) {
        if (value == null) {
            updateInt(digest, -1);
            return;
        }
        updateInt(digest, value.length);
        digest.update(value);
    }

    private static void updateNullableBytes(MessageDigest digest, byte[] value) {
        digest.update((byte) (value == null ? 0 : 1));
        updateBytes(digest, value);
    }

    private static String toHex(byte[] bytes) {
        StringBuilder builder = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            builder.append(Character.forDigit((b >>> 4) & 0xF, 16));
            builder.append(Character.forDigit(b & 0xF, 16));
        }
        return builder.toString();
    }
}
