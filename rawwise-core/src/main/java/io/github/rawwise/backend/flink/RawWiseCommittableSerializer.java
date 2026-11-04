package io.github.rawwise.backend.flink;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public class RawWiseCommittableSerializer implements SimpleVersionedSerializer<RawWiseCommittable> {

    static final int V1 = 1;
    static final int V2 = 2;
    private static final int CURRENT_VERSION = V2;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(RawWiseCommittable c) throws IOException {
        var baos = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(baos)) {
            out.writeUTF(c.tableId());
            out.writeInt(c.pendingIds().size());
            for (String id : c.pendingIds()) {
                out.writeUTF(id);
            }
            out.writeLong(c.txnId());
            out.writeLong(c.checkpointId());
            out.writeLong(c.schemaVersion());
            out.writeLong(c.minSeq());
            out.writeLong(c.maxSeq());
            out.writeLong(c.checkpointTs());
        }
        return baos.toByteArray();
    }

    @Override
    public RawWiseCommittable deserialize(int version, byte[] serialized) throws IOException {
        if (version < 1 || version > CURRENT_VERSION) {
            throw new IOException(
                    "Unsupported serializer version: "
                            + version
                            + ", current version: "
                            + CURRENT_VERSION);
        }
        try (var in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            String tableId = in.readUTF();
            int size = in.readInt();
            List<String> pendingIds = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                pendingIds.add(in.readUTF());
            }
            long txnId = in.readLong();
            long checkpointId = in.readLong();
            long schemaVersion = in.readLong();

            long minSeq = Long.MAX_VALUE;
            long maxSeq = Long.MIN_VALUE;
            long checkpointTs = 0L;

            if (version >= V2) {
                minSeq = in.readLong();
                maxSeq = in.readLong();
                checkpointTs = in.readLong();
            }

            return new RawWiseCommittable(
                    tableId,
                    pendingIds,
                    txnId,
                    checkpointId,
                    schemaVersion,
                    minSeq,
                    maxSeq,
                    checkpointTs);
        }
    }
}
