package io.github.rawwise.benchmark.ycsb;

import com.google.protobuf.ByteString;
import io.github.rawwise.server.BatchPointQueryRequest;
import io.github.rawwise.server.BatchPointQueryResponse;
import io.github.rawwise.server.BatchPointUpdateRequest;
import io.github.rawwise.server.BatchPointUpdateResponse;
import io.github.rawwise.server.ExecutorPointQueryServiceGrpc;
import io.github.rawwise.server.PointDeleteRequest;
import io.github.rawwise.server.PointDeleteResponse;
import io.github.rawwise.server.PointQueryRequest;
import io.github.rawwise.server.PointQueryResponse;
import io.github.rawwise.server.PointQueryResult;
import io.github.rawwise.server.PointUpdateRequest;
import io.github.rawwise.server.PointUpdateResponse;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class GrpcYcsbClient implements YcsbClient {

    private static final Logger LOG = Logger.getLogger(GrpcYcsbClient.class.getName());
    private static final int SCAN_LENGTH = 10;
    private static final int BATCH_SIZE = 500;

    private final String tableId;
    private final ManagedChannel channel;
    private final ExecutorPointQueryServiceGrpc.ExecutorPointQueryServiceBlockingStub stub;

    public GrpcYcsbClient(String host, int port, String tableId) {
        this.tableId = tableId;
        this.channel = NettyChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.stub = ExecutorPointQueryServiceGrpc.newBlockingStub(channel);
    }

    public void init(int recordCount) throws IOException {
        List<ByteString> keyBatch = new ArrayList<>(BATCH_SIZE);
        List<ByteString> valueBatch = new ArrayList<>(BATCH_SIZE);
        for (int i = 0; i < recordCount; i++) {
            keyBatch.add(intToBytes(i));
            valueBatch.add(ByteString.copyFrom(generateValue(i).getBytes(StandardCharsets.UTF_8)));
            if (keyBatch.size() >= BATCH_SIZE) {
                flushBatch(keyBatch, valueBatch);
                keyBatch.clear();
                valueBatch.clear();
            }
        }
        if (!keyBatch.isEmpty()) {
            flushBatch(keyBatch, valueBatch);
        }
        LOG.info("GrpcYcsbClient init: " + recordCount + " records written to " + tableId);
    }

    private void flushBatch(List<ByteString> keys, List<ByteString> values) throws IOException {
        BatchPointUpdateRequest request =
                BatchPointUpdateRequest.newBuilder()
                        .setTableId(tableId)
                        .addAllKeys(keys)
                        .addAllValues(values)
                        .build();
        BatchPointUpdateResponse response = stub.batchPointUpdate(request);
        if (!response.getSuccess()) {
            throw new IOException("Batch init write failed");
        }
    }

    public String read(int key) throws IOException {
        PointQueryRequest request =
                PointQueryRequest.newBuilder().setTableId(tableId).setKey(intToBytes(key)).build();

        PointQueryResponse response = stub.pointQuery(request);
        if (!response.getFound()) {
            return null;
        }
        return response.getData().toString(StandardCharsets.UTF_8);
    }

    public void write(int key) throws IOException {
        String value = generateValue(key);
        PointUpdateRequest request =
                PointUpdateRequest.newBuilder()
                        .setTableId(tableId)
                        .setKey(intToBytes(key))
                        .setData(ByteString.copyFrom(value.getBytes(StandardCharsets.UTF_8)))
                        .build();

        PointUpdateResponse response = stub.pointUpdate(request);
        if (!response.getSuccess()) {
            throw new IOException("Write failed for key=" + key);
        }
    }

    public void insert(int key) throws IOException {
        write(key);
    }

    public void delete(int key) throws IOException {
        PointDeleteRequest request =
                PointDeleteRequest.newBuilder().setTableId(tableId).setKey(intToBytes(key)).build();

        PointDeleteResponse response = stub.pointDelete(request);
        if (!response.getSuccess()) {
            throw new IOException("Delete failed for key=" + key);
        }
    }

    public int scan(int startKey) throws IOException {
        List<ByteString> keys = new ArrayList<>();
        for (int i = 0; i < SCAN_LENGTH; i++) {
            keys.add(intToBytes(startKey + i));
        }

        BatchPointQueryRequest request =
                BatchPointQueryRequest.newBuilder().setTableId(tableId).addAllKeys(keys).build();

        BatchPointQueryResponse response = stub.batchPointQuery(request);
        int count = 0;
        for (PointQueryResult result : response.getResultsList()) {
            if (result.getFound()) {
                count++;
            }
        }
        return count;
    }

    public void close() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static ByteString intToBytes(int value) {
        return ByteString.copyFrom(ByteBuffer.allocate(4).putInt(value).array());
    }

    private static String generateValue(int i) {
        return "field0-"
                + i
                + ",field1-"
                + (i * 2)
                + ",field2-"
                + (i * 3)
                + ",field3-"
                + (i * 4)
                + ",field4-"
                + (i * 5);
    }
}
