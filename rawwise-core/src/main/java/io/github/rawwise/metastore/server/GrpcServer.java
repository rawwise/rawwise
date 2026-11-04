package io.github.rawwise.metastore.server;

import com.google.protobuf.ByteString;
import io.github.rawwise.common.catalog.Catalog;
import io.github.rawwise.common.exception.TableNotFoundException;
import io.github.rawwise.meta.*;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import io.github.rawwise.model.record.ProtoTableMetaConverter;
import io.github.rawwise.model.record.RawWiseTable;
import io.github.rawwise.model.record.TableId;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GrpcServer {

    private final Server grpcServer;
    private final Catalog catalog;

    public GrpcServer(int port, Catalog catalog, Metastore metastore) {
        this.catalog = catalog;
        this.grpcServer =
                ServerBuilder.forPort(port)
                        .maxInboundMessageSize(16 * 1024 * 1024)
                        .addService(new MetastoreGrpcServiceImpl(catalog, metastore))
                        .build();
    }

    public void start() throws IOException {
        grpcServer.start();
        log.info("[gRPC] Metastore server started on port {}", grpcServer.getPort());
    }

    public void blockUntilShutdown() throws InterruptedException {
        grpcServer.awaitTermination();
    }

    public void shutdown() {
        grpcServer.shutdown();
        try {
            if (!grpcServer.awaitTermination(5, TimeUnit.SECONDS)) {
                grpcServer.shutdownNow();
            }
        } catch (InterruptedException e) {
            grpcServer.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("[gRPC] Server shutdown complete");
    }

    @Slf4j
    static class MetastoreGrpcServiceImpl extends MetastoreServiceGrpc.MetastoreServiceImplBase {

        private final Catalog catalog;
        private final Metastore metastore;

        MetastoreGrpcServiceImpl(Catalog catalog, Metastore metastore) {
            this.catalog = catalog;
            this.metastore = metastore;
        }

        // ── KV Operations ──

        @Override
        public void kvGet(KvGetRequest request, StreamObserver<KvGetResponse> responseObserver) {
            try {
                byte[] key = request.getKey().toByteArray();
                Optional<byte[]> value = metastore.get(key);
                responseObserver.onNext(
                        KvGetResponse.newBuilder()
                                .setFound(value.isPresent())
                                .setValue(
                                        value.isPresent()
                                                ? ByteString.copyFrom(value.get())
                                                : ByteString.EMPTY)
                                .build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                log.error("[gRPC] kvGet failed", e);
                responseObserver.onError(e);
            }
        }

        @Override
        public void kvPut(KvPutRequest request, StreamObserver<KvPutResponse> responseObserver) {
            try {
                metastore.put(request.getKey().toByteArray(), request.getValue().toByteArray());
                responseObserver.onNext(KvPutResponse.getDefaultInstance());
                responseObserver.onCompleted();
            } catch (Exception e) {
                log.error("[gRPC] kvPut failed", e);
                responseObserver.onError(e);
            }
        }

        @Override
        public void kvDelete(
                KvDeleteRequest request, StreamObserver<KvDeleteResponse> responseObserver) {
            try {
                metastore.delete(request.getKey().toByteArray());
                responseObserver.onNext(KvDeleteResponse.getDefaultInstance());
                responseObserver.onCompleted();
            } catch (Exception e) {
                log.error("[gRPC] kvDelete failed", e);
                responseObserver.onError(e);
            }
        }

        @Override
        public void kvExists(
                KvExistsRequest request, StreamObserver<KvExistsResponse> responseObserver) {
            try {
                boolean exists = metastore.exists(request.getKey().toByteArray());
                responseObserver.onNext(KvExistsResponse.newBuilder().setExists(exists).build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                log.error("[gRPC] kvExists failed", e);
                responseObserver.onError(e);
            }
        }

        @Override
        public void kvCompareAndSwap(
                io.github.rawwise.meta.KvCasRequest request,
                StreamObserver<io.github.rawwise.meta.KvCasResponse> responseObserver) {
            try {
                List<Precondition> preconditions = new ArrayList<>();
                for (PbPrecondition p : request.getPreconditionsList()) {
                    byte[] key = p.getKey().toByteArray();
                    if (p.getKind() == io.github.rawwise.meta.PbPreconditionKind.EXISTS) {
                        preconditions.add(new Precondition.Exists(key));
                    } else if (p.getKind()
                            == io.github.rawwise.meta.PbPreconditionKind.NOT_EXISTS) {
                        preconditions.add(new Precondition.NotExists(key));
                    } else if (p.getKind()
                            == io.github.rawwise.meta.PbPreconditionKind.VALUE_EQUALS) {
                        preconditions.add(
                                new Precondition.ValueEquals(
                                        key, p.getExpectedValue().toByteArray()));
                    }
                }
                List<Mutation> mutations = new ArrayList<>();
                for (PbMutation m : request.getMutationsList()) {
                    byte[] key = m.getKey().toByteArray();
                    if (m.getKind() == io.github.rawwise.meta.PbMutationKind.PUT) {
                        mutations.add(new Mutation.Put(key, m.getValue().toByteArray()));
                    } else if (m.getKind() == io.github.rawwise.meta.PbMutationKind.DELETE) {
                        mutations.add(new Mutation.Delete(key));
                    }
                }
                boolean success = metastore.compareAndSwap(preconditions, mutations);
                responseObserver.onNext(
                        io.github.rawwise.meta.KvCasResponse.newBuilder()
                                .setSuccess(success)
                                .build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                log.error("[gRPC] kvCompareAndSwap failed", e);
                responseObserver.onError(e);
            }
        }

        @Override
        public void kvScan(KvScanRequest request, StreamObserver<KvEntry> responseObserver) {
            try {
                byte[] prefix = request.getPrefix().toByteArray();
                int limit = request.hasLimit() ? request.getLimit() : -1;
                try (var it = limit > 0 ? metastore.scan(prefix, limit) : metastore.scan(prefix)) {
                    while (it.hasNext()) {
                        var kv = it.next();
                        responseObserver.onNext(
                                KvEntry.newBuilder()
                                        .setKey(ByteString.copyFrom(kv.key()))
                                        .setValue(ByteString.copyFrom(kv.value()))
                                        .build());
                    }
                }
                responseObserver.onCompleted();
            } catch (Exception e) {
                log.error("[gRPC] kvScan failed", e);
                responseObserver.onError(e);
            }
        }

        @Override
        public void kvScanRange(
                KvScanRangeRequest request, StreamObserver<KvEntry> responseObserver) {
            try {
                byte[] start = request.getStartInclusive().toByteArray();
                byte[] end = request.getEndExclusive().toByteArray();
                int limit = request.hasLimit() ? request.getLimit() : -1;
                try (var it =
                        limit > 0
                                ? metastore.scanRange(start, end, limit)
                                : metastore.scanRange(start, end)) {
                    while (it.hasNext()) {
                        var kv = it.next();
                        responseObserver.onNext(
                                KvEntry.newBuilder()
                                        .setKey(ByteString.copyFrom(kv.key()))
                                        .setValue(ByteString.copyFrom(kv.value()))
                                        .build());
                    }
                }
                responseObserver.onCompleted();
            } catch (Exception e) {
                log.error("[gRPC] kvScanRange failed", e);
                responseObserver.onError(e);
            }
        }

        @Override
        public void createTable(
                PbCreateTableRequest request, StreamObserver<PbOperationResult> responseObserver) {
            try {
                var identifier = request.getMetadata().getIdentifier();
                TableId id = TableId.parse(identifier.getTable());
                var schema =
                        ProtoTableMetaConverter.fromPbSchema(request.getMetadata().getSchema());
                catalog.createTable(id, schema, null, null);
                respondOk(responseObserver, "Table created: " + identifier.getTable());
            } catch (Exception e) {
                log.error("[gRPC] createTable failed", e);
                respondError(responseObserver, e);
            }
        }

        @Override
        public void dropTable(
                PbDropTableRequest request, StreamObserver<PbOperationResult> responseObserver) {
            try {
                TableId id = TableId.parse(request.getTableName());
                boolean dropped = catalog.dropTable(id, false);
                if (!dropped) {
                    responseObserver.onError(
                            io.grpc.Status.NOT_FOUND
                                    .withDescription("Table not found: " + request.getTableName())
                                    .asRuntimeException());
                    return;
                }
                respondOk(responseObserver, "Table dropped: " + request.getTableName());
            } catch (Exception e) {
                log.error("[gRPC] dropTable failed for {}", request.getTableName(), e);
                respondError(responseObserver, e);
            }
        }

        @Override
        public void getTable(
                PbGetTableRequest request, StreamObserver<PbTableMetadata> responseObserver) {
            try {
                TableId id = TableId.parse(request.getTableName());
                RawWiseTable table = catalog.loadTable(id);
                responseObserver.onNext(ProtoTableMetaConverter.toPb(table.meta()));
                responseObserver.onCompleted();
            } catch (TableNotFoundException e) {
                responseObserver.onError(
                        io.grpc.Status.NOT_FOUND
                                .withDescription("Table not found: " + request.getTableName())
                                .asRuntimeException());
            } catch (Exception e) {
                log.error("[gRPC] getTable failed for {}", request.getTableName(), e);
                responseObserver.onError(
                        io.grpc.Status.INTERNAL
                                .withDescription("Failed to load table: " + e.getMessage())
                                .asRuntimeException());
            }
        }

        @Override
        public void alterTable(
                PbAlterTableRequest request, StreamObserver<PbOperationResult> responseObserver) {
            try {
                TableId id = TableId.parse(request.getTableName());
                catalog.alterSchema(
                        id, ProtoTableMetaConverter.fromPbSchema(request.getNewSchema()));
                respondOk(responseObserver);
            } catch (Exception e) {
                log.error("[gRPC] alterTable failed for {}", request.getTableName(), e);
                respondError(responseObserver, e);
            }
        }

        @Override
        public void listTables(
                ListTablesRequest request, StreamObserver<ListTablesResponse> responseObserver) {
            try {
                String namespace =
                        !request.getDatabase().isBlank() ? request.getDatabase() : "default";
                var tables = catalog.listTables(namespace);
                responseObserver.onNext(
                        ListTablesResponse.newBuilder()
                                .addAllTableNames(tables.stream().map(TableId::tableName).toList())
                                .build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                log.error("[gRPC] listTables failed", e);
                responseObserver.onError(e);
            }
        }

        @Override
        public void getSnapshot(
                PbGetSnapshotRequest request, StreamObserver<PbSnapshot> responseObserver) {
            try {
                TableId id = TableId.parse(request.getTableName());
                var snapshot = catalog.getSnapshot(id, request.getSnapshotId());
                if (snapshot.isEmpty()) {
                    responseObserver.onError(
                            io.grpc.Status.NOT_FOUND
                                    .withDescription(
                                            "Snapshot not found: "
                                                    + request.getTableName()
                                                    + "#"
                                                    + request.getSnapshotId())
                                    .asRuntimeException());
                    return;
                }
                responseObserver.onNext(ProtoTableMetaConverter.toPbSnapshot(snapshot.get()));
                responseObserver.onCompleted();
            } catch (Exception e) {
                log.error(
                        "[gRPC] getSnapshot failed for {}#{}",
                        request.getTableName(),
                        request.getSnapshotId(),
                        e);
                responseObserver.onError(e);
            }
        }

        @Override
        public void getSnapshots(
                PbGetSnapshotsRequest request, StreamObserver<PbSnapshotList> responseObserver) {
            try {
                TableId id = TableId.parse(request.getTableName());
                var snapshots = catalog.listSnapshots(id);
                responseObserver.onNext(
                        PbSnapshotList.newBuilder()
                                .addAllSnapshots(
                                        snapshots.stream()
                                                .map(ProtoTableMetaConverter::toPbSnapshot)
                                                .toList())
                                .build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                log.error("[gRPC] getSnapshots failed for {}", request.getTableName(), e);
                responseObserver.onError(e);
            }
        }

        // ── Legacy protocol methods (no Catalog equivalent) ──

        @Override
        public void beginCommit(
                PbCommitRequest request, StreamObserver<PbOperationResult> responseObserver) {
            responseObserver.onError(
                    io.grpc.Status.UNIMPLEMENTED
                            .withDescription(
                                    "beginCommit is deprecated — use Catalog.commitWithRetry instead")
                            .asRuntimeException());
        }

        @Override
        public void commit(PbCommitId request, StreamObserver<PbOperationResult> responseObserver) {
            responseObserver.onError(
                    io.grpc.Status.UNIMPLEMENTED
                            .withDescription(
                                    "commit is deprecated — use Catalog.commitWithRetry instead")
                            .asRuntimeException());
        }

        @Override
        public void abortCommit(
                PbCommitId request, StreamObserver<PbOperationResult> responseObserver) {
            responseObserver.onError(
                    io.grpc.Status.UNIMPLEMENTED
                            .withDescription(
                                    "abortCommit is deprecated — use Catalog.abort instead")
                            .asRuntimeException());
        }

        @Override
        public void putFile(
                PbFileMetadata request, StreamObserver<PbOperationResult> responseObserver) {
            responseObserver.onError(
                    io.grpc.Status.UNIMPLEMENTED
                            .withDescription("putFile is deprecated")
                            .asRuntimeException());
        }

        @Override
        public void listFiles(
                PbListFilesRequest request, StreamObserver<PbListFilesResponse> responseObserver) {
            responseObserver.onError(
                    io.grpc.Status.UNIMPLEMENTED
                            .withDescription("listFiles is deprecated")
                            .asRuntimeException());
        }

        @Override
        public void removeFile(
                PbFileMetadata request, StreamObserver<PbOperationResult> responseObserver) {
            responseObserver.onError(
                    io.grpc.Status.UNIMPLEMENTED
                            .withDescription("removeFile is deprecated")
                            .asRuntimeException());
        }

        @Override
        public void getLatestSnapshot(
                PbGetLatestSnapshotRequest request, StreamObserver<PbSnapshot> responseObserver) {
            try {
                TableId id = TableId.parse(request.getTableName());
                var snapshots = catalog.listSnapshots(id);
                if (snapshots.isEmpty()) {
                    responseObserver.onError(
                            io.grpc.Status.NOT_FOUND
                                    .withDescription(
                                            "No snapshot found for table: "
                                                    + request.getTableName())
                                    .asRuntimeException());
                } else {
                    responseObserver.onNext(ProtoTableMetaConverter.toPbSnapshot(snapshots.get(0)));
                    responseObserver.onCompleted();
                }
            } catch (Exception e) {
                log.error("[gRPC] getLatestSnapshot failed for {}", request.getTableName(), e);
                responseObserver.onError(e);
            }
        }

        @Override
        public void generateSnapshotId(
                GenerateSnapshotIdRequest request,
                StreamObserver<GenerateSnapshotIdResponse> responseObserver) {
            responseObserver.onError(
                    io.grpc.Status.UNIMPLEMENTED
                            .withDescription("generateSnapshotId is deprecated")
                            .asRuntimeException());
        }

        @Override
        public void getManifest(
                GetManifestRequest request, StreamObserver<PbManifestContent> responseObserver) {
            responseObserver.onError(
                    io.grpc.Status.UNIMPLEMENTED
                            .withDescription(
                                    "getManifest is deprecated — use SnapshotManager.listDataFiles instead")
                            .asRuntimeException());
        }

        private void respondOk(StreamObserver<PbOperationResult> observer) {
            respondOk(observer, null);
        }

        private void respondOk(StreamObserver<PbOperationResult> observer, String message) {
            var builder =
                    PbOperationResult.newBuilder().setSuccess(true).setStatus(CommitStatus.OK);
            if (message != null) builder.setMessage(message);
            observer.onNext(builder.build());
            observer.onCompleted();
        }

        private void respondError(StreamObserver<PbOperationResult> observer, Exception e) {
            observer.onNext(
                    PbOperationResult.newBuilder()
                            .setSuccess(false)
                            .setStatus(CommitStatus.ERROR)
                            .setMessage(e.getMessage())
                            .build());
            observer.onCompleted();
        }
    }
}
