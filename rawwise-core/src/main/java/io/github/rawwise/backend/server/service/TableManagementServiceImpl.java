package io.github.rawwise.backend.server.service;

import io.github.rawwise.server.CreateTableRequest;
import io.github.rawwise.server.PartitionInfo;
import io.github.rawwise.server.PartitionList;
import io.github.rawwise.server.Status;
import io.github.rawwise.server.TableInfo;
import io.github.rawwise.server.TableManagementServiceGrpc;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TableManagementServiceImpl
        extends TableManagementServiceGrpc.TableManagementServiceImplBase {

    public TableManagementServiceImpl() {}

    @Override
    public void createTable(CreateTableRequest request, StreamObserver<Status> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("TableManagementService.createTable not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void dropTable(TableInfo request, StreamObserver<Status> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("TableManagementService.dropTable not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void getTableInfo(TableInfo request, StreamObserver<TableInfo> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("TableManagementService.getTableInfo not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void listTables(TableInfo request, StreamObserver<TableInfo> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("TableManagementService.listTables not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void describeTable(TableInfo request, StreamObserver<TableInfo> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("TableManagementService.describeTable not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void alterTable(TableInfo request, StreamObserver<Status> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("TableManagementService.alterTable not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void refreshTable(TableInfo request, StreamObserver<Status> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("TableManagementService.refreshTable not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void getPartitions(TableInfo request, StreamObserver<PartitionList> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("TableManagementService.getPartitions not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void addPartition(PartitionInfo request, StreamObserver<Status> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("TableManagementService.addPartition not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void dropPartition(PartitionInfo request, StreamObserver<Status> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("TableManagementService.dropPartition not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void repairPartition(PartitionInfo request, StreamObserver<Status> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription(
                                "TableManagementService.repairPartition not yet implemented")
                        .asRuntimeException());
    }
}
