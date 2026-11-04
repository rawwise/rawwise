package io.github.rawwise.backend.server.service;

import com.google.protobuf.Duration;
import com.google.protobuf.Empty;
import io.github.rawwise.server.CreateSessionRequest;
import io.github.rawwise.server.CreateSessionResponse;
import io.github.rawwise.server.SessionConfig;
import io.github.rawwise.server.SessionInfo;
import io.github.rawwise.server.SessionManagementServiceGrpc;
import io.github.rawwise.server.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SessionManagementServiceImpl
        extends SessionManagementServiceGrpc.SessionManagementServiceImplBase {

    @Override
    public void createSession(
            CreateSessionRequest request, StreamObserver<CreateSessionResponse> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription(
                                "SessionManagementService.createSession not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void closeSession(SessionInfo request, StreamObserver<Status> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription(
                                "SessionManagementService.closeSession not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void getSessionInfo(SessionInfo request, StreamObserver<SessionInfo> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription(
                                "SessionManagementService.getSessionInfo not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void updateSessionConfig(
            SessionConfig request, StreamObserver<Status> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription(
                                "SessionManagementService.updateSessionConfig not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void listSessions(Empty request, StreamObserver<SessionInfo> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription(
                                "SessionManagementService.listSessions not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void renewSession(SessionInfo request, StreamObserver<Status> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription(
                                "SessionManagementService.renewSession not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void cleanupIdleSessions(Duration request, StreamObserver<Status> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription(
                                "SessionManagementService.cleanupIdleSessions not yet implemented")
                        .asRuntimeException());
    }
}
