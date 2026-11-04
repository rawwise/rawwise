package io.github.rawwise.backend.server.service;

import io.github.rawwise.server.JobFilter;
import io.github.rawwise.server.JobInfo;
import io.github.rawwise.server.JobList;
import io.github.rawwise.server.JobManagementServiceGrpc;
import io.github.rawwise.server.JobMetrics;
import io.github.rawwise.server.LogMessage;
import io.github.rawwise.server.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobManagementServiceImpl
        extends JobManagementServiceGrpc.JobManagementServiceImplBase {

    @Override
    public void listJobs(JobFilter request, StreamObserver<JobList> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("JobManagementService.listJobs not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void getJobInfo(JobInfo request, StreamObserver<JobInfo> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("JobManagementService.getJobInfo not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void getJobMetrics(JobInfo request, StreamObserver<JobMetrics> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("JobManagementService.getJobMetrics not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void killJob(JobInfo request, StreamObserver<Status> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("JobManagementService.killJob not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void retryJob(JobInfo request, StreamObserver<Status> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("JobManagementService.retryJob not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void cleanupJobs(JobFilter request, StreamObserver<Status> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("JobManagementService.cleanupJobs not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void monitorJob(JobInfo request, StreamObserver<JobInfo> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("JobManagementService.monitorJob not yet implemented")
                        .asRuntimeException());
    }

    @Override
    public void getJobLogs(JobInfo request, StreamObserver<LogMessage> responseObserver) {
        responseObserver.onError(
                io.grpc.Status.UNIMPLEMENTED
                        .withDescription("JobManagementService.getJobLogs not yet implemented")
                        .asRuntimeException());
    }
}
