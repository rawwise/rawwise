package io.github.rawwise.metastore;

import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;

public class GrpcMetastoreFactory implements MetastoreFactory {

    @Override
    public String kind() {
        return "grpc";
    }

    @Override
    public Metastore create(RawWiseConfig config) {
        String grpcTarget = config.get(RawWiseOptions.METASTORE_GRPC_TARGET);
        if (grpcTarget != null && !grpcTarget.isBlank()) {
            String[] parts = grpcTarget.split(":", 2);
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9001;
            return new GrpcMetastoreClient(host, port);
        }
        String host = config.get(RawWiseOptions.METASTORE_GRPC_HOST);
        int port = config.get(RawWiseOptions.METASTORE_GRPC_PORT);
        return new GrpcMetastoreClient(host, port);
    }
}
