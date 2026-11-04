package io.github.rawwise.engine.compaction;

import org.apache.hadoop.conf.Configuration;

public final class HadoopConfHolder {

    private static final Configuration INSTANCE = new Configuration();

    private HadoopConfHolder() {}

    public static Configuration get() {
        return INSTANCE;
    }
}
