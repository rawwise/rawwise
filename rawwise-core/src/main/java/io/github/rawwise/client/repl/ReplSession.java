package io.github.rawwise.client.repl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ReplSession {

    public enum Format {
        TABLE,
        CSV,
        JSON
    }

    private static final int DEFAULT_QUERY_TIMEOUT_SECONDS = 300;

    private String currentDatabase;
    private Format outputFormat;
    private boolean timingEnabled;
    private long lastQueryTimeMs;
    private int queryTimeout;
    private final Map<String, String> variables;

    public ReplSession() {
        this.currentDatabase = "default";
        this.outputFormat = Format.TABLE;
        this.timingEnabled = true;
        this.queryTimeout = DEFAULT_QUERY_TIMEOUT_SECONDS;
        this.variables = new ConcurrentHashMap<>();
    }

    public void setVariable(String key, String value) {
        variables.put(key, value);
    }

    public String getVariable(String key) {
        return variables.get(key);
    }

    public Map<String, String> getVariables() {
        return variables;
    }

    public boolean toggleTiming() {
        this.timingEnabled = !this.timingEnabled;
        return this.timingEnabled;
    }
}
