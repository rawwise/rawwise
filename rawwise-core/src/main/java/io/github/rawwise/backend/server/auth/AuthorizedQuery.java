package io.github.rawwise.backend.server.auth;

import java.util.Map;
import org.apache.spark.sql.SaveMode;

public record AuthorizedQuery(String sql, Map<String, String> options, SaveMode saveMode) {}
