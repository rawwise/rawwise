package io.github.rawwise.backend.server.auth;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.spark.sql.SaveMode;

public class QueryAuthorizer {
    private static final Pattern DDL_PATTERN =
            Pattern.compile("(?i)^\\s*(DROP|ALTER|CREATE|TRUNCATE)\\b");
    private static final Pattern OUTPUT_PATH_PATTERN =
            Pattern.compile("^/rawwise/workspace/[^/]+/(output|checkpoints|inference)/.+$");

    public AuthorizedQuery authorize(Principal principal, String sql, Map<String, String> options) {
        if (principal.role() == Role.ADMIN) {
            return new AuthorizedQuery(sql, options, SaveMode.Append);
        }

        if (containsDDL(sql) && !principal.hasPermission(Permission.DDL)) {
            throw new AuthorizationException("Operation not allowed for current role");
        }

        String outputPath = options.get("outputPath");
        if (outputPath != null) {
            String canonicalPath = canonicalize(outputPath);
            if (!OUTPUT_PATH_PATTERN.matcher(canonicalPath).matches()) {
                throw new AuthorizationException("Output path outside allowed directory");
            }
            if (!canonicalPath.startsWith("/rawwise/workspace/" + principal.tenantId() + "/")) {
                throw new AuthorizationException("Cross-tenant access denied");
            }
        }

        return new AuthorizedQuery(sql, options, SaveMode.Append);
    }

    private boolean containsDDL(String sql) {
        String normalized = sql.trim();
        if (DDL_PATTERN.matcher(normalized).find()) return true;
        if (normalized.contains(";")) {
            String[] parts = normalized.split(";");
            for (String part : parts) {
                if (DDL_PATTERN.matcher(part.trim()).find()) return true;
            }
        }
        return false;
    }

    private String canonicalize(String path) {
        try {
            return Path.of(path).normalize().toString();
        } catch (InvalidPathException e) {
            throw new AuthorizationException("Invalid output path");
        }
    }
}
