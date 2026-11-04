package io.github.rawwise.governance.auth;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.Set;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
@JsonSubTypes({@JsonSubTypes.Type(value = Permission.TablePermission.class, name = "table")})
public sealed interface Permission permits Permission.TablePermission {

    String tableId();

    enum TableOp {
        SELECT,
        INSERT,
        UPDATE,
        DELETE,
        CREATE,
        DROP,
        ALTER,
        OWNERSHIP
    }

    record TablePermission(String tableId, Set<TableOp> allowedOps) implements Permission {
        public TablePermission {
            allowedOps = Set.copyOf(allowedOps);
        }
    }
}
