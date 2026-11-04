package io.github.rawwise.governance.auth;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = Principal.UserPrincipal.class, name = "user"),
    @JsonSubTypes.Type(value = Principal.AgentPrincipal.class, name = "agent")
})
public sealed interface Principal permits Principal.UserPrincipal, Principal.AgentPrincipal {

    String id();

    record UserPrincipal(String id, String displayName) implements Principal {}

    record AgentPrincipal(String agentId, String userId, String delegatedBy) implements Principal {
        @Override
        public String id() {
            return agentId;
        }
    }
}
