package io.github.rawwise.governance.auth;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public final class AuthJson {

    public static final ObjectMapper MAPPER = createMapper();

    private AuthJson() {}

    private static ObjectMapper createMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(DeserializationFeature.FAIL_ON_TRAILING_TOKENS, true);
        return mapper;
    }
}
