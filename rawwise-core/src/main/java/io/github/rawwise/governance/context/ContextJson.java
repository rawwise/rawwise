package io.github.rawwise.governance.context;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;

public final class ContextJson {

    public static final ObjectMapper MAPPER = createMapper();

    private ContextJson() {}

    private static ObjectMapper createMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(DeserializationFeature.FAIL_ON_TRAILING_TOKENS, true);
        return mapper;
    }

    public static byte[] writeValueAsBytes(Object value) throws IOException {
        return MAPPER.writeValueAsBytes(value);
    }

    public static <T> T readValue(byte[] src, Class<T> valueType) throws IOException {
        return MAPPER.readValue(src, valueType);
    }

    public static String writeValueAsString(Object value) throws IOException {
        return MAPPER.writeValueAsString(value);
    }
}
