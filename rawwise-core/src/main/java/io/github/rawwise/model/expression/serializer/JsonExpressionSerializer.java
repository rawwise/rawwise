package io.github.rawwise.model.expression.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.github.rawwise.model.expression.Expression;
import io.jsonwebtoken.io.SerializationException;
import java.io.IOException;

public class JsonExpressionSerializer implements ExpressionSerializer {

    private final ObjectMapper objectMapper;

    public JsonExpressionSerializer() {
        this.objectMapper = createObjectMapper();
    }

    private ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();

        // configure polymorphic serialization
        mapper.registerModule(createExpressionModule());

        // configure serialization options
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);

        return mapper;
    }

    private SimpleModule createExpressionModule() {
        SimpleModule module = new SimpleModule("ExpressionModule");

        // register serializer and deserializer(if custom serialization logic is needed)
        // default serialization is used here,Expressionand its subclasses are already serializable
        // POJOs

        return module;
    }

    @Override
    public byte[] serialize(Expression expression) {
        try {
            return objectMapper.writeValueAsBytes(expression);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Failed to serialize expression to JSON", e);
        }
    }

    @Override
    public Expression deserialize(byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, Expression.class);
        } catch (IOException e) {
            throw new SerializationException("Failed to deserialize expression from JSON", e);
        }
    }
}
