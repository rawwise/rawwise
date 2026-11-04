package io.github.rawwise.model.expression.serializer;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class ExpressionSerializerFactory {

    public enum SerializerType {
        JSON,
        PROTOBUF
    }

    private static final Map<SerializerType, Supplier<ExpressionSerializer>> SERIALIZER_CREATORS =
            new HashMap<>();

    static {
        SERIALIZER_CREATORS.put(SerializerType.JSON, JsonExpressionSerializer::new);
        // SERIALIZER_CREATORS.put(Serializer Type.PROTOBUF, Protobuf Expression Serializer::new);
    }

    public static ExpressionSerializer createSerializer(SerializerType type) {
        Supplier<ExpressionSerializer> creator = SERIALIZER_CREATORS.get(type);
        if (creator == null) {
            throw new IllegalArgumentException("Unsupported serializer type: " + type);
        }
        return creator.get();
    }

    public static ExpressionSerializer createDefaultSerializer() {
        return createSerializer(SerializerType.JSON);
    }

    public static ExpressionSerializer createSerializerFromConfig(String config) {
        // can read config from config file
        // simplified here, match by string
        try {
            SerializerType type = SerializerType.valueOf(config.toUpperCase());
            return createSerializer(type);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown serializer type in config: " + config, e);
        }
    }

    public static void registerSerializer(
            SerializerType type, Supplier<ExpressionSerializer> creator) {
        SERIALIZER_CREATORS.put(type, creator);
    }

    public static SerializerType[] getSupportedTypes() {
        return SERIALIZER_CREATORS.keySet().toArray(new SerializerType[0]);
    }
}
