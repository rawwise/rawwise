package io.github.rawwise.model.record;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TypeConverterRegistry {
    private final Map<Class<?>, TypeConverter<?>> converters = new HashMap<>();

    public TypeConverterRegistry() {
        registerBuiltinConverters();
        loadExtensionConverters();
    }

    private void registerBuiltinConverters() {
        registerConverter(Class.class, new JavaTypeConverter());
        try {
            Class.forName("org.apache.spark.sql.types.DataType");
            registerConverter(org.apache.spark.sql.types.DataType.class, new SparkTypeConverter());
            log.info("Spark type converter registered successfully");
        } catch (ClassNotFoundException e) {
            log.info("Spark not found in classpath, skipping Spark type converter");
        }
    }

    private void loadExtensionConverters() {
        ServiceLoader<TypeConverter> loader = ServiceLoader.load(TypeConverter.class);
        for (TypeConverter converter : loader) {
            Class<?> targetType = extractTargetType(converter);
            if (targetType != null) {
                registerConverter(targetType, converter);
                log.info("Registered extension converter for: {}", targetType.getName());
            }
        }
    }

    private Class<?> extractTargetType(TypeConverter converter) {
        try {
            // obtain generic parameter type via reflection
            java.lang.reflect.Type[] interfaces = converter.getClass().getGenericInterfaces();
            for (java.lang.reflect.Type iface : interfaces) {
                if (iface instanceof java.lang.reflect.ParameterizedType) {
                    java.lang.reflect.ParameterizedType pType =
                            (java.lang.reflect.ParameterizedType) iface;
                    if (pType.getRawType().equals(TypeConverter.class)) {
                        java.lang.reflect.Type[] typeArgs = pType.getActualTypeArguments();
                        if (typeArgs.length > 0 && typeArgs[0] instanceof Class) {
                            return (Class<?>) typeArgs[0];
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn(
                    "Failed to extract target type from converter: {}",
                    converter.getClass().getName());
        }
        return null;
    }

    public <T> void registerConverter(Class<?> targetType, TypeConverter<T> converter) {
        converters.put(targetType, converter);
    }

    @SuppressWarnings("unchecked")
    public <T> TypeConverter<T> getConverter(Class<T> targetType) {
        TypeConverter<?> converter = converters.get(targetType);
        if (converter == null) {
            throw new IllegalArgumentException(
                    "No converter registered for: " + targetType.getName());
        }
        return (TypeConverter<T>) converter;
    }

    public <T> RawWiseType toRawWiseType(T externalType, Class<T> targetType) {
        return getConverter(targetType).toRawWiseType(externalType);
    }

    public <T> T fromRawWiseType(RawWiseType dataType, Class<T> targetType) {
        return getConverter(targetType).fromRawWiseType(dataType);
    }
}
