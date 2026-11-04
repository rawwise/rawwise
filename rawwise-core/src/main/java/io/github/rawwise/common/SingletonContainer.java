package io.github.rawwise.common;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

@SuppressWarnings("unchecked")
@Slf4j
public class SingletonContainer {
    private static final Map<String, Object> INSTANCES = new ConcurrentHashMap<>();

    private SingletonContainer() {}

    private static String generateKey(Class<?> clazz, Object... params) {
        if (params == null || params.length == 0) {
            return clazz.getName();
        }
        int paramsHash = Arrays.deepHashCode(params);
        return clazz.getName() + "@" + Integer.toHexString(paramsHash);
    }

    public static <T> T getInstance(Class<T> clazz, Object... constructorArgs) {
        String key = generateKey(clazz, constructorArgs);
        return (T)
                INSTANCES.computeIfAbsent(
                        key,
                        k -> {
                            try {
                                if (constructorArgs == null || constructorArgs.length == 0) {
                                    Constructor<T> constructor = clazz.getDeclaredConstructor();
                                    constructor.setAccessible(true);
                                    return constructor.newInstance();
                                } else {
                                    Class<?>[] paramTypes =
                                            Arrays.stream(constructorArgs)
                                                    .map(Object::getClass)
                                                    .toArray(Class<?>[]::new);

                                    Constructor<T> constructor =
                                            clazz.getDeclaredConstructor(paramTypes);
                                    constructor.setAccessible(true);
                                    return constructor.newInstance(constructorArgs);
                                }
                            } catch (NoSuchMethodException e) {
                                throw new IllegalArgumentException(
                                        "class "
                                                + clazz.getName()
                                                + " do not have matching constructor, args: "
                                                + Arrays.toString(constructorArgs),
                                        e);
                            } catch (InstantiationException
                                    | IllegalAccessException
                                    | InvocationTargetException e) {
                                throw new RuntimeException(
                                        "create " + clazz.getName() + " singleton fail.", e);
                            }
                        });
    }

    public static <T> T destroyInstance(Class<T> clazz, Object... constructorArgs) {
        String key = generateKey(clazz, constructorArgs);
        Object instance = INSTANCES.remove(key);

        if (instance instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                log.warn("close singleton fail: {}", e.getMessage());
            }
        }

        return (T) instance;
    }
}
