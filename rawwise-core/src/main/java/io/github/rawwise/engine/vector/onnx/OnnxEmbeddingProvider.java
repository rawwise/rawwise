package io.github.rawwise.engine.vector.onnx;

import io.github.rawwise.engine.vector.EmbeddingProvider;
import io.github.rawwise.engine.vector.EmbeddingUnavailableException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OnnxEmbeddingProvider implements EmbeddingProvider, AutoCloseable {

    private final OnnxEmbeddingConfig config;
    private final boolean stubMode;
    private final boolean onnxRuntimeAvailable;
    private final Object ortSession;
    private final Object ortEnvironment;

    public OnnxEmbeddingProvider(OnnxEmbeddingConfig config) {
        this.config = config;
        this.stubMode = config.modelPath() != null && config.modelPath().startsWith("stub://");
        this.onnxRuntimeAvailable = checkOnnxRuntimeAvailable();
        this.ortEnvironment = stubMode || !onnxRuntimeAvailable ? null : createEnvironment();
        this.ortSession =
                (stubMode || !onnxRuntimeAvailable || ortEnvironment == null)
                        ? null
                        : createSession(ortEnvironment, config.modelPath());
        if (stubMode) {
            log.info(
                    "OnnxEmbeddingProvider initialized in stub mode: modelPath={} dimension={} executionProvider={}",
                    config.modelPath(),
                    config.dimension(),
                    config.executionProvider());
        } else if (!onnxRuntimeAvailable) {
            log.warn(
                    "OnnxEmbeddingProvider ONNX Runtime not on classpath, degrading to deterministic stub: modelPath={} dimension={}",
                    config.modelPath(),
                    config.dimension());
        } else if (ortSession == null) {
            log.warn(
                    "OnnxEmbeddingProvider failed to create ORT session, degrading to deterministic stub: modelPath={}",
                    config.modelPath());
        } else {
            log.info(
                    "OnnxEmbeddingProvider initialized with ONNX Runtime: modelPath={} dimension={} executionProvider={}",
                    config.modelPath(),
                    config.dimension(),
                    config.executionProvider());
        }
    }

    @Override
    public float[] embed(String text) {
        if (!config.enabled()) {
            throw new EmbeddingUnavailableException(
                    "OnnxEmbeddingProvider: disabled (config.enabled=false)");
        }
        if (stubMode || !onnxRuntimeAvailable || ortSession == null) {
            return deterministicVector(text);
        }
        return realOnnxEmbed(text);
    }

    @Override
    public float[][] embedBatch(List<String> texts) {
        float[][] result = new float[texts.size()][];
        for (int i = 0; i < texts.size(); i++) {
            result[i] = embed(texts.get(i));
        }
        return result;
    }

    public int dimension() {
        return config.dimension();
    }

    public String name() {
        return "onnx";
    }

    public boolean isStubMode() {
        return stubMode || !onnxRuntimeAvailable || ortSession == null;
    }

    @Override
    public String modelIdentifier() {
        return "onnx:" + config.executionProvider() + ":" + config.modelPath();
    }

    private float[] realOnnxEmbed(String text) {
        try {
            Class<?> tensorClass = Class.forName("ai.onnxruntime.OnnxTensor");
            Class<?> resultClass = Class.forName("ai.onnxruntime.OrtSession$Result");
            Class<?> stringClass = Class.forName("[Ljava.lang.String;");
            Method createStringTensor =
                    tensorClass.getMethod("createTensor", Object.class, stringClass);
            Method sessionRun = ortSession.getClass().getMethod("run", java.util.Map.class);
            Method closeTensor = tensorClass.getMethod("close");
            Method closeResult = resultClass.getMethod("close");
            Method get = resultClass.getMethod("get", int.class);

            String[] inputArray = new String[] {text};
            Object inputTensor = createStringTensor.invoke(null, ortEnvironment, inputArray);
            try {
                String inputName = "input";
                java.util.Map<String, Object> inputs = new java.util.HashMap<>();
                inputs.put(inputName, inputTensor);
                Object result = sessionRun.invoke(ortSession, inputs);
                try {
                    Object output = get.invoke(result, 0);
                    float[] embedding = extractFloatArray(output);
                    if (embedding.length != config.dimension()) {
                        throw new EmbeddingUnavailableException(
                                "OnnxEmbeddingProvider embedding dimension mismatch: expected "
                                        + config.dimension()
                                        + ", got "
                                        + embedding.length);
                    }
                    return embedding;
                } finally {
                    closeResult.invoke(result);
                }
            } finally {
                closeTensor.invoke(inputTensor);
            }
        } catch (ReflectiveOperationException | RuntimeException e) {
            log.warn(
                    "OnnxEmbeddingProvider real inference failed, degrading to deterministic stub: {}",
                    e.getMessage());
            return deterministicVector(text);
        }
    }

    private float[] extractFloatArray(Object output) throws ReflectiveOperationException {
        if (output.getClass().isArray()) {
            if (output instanceof float[] f) return f;
            if (output instanceof float[][] f2) return f2[0];
        }
        Class<?> valueClass = Class.forName("ai.onnxruntime.OnnxValue");
        Method getValue = valueClass.getMethod("getValue");
        Object value = getValue.invoke(output);
        if (value instanceof float[] f) return f;
        if (value instanceof float[][] f2) return f2[0];
        if (value instanceof List<?> list && !list.isEmpty() && list.get(0) instanceof float[] f)
            return f;
        throw new IllegalStateException(
                "Unexpected ONNX output type: " + (value == null ? "null" : value.getClass()));
    }

    private static boolean checkOnnxRuntimeAvailable() {
        try {
            Class.forName("ai.onnxruntime.OrtEnvironment");
            Class.forName("ai.onnxruntime.OrtSession");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    private static Object createEnvironment() {
        try {
            Class<?> envClass = Class.forName("ai.onnxruntime.OrtEnvironment");
            Class<?> loggingLevel = Class.forName("ai.onnxruntime.OrtLoggingLevel");
            Object warningLevel =
                    Enum.valueOf((Class<Enum>) loggingLevel, "ORT_LOGGING_LEVEL_WARNING");
            Method createStateful =
                    envClass.getMethod("createStateful", loggingLevel, String.class);
            return createStateful.invoke(null, warningLevel, "rawwise-onnx");
        } catch (ReflectiveOperationException | RuntimeException e) {
            log.warn("OnnxEmbeddingProvider failed to create ORT environment: {}", e.getMessage());
            return null;
        }
    }

    private static Object createSession(Object environment, String modelPath) {
        if (modelPath == null || modelPath.isBlank() || !Files.exists(Path.of(modelPath))) {
            log.warn("OnnxEmbeddingProvider model path not found or blank: {}", modelPath);
            return null;
        }
        try {
            Class<?> sessionClass = Class.forName("ai.onnxruntime.OrtSession");
            Class<?> sessionOptionsClass =
                    Class.forName("ai.onnxruntime.OrtSession$SessionOptions");
            Class<?> envClass = Class.forName("ai.onnxruntime.OrtEnvironment");
            Constructor<?> optionsCtor = sessionOptionsClass.getConstructor();
            Object options = optionsCtor.newInstance();
            Method addProvider = sessionOptionsClass.getMethod("addCPU", double.class, int.class);
            addProvider.invoke(options, 0.0, 1);
            Constructor<?> sessionCtor =
                    sessionClass.getConstructor(envClass, String.class, sessionOptionsClass);
            return sessionCtor.newInstance(environment, modelPath, options);
        } catch (ReflectiveOperationException | RuntimeException e) {
            log.warn(
                    "OnnxEmbeddingProvider failed to create ORT session for modelPath={}: {}",
                    modelPath,
                    e.getMessage());
            return null;
        }
    }

    private float[] deterministicVector(String text) {
        long seed = (text == null ? 0 : text.hashCode()) + 31L * config.dimension();
        Random rng = new Random(seed);
        float[] vector = new float[config.dimension()];
        double sumSq = 0;
        for (int i = 0; i < config.dimension(); i++) {
            vector[i] = (float) (rng.nextGaussian() * 0.1);
            sumSq += vector[i] * vector[i];
        }
        float norm = (float) Math.sqrt(sumSq);
        if (norm > 0) {
            for (int i = 0; i < config.dimension(); i++) {
                vector[i] /= norm;
            }
        }
        return vector;
    }

    @Override
    public void close() {
        if (ortSession != null) {
            try {
                ortSession.getClass().getMethod("close").invoke(ortSession);
            } catch (ReflectiveOperationException | RuntimeException e) {
                log.warn("OnnxEmbeddingProvider failed to close ORT session: {}", e.getMessage());
            }
        }
        if (ortEnvironment != null) {
            try {
                ortEnvironment.getClass().getMethod("close").invoke(ortEnvironment);
            } catch (ReflectiveOperationException | RuntimeException e) {
                log.warn(
                        "OnnxEmbeddingProvider failed to close ORT environment: {}",
                        e.getMessage());
            }
        }
    }
}
