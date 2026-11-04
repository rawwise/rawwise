package io.github.rawwise.engine.vector.onnx;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * ONNX cross-encoder reranker model that scores (query, document) pairs.
 *
 * <p>Follows the same reflection-based ONNX Runtime initialization pattern as {@link
 * OnnxEmbeddingProvider} to avoid a hard compile-time dependency on {@code ai.onnxruntime.*}. When
 * ONNX Runtime is not on the classpath or the model file is missing, the model degrades to a
 * deterministic lexical-overlap fallback (Jaccard similarity) so that the rerank pipeline remains
 * functional.
 *
 * <p>The cross-encoder takes a text pair {@code [query, document]} as a string tensor and produces
 * a single relevance logit. The logit is passed through a sigmoid to normalize the score to {@code
 * [0, 1]}.
 */
@Slf4j
public class DefaultRerankerModel implements AutoCloseable {

    private final String modelPath;
    private final boolean onnxRuntimeAvailable;
    private final Object ortSession;
    private final Object ortEnvironment;
    private final String inputName;
    private final String outputName;

    public DefaultRerankerModel(String modelPath) {
        this(modelPath, "input", "logits");
    }

    public DefaultRerankerModel(String modelPath, String inputName, String outputName) {
        this.modelPath = modelPath;
        this.inputName = inputName;
        this.outputName = outputName;
        this.onnxRuntimeAvailable = checkOnnxRuntimeAvailable();
        this.ortEnvironment =
                (!onnxRuntimeAvailable || modelPath == null || modelPath.isBlank())
                        ? null
                        : createEnvironment();
        this.ortSession =
                (!onnxRuntimeAvailable
                                || ortEnvironment == null
                                || modelPath == null
                                || modelPath.isBlank())
                        ? null
                        : createSession(ortEnvironment, modelPath);
        if (!onnxRuntimeAvailable) {
            log.warn(
                    "DefaultRerankerModel ONNX Runtime not on classpath, degrading to lexical-overlap fallback: modelPath={}",
                    modelPath);
        } else if (ortSession == null) {
            log.warn(
                    "DefaultRerankerModel failed to create ORT session, degrading to lexical-overlap fallback: modelPath={}",
                    modelPath);
        } else {
            log.info(
                    "DefaultRerankerModel initialized with ONNX Runtime: modelPath={} input={} output={}",
                    modelPath,
                    inputName,
                    outputName);
        }
    }

    /**
     * Score a (query, document) pair, returning a relevance score in {@code [0, 1]}.
     *
     * <p>When ONNX Runtime is available and the session was created successfully, the raw logit
     * from the model is sigmoid-normalized. Otherwise, a Jaccard lexical-overlap score is returned
     * as fallback.
     *
     * @param query the search query
     * @param document the candidate document text
     * @return relevance score in {@code [0, 1]}, higher means more relevant
     */
    public float score(String query, String document) {
        if (ortSession == null) {
            return lexicalOverlapScore(query, document);
        }
        return realOnnxScore(query, document);
    }

    public boolean isFallback() {
        return ortSession == null;
    }

    public String modelIdentifier() {
        return "cross-encoder-onnx:" + (modelPath != null ? modelPath : "fallback");
    }

    // ------------------------------------------------------------------
    // ONNX inference (reflection-based, same pattern as OnnxEmbeddingProvider)
    // ------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private float realOnnxScore(String query, String document) {
        try {
            Class<?> tensorClass = Class.forName("ai.onnxruntime.OnnxTensor");
            Class<?> resultClass = Class.forName("ai.onnxruntime.OrtSession$Result");
            Class<?> stringArrayClass = Class.forName("[Ljava.lang.String;");
            Method createStringTensor =
                    tensorClass.getMethod("createTensor", Object.class, stringArrayClass);
            Method sessionRun = ortSession.getClass().getMethod("run", Map.class);
            Method closeTensor = tensorClass.getMethod("close");
            Method closeResult = resultClass.getMethod("close");
            Method get = resultClass.getMethod("get", int.class);

            String[] pair =
                    new String[] {query != null ? query : "", document != null ? document : ""};
            Object inputTensor = createStringTensor.invoke(null, ortEnvironment, pair);
            try {
                Map<String, Object> inputs = new java.util.HashMap<>();
                inputs.put(inputName, inputTensor);
                Object result = sessionRun.invoke(ortSession, inputs);
                try {
                    Object output = get.invoke(result, 0);
                    float logit = extractFloatScalar(output);
                    return sigmoid(logit);
                } finally {
                    closeResult.invoke(result);
                }
            } finally {
                closeTensor.invoke(inputTensor);
            }
        } catch (ReflectiveOperationException | RuntimeException e) {
            log.warn(
                    "DefaultRerankerModel real inference failed, degrading to lexical-overlap: {}",
                    e.getMessage());
            return lexicalOverlapScore(query, document);
        }
    }

    private float extractFloatScalar(Object output) throws ReflectiveOperationException {
        if (output.getClass().isArray()) {
            if (output instanceof float[] f) return f.length > 0 ? f[0] : 0f;
            if (output instanceof float[][] f2)
                return f2.length > 0 && f2[0].length > 0 ? f2[0][0] : 0f;
        }
        Class<?> valueClass = Class.forName("ai.onnxruntime.OnnxValue");
        Method getValue = valueClass.getMethod("getValue");
        Object value = getValue.invoke(output);
        if (value instanceof Float fv) return fv;
        if (value instanceof Double dv) return dv.floatValue();
        if (value instanceof float[] f) return f.length > 0 ? f[0] : 0f;
        if (value instanceof float[][] f2) return f2.length > 0 && f2[0].length > 0 ? f2[0][0] : 0f;
        if (value instanceof Number n) return n.floatValue();
        throw new IllegalStateException(
                "Unexpected ONNX output type for reranker: "
                        + (value == null ? "null" : value.getClass()));
    }

    private static float sigmoid(float logit) {
        return (float) (1.0 / (1.0 + Math.exp(-logit)));
    }

    // ------------------------------------------------------------------
    // Lexical-overlap fallback (Jaccard similarity on whitespace tokens)
    // ------------------------------------------------------------------

    private static float lexicalOverlapScore(String query, String document) {
        if (query == null || document == null || query.isBlank() || document.isBlank()) {
            return 0f;
        }
        Set<String> qTokens = tokenize(query);
        Set<String> dTokens = tokenize(document);
        if (qTokens.isEmpty() || dTokens.isEmpty()) {
            return 0f;
        }
        Set<String> intersection = new HashSet<>(qTokens);
        intersection.retainAll(dTokens);
        Set<String> union = new HashSet<>(qTokens);
        union.addAll(dTokens);
        return (float) intersection.size() / union.size();
    }

    private static Set<String> tokenize(String text) {
        Set<String> tokens = new HashSet<>();
        String normalized = text.toLowerCase(Locale.ROOT);
        for (String token : normalized.split("\\s+")) {
            if (!token.isBlank()) {
                tokens.add(token);
            }
        }
        return tokens;
    }

    // ------------------------------------------------------------------
    // ONNX Runtime initialization (reflection, mirrors OnnxEmbeddingProvider)
    // ------------------------------------------------------------------

    private static boolean checkOnnxRuntimeAvailable() {
        try {
            Class.forName("ai.onnxruntime.OrtEnvironment");
            Class.forName("ai.onnxruntime.OrtSession");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private static Object createEnvironment() {
        try {
            Class<?> envClass = Class.forName("ai.onnxruntime.OrtEnvironment");
            Class<?> loggingLevel = Class.forName("ai.onnxruntime.OrtLoggingLevel");
            Object warningLevel =
                    Enum.valueOf((Class<Enum>) loggingLevel, "ORT_LOGGING_LEVEL_WARNING");
            Method createStateful =
                    envClass.getMethod("createStateful", loggingLevel, String.class);
            return createStateful.invoke(null, warningLevel, "rawwise-reranker");
        } catch (ReflectiveOperationException | RuntimeException e) {
            log.warn("DefaultRerankerModel failed to create ORT environment: {}", e.getMessage());
            return null;
        }
    }

    private static Object createSession(Object environment, String modelPath) {
        if (modelPath == null || modelPath.isBlank() || !Files.exists(Path.of(modelPath))) {
            log.warn("DefaultRerankerModel model path not found or blank: {}", modelPath);
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
                    "DefaultRerankerModel failed to create ORT session for modelPath={}: {}",
                    modelPath,
                    e.getMessage());
            return null;
        }
    }

    @Override
    public void close() {
        if (ortSession != null) {
            try {
                ortSession.getClass().getMethod("close").invoke(ortSession);
            } catch (ReflectiveOperationException | RuntimeException e) {
                log.warn("DefaultRerankerModel failed to close ORT session: {}", e.getMessage());
            }
        }
        if (ortEnvironment != null) {
            try {
                ortEnvironment.getClass().getMethod("close").invoke(ortEnvironment);
            } catch (ReflectiveOperationException | RuntimeException e) {
                log.warn(
                        "DefaultRerankerModel failed to close ORT environment: {}", e.getMessage());
            }
        }
    }
}
