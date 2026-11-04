package io.github.rawwise.axiom.pipeline.benchmark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BenchmarkFramework {

    public BenchmarkResult runBaseline(
            BenchmarkConfig config,
            List<String> prompts,
            Function<String, Map<String, Object>> generateFn) {
        log.info(
                "Running baseline: {} (prompts={}, warmup={}, measure={})",
                config.experimentName(),
                prompts.size(),
                config.warmupRounds(),
                config.measureRounds());

        for (int i = 0; i < config.warmupRounds(); i++) {
            for (String prompt : prompts) {
                generateFn.apply(prompt);
            }
        }

        List<Map<String, Object>> samples = new ArrayList<>();
        for (int i = 0; i < config.measureRounds(); i++) {
            for (String prompt : prompts) {
                Map<String, Object> result = new HashMap<>(generateFn.apply(prompt));
                result.put("prompt", prompt);
                result.put("round", i);
                samples.add(result);
            }
        }

        Map<String, Double> aggregate = aggregateNumericMetrics(samples);
        return new BenchmarkResult(
                config.experimentName(), false, samples.size(), samples, aggregate);
    }

    public BenchmarkResult runWithRawWise(
            BenchmarkConfig config,
            List<String> prompts,
            Function<String, Map<String, Object>> rawwiseFn) {
        log.info(
                "Running with RawWise: {} (prompts={}, warmup={}, measure={})",
                config.experimentName(),
                prompts.size(),
                config.warmupRounds(),
                config.measureRounds());

        for (int i = 0; i < config.warmupRounds(); i++) {
            for (String prompt : prompts) {
                rawwiseFn.apply(prompt);
            }
        }

        List<Map<String, Object>> samples = new ArrayList<>();
        for (int i = 0; i < config.measureRounds(); i++) {
            for (String prompt : prompts) {
                Map<String, Object> result = new HashMap<>(rawwiseFn.apply(prompt));
                result.put("prompt", prompt);
                result.put("round", i);
                samples.add(result);
            }
        }

        Map<String, Double> aggregate = aggregateNumericMetrics(samples);
        return new BenchmarkResult(
                config.experimentName(), true, samples.size(), samples, aggregate);
    }

    public BenchmarkResult.Delta compare(BenchmarkResult baseline, BenchmarkResult experiment) {
        Map<String, Double> improvements = new HashMap<>();
        for (String metric : baseline.aggregateMetrics().keySet()) {
            double baseVal = baseline.aggregateMetrics().getOrDefault(metric, 0.0);
            double expVal = experiment.aggregateMetrics().getOrDefault(metric, 0.0);
            improvements.put(metric, expVal - baseVal);
        }
        return new BenchmarkResult.Delta(baseline, experiment, improvements);
    }

    public static Map<String, Double> aggregateNumericMetrics(List<Map<String, Object>> samples) {
        Map<String, Double> sum = new HashMap<>();
        Map<String, Integer> count = new HashMap<>();

        for (Map<String, Object> sample : samples) {
            for (Map.Entry<String, Object> entry : sample.entrySet()) {
                String key = entry.getKey();
                Object val = entry.getValue();
                if (val instanceof Number num) {
                    sum.merge(key, num.doubleValue(), Double::sum);
                    count.merge(key, 1, Integer::sum);
                }
            }
        }

        Map<String, Double> result = new HashMap<>();
        for (String key : sum.keySet()) {
            result.put(key, sum.get(key) / count.get(key));
        }
        return result;
    }
}
