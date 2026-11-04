package io.github.rawwise.benchmark.vbench.feedback;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class VideoFeedbackLoopE2ETest {

    @Test
    void storesExperimentResultAndAnnotatesMetricFeedback() throws Exception {
        Path resultFile = writeResultFile();
        RecordingAxiomClient axiom = new RecordingAxiomClient();
        VideoFeedbackLoop loop =
                new VideoFeedbackLoop(axiom, new RuleBasedLlmVideoFeedbackProvider());

        VideoFeedbackLoopReport report = loop.ingestAndAnnotate(resultFile, true);

        assertEquals(3, axiom.ingests.size());
        assertEquals("default.video_generation_runs", axiom.ingests.get(0).tableId());
        assertEquals("vbench:prompt_001:baseline", axiom.ingests.get(0).key());
        assertEquals("baseline", axiom.ingests.get(0).properties().get("tag"));
        assertEquals("default.video_generation_runs", axiom.ingests.get(1).tableId());
        assertEquals("vbench:prompt_001:rawwise", axiom.ingests.get(1).key());
        assertEquals("default.video_evaluations", axiom.ingests.get(2).tableId());

        assertFalse(axiom.annotations.isEmpty());
        assertTrue(
                axiom.annotations.stream()
                        .anyMatch(
                                a ->
                                        a.viewId().equals("view:vbench:prompt_001:rawwise")
                                                && a.type().equals("POSITIVE")
                                                && a.value()
                                                        .contains("vbench_subset_score improved")));
        assertTrue(
                axiom.annotations.stream()
                        .anyMatch(
                                a ->
                                        a.viewId().equals("view:vbench:prompt_001:baseline")
                                                && a.type().equals("NEGATIVE")
                                                && a.value()
                                                        .contains("temporal_flickering=0.4349")));
        assertTrue(report.autoFeedbackCount() >= 3);
        assertEquals(2, report.llmFeedbackCount());
    }

    @Test
    void llmFeedbackCanReplaceHumanFeedbackButKeepsLowerConfidence() throws Exception {
        Path resultFile = writeResultFile();
        RecordingAxiomClient axiom = new RecordingAxiomClient();
        VideoFeedbackLoop loop =
                new VideoFeedbackLoop(axiom, new RuleBasedLlmVideoFeedbackProvider());

        loop.ingestAndAnnotate(resultFile, true);

        List<AxiomAnnotation> llmAnnotations =
                axiom.annotations.stream()
                        .filter(a -> a.annotatorId().equals("llm-video-judge"))
                        .toList();
        assertEquals(2, llmAnnotations.size());
        assertTrue(llmAnnotations.stream().allMatch(a -> a.confidence() <= 0.70));
        assertTrue(
                llmAnnotations.stream()
                        .anyMatch(a -> a.value().contains("temporal stability improved")));
        assertTrue(
                llmAnnotations.stream()
                        .anyMatch(a -> a.value().contains("aesthetic quality regressed")));
    }

    private Path writeResultFile() throws Exception {
        Path dir = Files.createTempDirectory("vbench-feedback-test");
        Path result = dir.resolve("prompt_001_real.json");
        Files.writeString(
                result,
                """
            {
              "prompt_id": "prompt_001",
              "prompt": "A young woman in a red jacket walking down a street",
              "provider": "cogvideox-api",
              "model": "cogvideox-3",
              "baseline": {
                "vbench_subset_score": 0.5787,
                "dimensions_evaluated": 7,
                "dimensions_total_vbench": 16,
                "coverage_ratio": 0.44,
                "scores": {
                  "subject_consistency": 0.9697,
                  "background_consistency": 0.9789,
                  "temporal_flickering": 0.4349,
                  "aesthetic_quality": 0.5538,
                  "imaging_quality": 0.0437,
                  "appearance_style": 0.2697,
                  "overall_consistency": 0.8005
                },
                "video_path": "benchmark/vbench/videos/baseline/prompt_001_run_0.mp4"
              },
              "rawwise": {
                "vbench_subset_score": 0.6245,
                "dimensions_evaluated": 7,
                "dimensions_total_vbench": 16,
                "coverage_ratio": 0.44,
                "scores": {
                  "subject_consistency": 0.9715,
                  "background_consistency": 0.9887,
                  "temporal_flickering": 0.6370,
                  "aesthetic_quality": 0.5399,
                  "imaging_quality": 0.0551,
                  "appearance_style": 0.3048,
                  "overall_consistency": 0.8745
                },
                "video_path": "benchmark/vbench/videos/rawwise/prompt_001_run_0.mp4"
              },
              "delta": {
                "vbench_subset_score": 0.0458,
                "relative_pct": 7.91,
                "temporal_flickering": 0.2021,
                "aesthetic_quality": -0.0139,
                "overall_consistency": 0.0740
              }
            }
            """);
        return result;
    }

    private static final class RecordingAxiomClient implements AxiomClient {
        private final List<AxiomIngest> ingests = new ArrayList<>();
        private final List<AxiomAnnotation> annotations = new ArrayList<>();

        @Override
        public String ingest(AxiomIngest ingest) {
            ingests.add(ingest);
            return "view:" + ingest.key();
        }

        @Override
        public void annotate(AxiomAnnotation annotation) {
            annotations.add(annotation);
        }
    }
}
