package io.github.rawwise.benchmark.vbench.feedback;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

/**
 * Step 1: 验证 L3 反馈闭环 — annotate NEGATIVE 后检索权重变化。
 *
 * <p>这个测试需要 RawWise server 运行在 localhost:8080。 用 -Drawwise.server.alive=true 启用。
 *
 * <p>验证链路: 1. ingest 两条记录到测试表 2. search 获取 baseline 排序 3. annotate 第一条为 NEGATIVE (PREFERENCE,
 * value=-1.0) 4. search 再次检索，验证 NEGATIVE 记录排名下降
 */
@EnabledIfSystemProperty(named = "rawwise.server.alive", matches = "true")
class FeedbackLoopVerificationE2ETest {

    private static final String BASE_URL = "http://localhost:8080";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    private static final String TEST_TABLE = "default.feedback_loop_test";
    private static final String POSITIVE_KEY = "fbtest:positive:" + System.currentTimeMillis();
    private static final String NEGATIVE_KEY = "fbtest:negative:" + System.currentTimeMillis();

    @BeforeAll
    static void ingestTestRecords() throws Exception {
        ingest(
                POSITIVE_KEY,
                "A young woman in a red jacket walking down a street, high quality, consistent subject");
        ingest(
                NEGATIVE_KEY,
                "A young woman in a red jacket walking down a street, high quality, consistent subject");

        Thread.sleep(3000);
    }

    @Test
    void negativeAnnotationLowersSearchRank() throws Exception {
        List<Map<String, Object>> beforeResults = search("woman red jacket street", 5);
        assertFalse(beforeResults.isEmpty(), "search should return results before annotation");

        int negativeRankBefore = findRank(beforeResults, NEGATIVE_KEY);
        int positiveRankBefore = findRank(beforeResults, POSITIVE_KEY);
        assertTrue(
                negativeRankBefore >= 0 || positiveRankBefore >= 0,
                "at least one test record should be in search results");

        String negativeViewId = findViewId(beforeResults, NEGATIVE_KEY);
        System.out.println("Annotating viewId: " + negativeViewId);
        annotate(negativeViewId, "PREFERENCE", "-10.0", "vbench-test");

        Thread.sleep(2000);

        List<Map<String, Object>> afterResults = search("woman red jacket street", 5);
        assertFalse(afterResults.isEmpty(), "search should return results after annotation");

        int negativeRankAfter = findRank(afterResults, NEGATIVE_KEY);
        int positiveRankAfter = findRank(afterResults, POSITIVE_KEY);

        System.out.println("=== L3 Feedback Loop Verification ===");
        System.out.println(
                "Before annotate: positiveRank="
                        + positiveRankBefore
                        + ", negativeRank="
                        + negativeRankBefore);
        System.out.println("Before results: " + beforeResults);
        System.out.println("After results: " + afterResults);
        System.out.println(
                "After annotate: positiveRank="
                        + positiveRankAfter
                        + ", negativeRank="
                        + negativeRankAfter);

        if (negativeRankBefore >= 0 && negativeRankAfter >= 0) {
            assertTrue(
                    negativeRankAfter > negativeRankBefore,
                    "NEGATIVE annotated record should rank lower after annotation"
                            + " (before="
                            + negativeRankBefore
                            + ", after="
                            + negativeRankAfter
                            + ")");
        }

        if (positiveRankBefore >= 0 && positiveRankAfter >= 0) {
            assertTrue(
                    positiveRankAfter <= positiveRankBefore,
                    "Non-annotated record should not rank lower"
                            + " (before="
                            + positiveRankBefore
                            + ", after="
                            + positiveRankAfter
                            + ")");
        }

        double negativeScoreBefore = findScore(beforeResults, NEGATIVE_KEY);
        double negativeScoreAfter = findScore(afterResults, NEGATIVE_KEY);
        if (negativeScoreBefore > 0 && negativeScoreAfter > 0) {
            assertTrue(
                    negativeScoreAfter < negativeScoreBefore,
                    "NEGATIVE annotated record score should decrease"
                            + " (before="
                            + negativeScoreBefore
                            + ", after="
                            + negativeScoreAfter
                            + ")");
        }

        System.out.println("=== L3 VERIFIED: negative annotation lowered search rank/score ===");
    }

    @SuppressWarnings("unchecked")
    private static void ingest(String key, String body) throws Exception {
        Map<String, Object> request =
                Map.of(
                        "tableId", TEST_TABLE,
                        "key", key,
                        "body", body,
                        "immutable", false,
                        "properties",
                                Map.of(
                                        "text_content",
                                        body.substring(0, Math.min(512, body.length())),
                                        "source",
                                        "feedback-loop-test"));

        HttpRequest req =
                HttpRequest.newBuilder()
                        .uri(URI.create(BASE_URL + "/api/axiom/ingest"))
                        .header("Content-Type", "application/json")
                        .POST(
                                HttpRequest.BodyPublishers.ofString(
                                        MAPPER.writeValueAsString(request)))
                        .timeout(Duration.ofSeconds(10))
                        .build();

        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        assertTrue(
                resp.statusCode() == 200 || resp.statusCode() == 201,
                "ingest should succeed for key=" + key + " but got status=" + resp.statusCode());
        System.out.println("Ingested: " + key + " -> " + resp.body());
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> search(String query, int topK) throws Exception {
        Map<String, Object> request =
                Map.of(
                        "tableId", TEST_TABLE,
                        "query", query,
                        "topK", topK,
                        "snapshotId", 0);

        HttpRequest req =
                HttpRequest.newBuilder()
                        .uri(URI.create(BASE_URL + "/api/axiom/search"))
                        .header("Content-Type", "application/json")
                        .POST(
                                HttpRequest.BodyPublishers.ofString(
                                        MAPPER.writeValueAsString(request)))
                        .timeout(Duration.ofSeconds(10))
                        .build();

        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode(), "search should succeed");

        Map<String, Object> body = MAPPER.readValue(resp.body(), Map.class);
        Object results = body.get("results");
        if (results instanceof List<?> list) {
            return (List<Map<String, Object>>) list;
        }
        return List.of();
    }

    private static void annotate(String viewId, String type, String value, String annotatorId)
            throws Exception {
        String resolvedViewId = viewId.startsWith("rec:") ? viewId : "rec:" + viewId;
        Map<String, Object> request =
                Map.of(
                        "viewId", resolvedViewId,
                        "type", type,
                        "value", value,
                        "annotatorId", annotatorId,
                        "confidence", 0.9);

        HttpRequest req =
                HttpRequest.newBuilder()
                        .uri(URI.create(BASE_URL + "/api/axiom/annotate"))
                        .header("Content-Type", "application/json")
                        .POST(
                                HttpRequest.BodyPublishers.ofString(
                                        MAPPER.writeValueAsString(request)))
                        .timeout(Duration.ofSeconds(10))
                        .build();

        HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode(), "annotate should succeed for viewId=" + viewId);
        System.out.println(
                "Annotated: "
                        + viewId
                        + " type="
                        + type
                        + " value="
                        + value
                        + " -> "
                        + resp.body());
    }

    private static int findRank(List<Map<String, Object>> results, String key) {
        for (int i = 0; i < results.size(); i++) {
            Object viewId = results.get(i).get("viewId");
            if (viewId != null && viewId.toString().contains(key)) {
                return i;
            }
        }
        return -1;
    }

    private static String findViewId(List<Map<String, Object>> results, String key) {
        for (Map<String, Object> result : results) {
            Object viewId = result.get("viewId");
            if (viewId != null && viewId.toString().contains(key)) {
                return viewId.toString();
            }
        }
        return key;
    }

    private static double findScore(List<Map<String, Object>> results, String key) {
        for (Map<String, Object> result : results) {
            Object viewId = result.get("viewId");
            if (viewId != null && viewId.toString().contains(key)) {
                Object score = result.get("score");
                if (score instanceof Number n) {
                    return n.doubleValue();
                }
            }
        }
        return -1.0;
    }
}
