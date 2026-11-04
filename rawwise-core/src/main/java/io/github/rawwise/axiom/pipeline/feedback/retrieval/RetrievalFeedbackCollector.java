package io.github.rawwise.axiom.pipeline.feedback.retrieval;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.rawwise.common.collection.iterator.CloseableIterator;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.metastore.KeyValue;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RetrievalFeedbackCollector {

    public static final String KEY_PREFIX = "retrieval_feedback:";

    private static final ObjectMapper MAPPER =
            new ObjectMapper().registerModule(new JavaTimeModule());

    private final KnowledgeStore knowledgeStore;
    private final String tableId;
    private final AtomicLong counter = new AtomicLong(-1L);

    public RetrievalFeedbackCollector(KnowledgeStore knowledgeStore, String tableId) {
        this.knowledgeStore = knowledgeStore;
        this.tableId = tableId;
    }

    public String tableId() {
        return tableId;
    }

    public void recordFeedback(FeedbackRecord record) {
        if (record == null) return;
        FeedbackRecord toPersist =
                record.recordId() == null || record.recordId().isBlank()
                        ? FeedbackRecord.builder()
                                .recordId(UUID.randomUUID().toString())
                                .query(record.query())
                                .retrievedHitIds(record.retrievedHitIds())
                                .userExpectedHitId(record.userExpectedHitId())
                                .feedbackType(record.feedbackType())
                                .correctionText(record.correctionText())
                                .timestamp(
                                        record.timestamp() == null
                                                ? Instant.now()
                                                : record.timestamp())
                                .metadata(record.metadata())
                                .build()
                        : record;
        try {
            byte[] key = (KEY_PREFIX + toPersist.recordId()).getBytes(StandardCharsets.UTF_8);
            knowledgeStore.put(key, MAPPER.writeValueAsBytes(toPersist));
            if (counter.get() >= 0) {
                counter.incrementAndGet();
            }
            log.debug(
                    "RetrievalFeedbackCollector.recordFeedback persisted recordId={} type={}",
                    toPersist.recordId(),
                    toPersist.feedbackType());
        } catch (IOException e) {
            log.warn(
                    "RetrievalFeedbackCollector.recordFeedback failed for recordId={}: {}",
                    toPersist.recordId(),
                    e.getMessage());
        }
    }

    public List<FeedbackRecord> scanRecent(int limit, Instant since) {
        List<FeedbackRecord> result = new ArrayList<>();
        try (CloseableIterator<KeyValue> it =
                knowledgeStore.scan(KEY_PREFIX.getBytes(StandardCharsets.UTF_8))) {
            while (it.hasNext() && (limit <= 0 || result.size() < limit)) {
                KeyValue kv = it.next();
                try {
                    FeedbackRecord record = MAPPER.readValue(kv.value(), FeedbackRecord.class);
                    if (since == null
                            || record.timestamp() == null
                            || !record.timestamp().isBefore(since)) {
                        result.add(record);
                    }
                } catch (IOException parseErr) {
                    log.debug(
                            "RetrievalFeedbackCollector.scanRecent parse failed: {}",
                            parseErr.getMessage());
                }
            }
        } catch (IOException e) {
            log.warn("RetrievalFeedbackCollector.scanRecent failed: {}", e.getMessage());
        }
        return result;
    }

    public long count() {
        long cached = counter.get();
        if (cached >= 0) {
            return cached;
        }
        return recount();
    }

    public long recount() {
        long total = 0;
        try (CloseableIterator<KeyValue> it =
                knowledgeStore.scan(KEY_PREFIX.getBytes(StandardCharsets.UTF_8))) {
            while (it.hasNext()) {
                it.next();
                total++;
            }
        } catch (IOException e) {
            log.warn("RetrievalFeedbackCollector.recount failed: {}", e.getMessage());
            return counter.get() < 0 ? 0L : counter.get();
        }
        counter.set(total);
        return total;
    }
}
