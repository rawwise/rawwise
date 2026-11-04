package io.github.rawwise.axiom.pipeline.feedback.retrieval;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@Slf4j
@DisplayName("RerankerAbSwitcher E2E - deterministic AB routing")
class RerankerAbSwitcherE2ETest {

    @Test
    @DisplayName(
            "abTrafficPercent=0.10 -> approximately 10% of 1000 requestIds choose new model (tolerance ±3%)")
    void tenPercentTrafficRouting() {
        RerankerAbSwitcher switcher = new RerankerAbSwitcher(0.10);
        int n = 1000;
        int newModelCount = 0;
        for (int i = 0; i < n; i++) {
            String id = UUID.nameUUIDFromBytes(("req-" + i).getBytes()).toString();
            if (switcher.useNewModel(id)) newModelCount++;
        }
        double frac = newModelCount / (double) n;
        assertThat(frac).isBetween(0.07, 0.13);
    }

    @Test
    @DisplayName("promoteToFull -> all requests use new model")
    void promoteToFullRoutesAll() {
        RerankerAbSwitcher switcher = new RerankerAbSwitcher(0.10);
        switcher.promoteToFull();
        for (int i = 0; i < 1000; i++) {
            assertThat(switcher.useNewModel("req-" + i)).isTrue();
        }
        assertThat(switcher.getAbTrafficPercent()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("rollback -> no requests use new model")
    void rollbackRoutesNone() {
        RerankerAbSwitcher switcher = new RerankerAbSwitcher(0.50);
        switcher.rollback();
        for (int i = 0; i < 1000; i++) {
            assertThat(switcher.useNewModel("req-" + i)).isFalse();
        }
        assertThat(switcher.getAbTrafficPercent()).isEqualTo(0.0);
    }

    @Test
    @DisplayName("same requestId returns same decision (deterministic stable hash)")
    void deterministicForSameRequestId() {
        RerankerAbSwitcher switcher = new RerankerAbSwitcher(0.50);
        String id = "stable-request-id-abc";
        boolean first = switcher.useNewModel(id);
        for (int i = 0; i < 100; i++) {
            assertThat(switcher.useNewModel(id)).isEqualTo(first);
        }
    }

    @Test
    @DisplayName("null requestId returns false")
    void nullRequestIdReturnsFalse() {
        RerankerAbSwitcher switcher = new RerankerAbSwitcher(1.0);
        assertThat(switcher.useNewModel(null)).isFalse();
    }
}
