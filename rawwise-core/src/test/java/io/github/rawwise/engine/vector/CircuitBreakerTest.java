package io.github.rawwise.engine.vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("CircuitBreaker - status test")
class CircuitBreakerTest {

    private CircuitBreaker breaker;

    @BeforeEach
    void setUp() {
        breaker = new CircuitBreaker(3, 200);
    }

    @Nested
    @DisplayName("CLOSEDstatus")
    class ClosedState {

        @Test
        @DisplayName("statusisCLOSED")
        void initialStateIsClosed() {
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
        }

        @Test
        @DisplayName("CLOSEDin statebeforeRequestdoes not throw exception")
        void beforeRequestDoesNotThrow() {
            assertThatCode(() -> breaker.beforeRequest()).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("CLOSEDin stateonSuccessdecayfailureCount")
        void onSuccessDecrementsFailureCount() {
            breaker.onFailure();
            breaker.onFailure();
            assertThat(breaker.getFailureCount()).isEqualTo(2);

            breaker.onSuccess();
            assertThat(breaker.getFailureCount()).isEqualTo(1);
        }

        @Test
        @DisplayName("failureCount 0")
        void failureCountDoesNotGoBelowZero() {
            breaker.onSuccess();
            assertThat(breaker.getFailureCount()).isEqualTo(0);
        }

        @Test
        @DisplayName("consecutive failures reach threshold isOPEN")
        void consecutiveFailuresOpenCircuit() {
            breaker.onFailure();
            breaker.onFailure();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);

            breaker.onFailure();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        }

        @Test
        @DisplayName("success failureCount tothreshold")
        void successPreventsThresholdReach() {
            breaker.onFailure();
            breaker.onFailure();
            breaker.onSuccess();
            breaker.onFailure();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
            assertThat(breaker.getFailureCount()).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("OPENstatus")
    class OpenState {

        @BeforeEach
        void tripBreaker() {
            breaker.onFailure();
            breaker.onFailure();
            breaker.onFailure();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        }

        @Test
        @DisplayName("OPENin statebeforeRequest EmbeddingUnavailableException")
        void beforeRequestThrowsInOpenState() {
            assertThatThrownBy(() -> breaker.beforeRequest())
                    .isInstanceOf(EmbeddingUnavailableException.class)
                    .hasMessageContaining("OPEN");
        }

        @Test
        @DisplayName("OPENin stateonFailuremaintainOPEN")
        void onFailureKeepsOpen() {
            breaker.onFailure();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        }

        @Test
        @DisplayName("OPENin stateonSuccess status")
        void onSuccessDoesNotChangeOpenState() {
            breaker.onSuccess();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        }

        @Test
        @DisplayName("timeoutafter isHALF_OPEN")
        void transitionsToHalfOpenAfterTimeout() throws InterruptedException {
            Thread.sleep(250);
            breaker.beforeRequest();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.HALF_OPEN);
        }

        @Test
        @DisplayName("not yettimeout isHALF_OPEN")
        void doesNotTransitionBeforeTimeout() {
            assertThatThrownBy(() -> breaker.beforeRequest())
                    .isInstanceOf(EmbeddingUnavailableException.class);
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        }
    }

    @Nested
    @DisplayName("HALF_OPENstatus")
    class HalfOpenState {

        @BeforeEach
        void transitionToHalfOpen() throws InterruptedException {
            breaker.onFailure();
            breaker.onFailure();
            breaker.onFailure();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
            Thread.sleep(250);
            breaker.beforeRequest();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.HALF_OPEN);
        }

        @Test
        @DisplayName("HALF_OPENin statebeforeRequest exception(rowin)")
        void beforeRequestThrowsInHalfOpen() {
            assertThatThrownBy(() -> breaker.beforeRequest())
                    .isInstanceOf(EmbeddingUnavailableException.class)
                    .hasMessageContaining("HALF_OPEN");
        }

        @Test
        @DisplayName("HALF_OPEN onSuccess CLOSED failureCount")
        void onSuccessTransitionsToClosed() {
            breaker.onSuccess();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
            assertThat(breaker.getFailureCount()).isEqualTo(0);
        }

        @Test
        @DisplayName("HALF_OPEN onFailure OPEN")
        void onFailureTransitionsToOpen() {
            breaker.onFailure();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        }
    }

    @Nested
    @DisplayName("lifecycle")
    class Lifecycle {

        @Test
        @DisplayName("CLOSED->OPEN->HALF_OPEN->CLOSEDfull cycle")
        void fullCycleClosedOpenHalfOpenClosed() throws InterruptedException {
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);

            breaker.onFailure();
            breaker.onFailure();
            breaker.onFailure();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);

            Thread.sleep(250);
            breaker.beforeRequest();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.HALF_OPEN);

            breaker.onSuccess();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
            assertThat(breaker.getFailureCount()).isEqualTo(0);
        }

        @Test
        @DisplayName("CLOSED->OPEN->HALF_OPEN->OPENfallback cycle")
        void halfOpenFallsBackToOpen() throws InterruptedException {
            breaker.onFailure();
            breaker.onFailure();
            breaker.onFailure();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);

            Thread.sleep(250);
            breaker.beforeRequest();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.HALF_OPEN);

            breaker.onFailure();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);

            assertThatThrownBy(() -> breaker.beforeRequest())
                    .isInstanceOf(EmbeddingUnavailableException.class);
        }

        @Test
        @DisplayName("recoveryafter trigger")
        void reTripAfterRecovery() throws InterruptedException {
            breaker.onFailure();
            breaker.onFailure();
            breaker.onFailure();
            Thread.sleep(250);
            breaker.beforeRequest();
            breaker.onSuccess();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);

            breaker.onFailure();
            breaker.onFailure();
            breaker.onFailure();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        }
    }

    @Nested
    @DisplayName("concurrent")
    class Concurrency {

        @Test
        @DisplayName("multi-threaded concurrenttrigger")
        void concurrentFailureTransition() throws Exception {
            int threadCount = 10;
            var threads = new Thread[threadCount];
            for (int i = 0; i < threadCount; i++) {
                threads[i] = new Thread(breaker::onFailure);
            }
            for (Thread t : threads) t.start();
            for (Thread t : threads) t.join();

            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        }

        @Test
        @DisplayName("multi-threaded concurrentHALF_OPEN->CLOSED has itemsuccess")
        void concurrentHalfOpenToClose() throws Exception {
            breaker.onFailure();
            breaker.onFailure();
            breaker.onFailure();
            Thread.sleep(250);
            breaker.beforeRequest();
            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.HALF_OPEN);

            int threadCount = 5;
            var threads = new Thread[threadCount];
            for (int i = 0; i < threadCount; i++) {
                threads[i] = new Thread(breaker::onSuccess);
            }
            for (Thread t : threads) t.start();
            for (Thread t : threads) t.join();

            assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);
            assertThat(breaker.getFailureCount()).isEqualTo(0);
        }
    }
}
