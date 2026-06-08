package com.example.mm1;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * M/M/1 Queue simulation with Resilience4j Circuit Breaker.
 *
 * M/M/1 means:
 *   - M: Poisson arrival process (memoryless inter-arrival times)
 *   - M: Exponential service times (memoryless service)
 *   - 1: Single server
 *
 * Key M/M/1 steady-state formulas (require rho = lambda/mu < 1):
 *   rho = lambda/mu           (utilization / traffic intensity)
 *   Lq  = rho^2 / (1 - rho)  (mean queue length, excluding in-service)
 *   L   = rho / (1 - rho)    (mean total jobs in system)
 *   Wq  = lambda / (mu*(mu - lambda))  (mean waiting time in queue)
 *   W   = 1 / (mu - lambda)            (mean total time in system)
 *
 * The circuit breaker wraps calls to the "backend service" (the server).
 * When the server becomes overloaded (rho -> 1), slow calls accumulate and
 * the circuit breaker trips OPEN, shedding load to protect the system.
 */
public class MM1CircuitBreakerExample {

    // --- M/M/1 Parameters ---
    static final double LAMBDA = 40.0;   // arrival rate (requests/second)
    static final double MU     = 60.0;   // service rate (requests/second)
    // rho = 40/60 ≈ 0.667 → stable queue, Lq ≈ 1.33, Wq ≈ 33ms

    static final Random rng = new Random();

    public static void main(String[] args) throws InterruptedException {

        // ── 1. Configure the Circuit Breaker ──────────────────────────────
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
            // Trip OPEN if >= 50% of calls in the window fail
            .failureRateThreshold(50) 
            // Trip OPEN if >= 25% of calls in the window fail
            //.failureRateThreshold(25)
            // Also trip OPEN if >= 80% of calls exceed slowCallDurationThreshold
            .slowCallRateThreshold(80)
            //.slowCallDurationThreshold(Duration.ofMillis(20))
            .slowCallDurationThreshold(Duration.ofMillis(200))

            // Count-based sliding window of 20 calls
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            .slidingWindowSize(20)
            // Need at least 10 calls recorded before the CB can make a decision
            // (Prevents tripping on the very first failure)
            .minimumNumberOfCalls(10)
            // Stay OPEN for 3 seconds, then probe with HALF_OPEN
            .waitDurationInOpenState(Duration.ofSeconds(3))
            // Allow 3 test calls in HALF_OPEN before deciding to re-CLOSE or stay OPEN
            .permittedNumberOfCallsInHalfOpenState(3)
            // Record backend runtime failures as circuit failures too
            //.recordExceptions(RuntimeException.class, TimeoutException.class, ExecutionException.class)
            .recordExceptions(TimeoutException.class, ExecutionException.class)
            // Business-level "no results" is NOT a circuit-level failure
            .ignoreExceptions(NoSuchElementException.class)
            .build();

        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(config);
        CircuitBreaker cb = registry.circuitBreaker("mm1-backend");

        // ── 2. Subscribe to state-transition events ───────────────────────
        cb.getEventPublisher()
            .onStateTransition(e -> System.out.printf(
                "[CB] %s -> %s%n",
                e.getStateTransition().getFromState(),
                e.getStateTransition().getToState()))
            .onCallNotPermitted(e ->
                System.out.println("[CB] Call REJECTED -- circuit is OPEN"))
            .onError(e -> System.out.printf(
                "[CB] Error recorded: %s (%.1fms)%n",
                e.getThrowable().getClass().getSimpleName(),
                e.getElapsedDuration().toMillis() * 1.0))
            .onSlowCallRateExceeded(e -> System.out.printf(
                "[CB] Slow call rate threshold breached: %.1f%%%n", e.getSlowCallRate()))
            .onFailureRateExceeded(e -> System.out.printf(
                "[CB] Failure rate threshold breached: %.1f%%%n", e.getFailureRate()));

        // ── 3. Single-threaded M/M/1 simulation loop ─────────────────────
        AtomicInteger blocked = new AtomicInteger();
        AtomicInteger served  = new AtomicInteger();
        long simDuration = 15_000; // 15 seconds of wall time
        long start = System.currentTimeMillis();

        System.out.println("=== M/M/1 Simulation starting (lambda=" + LAMBDA +
            ", mu=" + MU + ", rho=" + String.format("%.3f", LAMBDA / MU) + ") ===");
        printTheory();

        while (System.currentTimeMillis() - start < simDuration) {

            // Poisson inter-arrival: sleep for Exp(lambda) seconds
            long interArrivalMs = (long) (exponential(LAMBDA) * 1000);
            Thread.sleep(Math.max(1, interArrivalMs));

            // Wrap the backend call with the circuit breaker
            Supplier<String> backendCall = CircuitBreaker.decorateSupplier(cb, () -> {
                // Exponential service time: Exp(mu) seconds
                long serviceMs = (long) (exponential(MU) * 1000);

                // Simulate overload: service times > 20ms now fail fast.
                // This makes tripping OPEN visible in a short demo run.
                if (serviceMs > 20) {
                    throw new RuntimeException("Service timeout -- server overloaded");
                }

                try {
                    Thread.sleep(serviceMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return "OK after " + serviceMs + "ms";
            });

            try {
                backendCall.get();
                served.incrementAndGet();
            } catch (io.github.resilience4j.circuitbreaker.CallNotPermittedException e) {
                // Circuit is OPEN -- call rejected immediately (fail-fast)
                blocked.incrementAndGet();
            } catch (Exception e) {
                // Call went through but the backend failed;
                // CB records this as a failure in its sliding window
            }
        }

        // ── 4. Print results ──────────────────────────────────────────────
        System.out.println("\n=== Simulation complete ===");
        System.out.printf("Served:  %d requests%n", served.get());
        System.out.printf("Blocked: %d requests (shed by circuit breaker)%n", blocked.get());
        System.out.printf("Final CB state: %s%n", cb.getState());

        CircuitBreaker.Metrics m = cb.getMetrics();
        System.out.printf("Failure rate:   %.1f%%%n", m.getFailureRate());
        System.out.printf("Slow call rate: %.1f%%%n", m.getSlowCallRate());
    }

    /**
     * Draw a sample from an Exponential distribution.
     * For a Poisson process with rate r (events/sec),
     * inter-event times are Exp(r): X = -ln(U) / r, where U ~ Uniform(0,1).
     */
    static double exponential(double rate) {
        return -Math.log(1 - rng.nextDouble()) / rate;
    }

    static void printTheory() {
        double rho = LAMBDA / MU;
        double lq  = (rho * rho) / (1 - rho);
        double l   = rho / (1 - rho);
        double wq  = LAMBDA / (MU * (MU - LAMBDA)); // seconds
        double w   = 1.0 / (MU - LAMBDA);            // seconds

        System.out.println("\n--- M/M/1 Steady-State Theory (rho=" + String.format("%.3f", rho) + ") ---");
        System.out.printf("Lq (mean queue length):   %.3f jobs%n", lq);
        System.out.printf("L  (mean jobs in system): %.3f jobs%n", l);
        System.out.printf("Wq (mean wait in queue):  %.1f ms%n",  wq * 1000);
        System.out.printf("W  (mean time in system): %.1f ms%n",  w  * 1000);
        System.out.println("Note: CB slow-call threshold (200ms) > Wq -- CB is a safety net, not a bottleneck.");
        System.out.println("---------------------------------------------------\n");
    }
}