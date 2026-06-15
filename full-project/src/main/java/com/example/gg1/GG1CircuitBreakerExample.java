package com.example.gg1;

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
 * G/G/1 Queue simulation with Resilience4j Circuit Breaker.
 *
 * G/G/1 means:
 *   - G: General inter-arrival time distribution (NOT necessarily Poisson)
 *   - G: General service time distribution
 *   - 1: Single server
 *
 * This is the most general single-server queue. Because both distributions
 * are arbitrary, there is NO exact closed-form solution for Lq or Wq.
 * Instead we use the Kingman approximation (also called the VUT equation):
 *
 *   Wq ≈ (rho / (mu * (1 - rho))) * ((ca^2 + cs^2) / 2)
 *
 * Where:
 *   rho = lambda / mu          (utilization)
 *   ca^2 = variance(inter-arrival) / mean(inter-arrival)^2   (squared CV of arrivals)
 *   cs^2 = variance(service)   / mean(service)^2             (squared CV of service)
 *   CV   = coefficient of variation = std_dev / mean
 *
 * Key intuition from Kingman:
 *   - The first term (rho / (mu*(1-rho))) is just the M/M/1 Wq
 *   - The second term ((ca^2 + cs^2)/2) is a MULTIPLIER based on variability
 *   - If both are exponential: ca^2 = cs^2 = 1, so multiplier = 1 → reduces to M/M/1
 *   - Lower variability → multiplier < 1 → shorter queues than M/M/1
 *   - Higher variability → multiplier > 1 → longer queues than M/M/1
 *
 * This simulation uses:
 *   Arrivals:  Normal distribution (bounded away from 0) — more regular than Poisson
 *   Service:   Lognormal distribution — heavy-tailed, high variance
 */
public class GG1CircuitBreakerExample {

    // --- G/G/1 Parameters ---
    static final double LAMBDA      = 40.0;        // mean arrival rate (req/s)
    static final double MU          = 60.0;        // mean service rate (req/s)
    static final double MEAN_IA     = 1.0 / LAMBDA; // mean inter-arrival time (s)
    static final double MEAN_S      = 1.0 / MU;    // mean service time (s)

    // Normal inter-arrival distribution parameters
    // Using CV_A = 0.5 → less bursty than Poisson (which has CV=1)
    static final double CV_A        = 0.5;
    static final double STD_IA      = CV_A * MEAN_IA;  // std dev of inter-arrival

    // Lognormal service distribution parameters (same as M/G/1 example)
    static final double CV_S        = 2.0;             // high variance in service
    static final double SIGMA_LN    = Math.sqrt(Math.log(1 + CV_S * CV_S));
    static final double MU_LN       = Math.log(MEAN_S) - (SIGMA_LN * SIGMA_LN) / 2.0;

    static final Random rng = new Random();

    public static void main(String[] args) throws InterruptedException {

        // ── 1. Configure the Circuit Breaker ──────────────────────────────
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
            .failureRateThreshold(50)
            .slowCallRateThreshold(80)
            .slowCallDurationThreshold(Duration.ofMillis(200))
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            .slidingWindowSize(20)
            .minimumNumberOfCalls(10)
            .waitDurationInOpenState(Duration.ofSeconds(3))
            .permittedNumberOfCallsInHalfOpenState(3)
            .recordExceptions(RuntimeException.class, TimeoutException.class, ExecutionException.class)
            .ignoreExceptions(NoSuchElementException.class)
            .build();

        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(config);
        CircuitBreaker cb = registry.circuitBreaker("gg1-backend");

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

        // ── 3. Simulation loop ────────────────────────────────────────────
        AtomicInteger blocked = new AtomicInteger();
        AtomicInteger served  = new AtomicInteger();
        long simDuration = 15_000;
        long start = System.currentTimeMillis();

        System.out.println("=== G/G/1 Simulation starting (lambda=" + LAMBDA +
            ", mu=" + MU + ", rho=" + String.format("%.3f", LAMBDA / MU) + ") ===");
        System.out.printf("Arrival dist:  Normal(mean=%.1fms, CV=%.1f)%n", MEAN_IA * 1000, CV_A);
        System.out.printf("Service dist:  Lognormal(mean=%.1fms, CV=%.1f)%n", MEAN_S * 1000, CV_S);
        printTheory();

        while (System.currentTimeMillis() - start < simDuration) {

            // G arrivals: Normal inter-arrival time, clipped at 1ms minimum
            // Normal is more regular than Poisson (CV < 1) — models a rate-limited client
            long interArrivalMs = (long)(normalInterArrivalSeconds() * 1000);
            Thread.sleep(Math.max(1, interArrivalMs));

            Supplier<String> backendCall = CircuitBreaker.decorateSupplier(cb, () -> {
                // G service: Lognormal — high variance, heavy tail
                long serviceMs = lognormalServiceTimeMs();

                // Sleep first, then check for timeout (realistic latency before failure)
                try {
                    Thread.sleep(serviceMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                if (serviceMs > 200) {
                    throw new RuntimeException("Service timeout -- server overloaded");
                }

                return "OK after " + serviceMs + "ms";
            });

            try {
                backendCall.get();
                served.incrementAndGet();
            } catch (io.github.resilience4j.circuitbreaker.CallNotPermittedException e) {
                blocked.incrementAndGet();
            } catch (Exception e) {
                // recorded as failure in CB sliding window
            }
        }

        // ── 4. Results ────────────────────────────────────────────────────
        System.out.println("\n=== Simulation complete ===");
        System.out.printf("Served:  %d requests%n", served.get());
        System.out.printf("Blocked: %d requests (shed by circuit breaker)%n", blocked.get());
        System.out.printf("Final CB state: %s%n", cb.getState());

        CircuitBreaker.Metrics m = cb.getMetrics();
        System.out.printf("Failure rate:   %.1f%%%n", m.getFailureRate());
        System.out.printf("Slow call rate: %.1f%%%n", m.getSlowCallRate());
    }

    /**
     * Sample a Normal inter-arrival time in seconds.
     * Uses Box-Muller transform. Clipped at a small positive value
     * since inter-arrival times cannot be negative.
     */
    static double normalInterArrivalSeconds() {
        double u1 = rng.nextDouble();
        double u2 = rng.nextDouble();
        double z  = Math.sqrt(-2.0 * Math.log(u1)) * Math.cos(2.0 * Math.PI * u2);
        double sample = MEAN_IA + STD_IA * z;
        // Clip to avoid negative or zero inter-arrival times
        return Math.max(0.001, sample);
    }

    /**
     * Sample a Lognormal service time in milliseconds.
     * High CV_S means a heavy tail — most calls are fast, but some are very slow.
     */
    static long lognormalServiceTimeMs() {
        double u1 = rng.nextDouble();
        double u2 = rng.nextDouble();
        double z  = Math.sqrt(-2.0 * Math.log(u1)) * Math.cos(2.0 * Math.PI * u2);
        double seconds = Math.exp(MU_LN + SIGMA_LN * z);
        return Math.max(1, (long)(seconds * 1000));
    }

    static void printTheory() {
        double rho  = LAMBDA / MU;
        double ca2  = CV_A * CV_A;   // squared CV of inter-arrival
        double cs2  = CV_S * CV_S;   // squared CV of service

        // Kingman approximation
        double wq_mm1   = rho / (MU * (1 - rho));          // M/M/1 baseline Wq (seconds)
        double kingman   = wq_mm1 * ((ca2 + cs2) / 2.0);   // G/G/1 Wq approximation
        double w         = kingman + MEAN_S;
        double lq        = LAMBDA * kingman;                 // Little's Law
        double l         = LAMBDA * w;

        // Show what M/M/1 would predict for comparison
        double lq_mm1   = (rho * rho) / (1 - rho);

        System.out.println("\n--- G/G/1 Kingman Approximation (rho=" + String.format("%.3f", rho) + ") ---");
        System.out.printf("ca^2 (arrival variability):  %.2f  (1.0 = Poisson)%n", ca2);
        System.out.printf("cs^2 (service variability):  %.2f  (1.0 = Exponential)%n", cs2);
        System.out.printf("Variability multiplier:      %.2fx  vs M/M/1%n", (ca2 + cs2) / 2.0);
        System.out.printf("Lq - M/M/1 baseline:         %.3f jobs%n", lq_mm1);
        System.out.printf("Lq - Kingman G/G/1:          %.3f jobs%n", lq);
        System.out.printf("L  (mean jobs in system):    %.3f jobs%n", l);
        System.out.printf("Wq (mean wait, Kingman):     %.1f ms%n",  kingman * 1000);
        System.out.printf("W  (mean time in system):    %.1f ms%n",  w * 1000);
        System.out.println("Note: Kingman is an approximation; exact G/G/1 has no closed form.");
        System.out.println("---------------------------------------------------\n");
    }
}
