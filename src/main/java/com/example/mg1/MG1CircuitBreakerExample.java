package com.example.mg1;

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
 * M/G/1 Queue simulation with Resilience4j Circuit Breaker.
 *
 * M/G/1 means:
 *   - M: Poisson arrival process (memoryless inter-arrival times) — same as M/M/1
 *   - G: General service time distribution — this is what changes vs M/M/1
 *   - 1: Single server
 * 
 * In simpler terms, we have expected outcomes for a service, say a simple lookup would take 5-20ms (The G) but we don't controk
 * when a request comes in, and so these calls can come any time but we know how much time each call will take.
 *
 * Because service times are no longer exponential, we can no longer use
 * the simple M/M/1 closed-form formulas. Instead we use the
 * Pollaczek-Khinchine (P-K) mean value formula:
 *
 *   rho  = lambda * E[S]               (utilization)
 *   Lq   = (lambda^2 * E[S^2]) / (2 * (1 - rho))   (mean queue length)
 *   Wq   = Lq / lambda                 (mean wait, by Little's Law)
 *   W    = Wq + E[S]                   (mean time in system)
 *   L    = lambda * W                  (mean jobs in system, by Little's Law)
 *
 * Where E[S] = mean service time, E[S^2] = second moment of service time.
 *
 * The key insight: variance in service time makes queues WORSE.
 * For the same mean service time (same rho), a high-variance service
 * distribution (e.g. lognormal, Pareto) produces longer queues than
 * exponential, which in turn produces longer queues than deterministic.
 * This is captured in the P-K formula via E[S^2].
 *
 * We demonstrate three service distributions with the same mean (1/mu):
 *   1. Deterministic (constant) — lowest variance, shortest queues
 *   2. Exponential  — medium variance (reduces to M/M/1)
 *   3. Lognormal    — high variance, longest queues
 */
public class MG1CircuitBreakerExample {

    // --- M/G/1 Parameters ---
    static final double LAMBDA    = 40.0;   // arrival rate (requests/second)
    static final double MU        = 60.0;   // mean service rate (requests/second)
    static final double MEAN_S    = 1.0 / MU;  // average time to serve one request

    // Lognormal parameters — tuned so mean service time = MEAN_S
    // For lognormal: mean = exp(mu_ln + sigma_ln^2/2), so mu_ln = ln(mean) - sigma_ln^2/2
    static final double SIGMA_LN  = 0.8;    // log-scale std dev (controls variance)
    static final double MU_LN     = Math.log(MEAN_S) - (SIGMA_LN * SIGMA_LN) / 2.0;

    static final Random rng = new Random();

    // Which service distribution to use — CHANGE THIS TO COMPARE - Question could we possibly run these all together and compare them by using threads?
    enum ServiceDist { DETERMINISTIC, EXPONENTIAL, LOGNORMAL }
    static final ServiceDist DIST = ServiceDist.LOGNORMAL;

    public static void main(String[] args) throws InterruptedException {

        // ── 1. Circuit Breaker Configuration (same structure as M/M/1) ────
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
            // Trips OPEN if 50% or more of the last N calls failed outright
            .failureRateThreshold(50)
            // ALSO trip OPEN if 80% or more of the last N calls were "slow"
            .slowCallRateThreshold(80)
            // A call counts as "slow" if it takes longer than 200ms - important to a MG1 queue type
            .slowCallDurationThreshold(Duration.ofMillis(200))
            // Evaluate the last 20 calls as a rolling window - we also use COUNT_BASED (regardless of time)
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            .slidingWindowSize(20)
            // Don't make any CB decisions until at least 10 calls have been recorded
            .minimumNumberOfCalls(10)
            // Once OPEN, stay OPEN for 3 seconds before moving to HALF_OPEN
            .waitDurationInOpenState(Duration.ofSeconds(3))
            // When in HALF_OPEN, allow exactly 3 probe requests through - if succeed go CLOSED if fail go OPEN again
            .permittedNumberOfCallsInHalfOpenState(3)

            .recordExceptions(RuntimeException.class, TimeoutException.class, ExecutionException.class)
            .ignoreExceptions(NoSuchElementException.class)
            
            .build();

        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(config);
        CircuitBreaker cb = registry.circuitBreaker("mg1-backend");

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

        System.out.println("=== M/G/1 Simulation starting (lambda=" + LAMBDA + ", mu=" + MU + ", dist=" + DIST + ") ===");
        printTheory();

        while (System.currentTimeMillis() - start < simDuration) {

            // Arrivals are still Poisson — the G only changes service times
            long interArrivalMs = (long)(exponential(LAMBDA) * 1000);
            Thread.sleep(Math.max(1, interArrivalMs));

            Supplier<String> backendCall = CircuitBreaker.decorateSupplier(cb, () -> {
                // Sample service time from chosen distribution
                long serviceMs = sampleServiceTimeMs();

                // Sleep first (latency before failure — same fix as M/M/1)
                try {
                    Thread.sleep(serviceMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                // removed since we already have a config in our circuit breaker that's wacthing the call's duration
                // if (serviceMs > 200) {
                //     throw new RuntimeException("Service timeout -- server overloaded");
                // }

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
        System.out.printf("Distribution: %s%n", DIST);
        System.out.printf("Served:  %d requests%n", served.get());
        System.out.printf("Blocked: %d requests (shed by circuit breaker)%n", blocked.get());
        System.out.printf("Final CB state: %s%n", cb.getState());

        CircuitBreaker.Metrics m = cb.getMetrics();
        System.out.printf("Failure rate:   %.1f%%%n", m.getFailureRate());
        System.out.printf("Slow call rate: %.1f%%%n", m.getSlowCallRate());
    }

    /**
     * Sample a service time in milliseconds from the chosen distribution.
     * All distributions are tuned to have the same mean (MEAN_S seconds).
     */
    static long sampleServiceTimeMs() {
        double seconds = switch (DIST) {
            // Deterministic: always exactly MEAN_S. Zero variance.
            // P-K formula: E[S^2] = E[S]^2, so Lq = rho^2 / (2*(1-rho)) — half of M/M/1!
            case DETERMINISTIC -> MEAN_S;

            // Exponential: same as M/M/1. Variance = 1/mu^2.
            // E[S^2] = 2/mu^2, which gives the standard M/M/1 Lq formula.
            case EXPONENTIAL -> exponential(MU);

            // Lognormal: heavy-tailed, high variance. Models realistic workloads
            // where most requests are fast but some are very slow (GC pauses,
            // cold starts, etc). Variance = (exp(sigma^2)-1) * exp(2*mu_ln + sigma^2)
            case LOGNORMAL -> lognormal();
        };
        return Math.max(1, (long)(seconds * 1000));
    }

    /**
     * Sample from Exponential(rate) using inverse transform method.
     * Same as M/M/1 — Poisson arrivals are unchanged in M/G/1.
     */
    static double exponential(double rate) {
        return -Math.log(1 - rng.nextDouble()) / rate;
    }

    /**
     * Sample from Lognormal(MU_LN, SIGMA_LN) using Box-Muller transform.
     * If Z ~ Normal(0,1), then exp(MU_LN + SIGMA_LN * Z) ~ Lognormal(MU_LN, SIGMA_LN).
     */
    static double lognormal() {
        // Box-Muller: transform two uniform samples into a standard normal
        double u1 = rng.nextDouble();
        double u2 = rng.nextDouble();
        double z  = Math.sqrt(-2.0 * Math.log(u1)) * Math.cos(2.0 * Math.PI * u2);
        return Math.exp(MU_LN + SIGMA_LN * z);
    }

    static void printTheory() {
        double rho   = LAMBDA * MEAN_S;

        System.out.println("\n--- M/G/1 P-K Theory (" + DIST + ", rho=" + String.format("%.3f", rho) + ") ---");

        // Guard: P-K formula only valid when rho < 1
        // this happens when our lambda > mu
        if (rho >= 1.0) {
            System.out.println("UNSTABLE (rho >= 1) — P-K formulas do not apply.");
            System.out.println("Queue grows without bound -> timeouts -> circuit opens.");
            System.out.println("---------------------------------------------------\n");
            return;  // exit early, skip the formula calculations
        }

        // Second moment E[S^2] depends on the distribution
        double eS2 = switch (DIST) {
            // Deterministic: E[S^2] = E[S]^2 (no variance)
            case DETERMINISTIC -> MEAN_S * MEAN_S;
            // Exponential: E[S^2] = 2/mu^2
            case EXPONENTIAL   -> 2.0 / (MU * MU);
            // Lognormal: E[S^2] = exp(2*mu_ln + 2*sigma_ln^2)
            case LOGNORMAL     -> Math.exp(2 * MU_LN + 2 * SIGMA_LN * SIGMA_LN);
        };

        // P-K mean value formula
        double lq  = (LAMBDA * LAMBDA * eS2) / (2.0 * (1 - rho)); // will cause an issue if lambda > mu
        double wq  = lq / LAMBDA;
        double w   = wq + MEAN_S;
        double l   = LAMBDA * w;

        // System.out.println("\n--- M/G/1 P-K Theory (" + DIST + ", rho=" + String.format("%.3f", rho) + ") ---");
        System.out.printf("E[S]  (mean service time):  %.1f ms%n", MEAN_S * 1000);
        System.out.printf("E[S^2](2nd moment):         %.6f s^2%n", eS2);
        System.out.printf("Lq (mean queue length):     %.3f jobs%n", lq);
        System.out.printf("L  (mean jobs in system):   %.3f jobs%n", l);
        System.out.printf("Wq (mean wait in queue):    %.1f ms%n",   wq * 1000);
        System.out.printf("W  (mean time in system):   %.1f ms%n",   w  * 1000);
        System.out.println("Tip: Change DIST to DETERMINISTIC or EXPONENTIAL to compare Lq.");
        System.out.println("---------------------------------------------------\n");
    }
}
