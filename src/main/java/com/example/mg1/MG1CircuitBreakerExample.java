package com.example.mg1;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * M/G/1 Queue simulation with Resilience4j Circuit Breaker.
 *
 * M/G/1 means:
 *   - M: Poisson arrival process (memoryless inter-arrival times) — same as M/M/1
 *   - G: General service time distribution — this is what changes vs M/M/1
 *   - 1: Single server
 *
 * In simpler terms, we have expected outcomes for a service, say a simple lookup would take 5-20ms (The G) but we don't control
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
 * 
 * Simple Terms (just for my own understanding):
 * Deterministic - a system with totally predictable processing time
 * Exponential    - a system where EVERY query has an ongoing chance of being slow
 * Lognormal      - a system that's normally fast but occasionally hits something expensive (a full table scan, a cold cache, etc)
 *
 * Modified values:
 * Stable     - LAMBDA = 45.0 | MU = 60
 * Stressed   - LAMBDA = 54.0 | MU = 60
 * Overloaded - LAMBDA = 80.0 | MU = 60
 *
 * ── ARCHITECTURE NOTE — why this version added a real queue ──────────────
 * Earlier versions sampled a service time and slept for that long on each
 * request independently — but never made later requests WAIT behind an
 * already-busy server. That means no actual queueing delay ever built up,
 * so the simulation could never reproduce the Wq the P-K formula predicts,
 * no matter how high SIGMA_LN was pushed.
 *
 * This version fixes that with a real single-server queue (same pattern as
 * the M/M/1 example):
 *   - executor (1 thread)         = the single server
 *   - queueSlots (Semaphore)      = the finite waiting room (M/G/1/K)
 *   - future.get(timeoutMs)       = how a caller detects it waited too long
 *       behind other slow requests already in the queue
 *
 * Now if request #50 takes 900ms to service, request #51 actually has to
 * wait behind it before its own service even starts — real queueing delay.
 */
public class MG1CircuitBreakerExample {

    // --- M/G/1 Parameters ---
    static final double LAMBDA    = 54.0;   // arrival rate (requests/second) — "Stressed", rho=0.9
    static final double MU        = 60.0;   // mean service rate (requests/second)
    static final double MEAN_S    = 1.0 / MU;  // average time to serve one request

    // Lognormal parameters — tuned so mean service time = MEAN_S
    // For lognormal: mean = exp(mu_ln + sigma_ln^2/2), so mu_ln = ln(mean) - sigma_ln^2/2
    static final double SIGMA_LN  = 1.5;    // log-scale std dev (controls variance)
    static final double MU_LN     = Math.log(MEAN_S) - (SIGMA_LN * SIGMA_LN) / 2.0;

    // Queue / timeout parameters for the real single-server queue (new)
    static final int    QUEUE_CAPACITY = 30;   // finite waiting room (M/G/1/K)
    static final long   CALL_TIMEOUT_MS = 300; // how long a caller waits before giving up

    static final Random rng = new Random();

    // DEBUG: track the longest service time actually observed during the run
    static long maxServiceSeen = 0;

    // The single server: one thread = one server (the "1" in M/G/1)
    static ExecutorService executor;
    // Tracks current queue occupancy — rejects new arrivals once full
    static Semaphore queueSlots;

    static final AtomicInteger totalServed   = new AtomicInteger(0);
    static final AtomicInteger totalRejected = new AtomicInteger(0);
    static final AtomicInteger totalTimeout  = new AtomicInteger(0);

    // Which service distribution is currently active — set per simulation run
    enum ServiceDist { DETERMINISTIC, EXPONENTIAL, LOGNORMAL }
    static ServiceDist DIST = ServiceDist.LOGNORMAL;

    public static void main(String[] args) throws InterruptedException {
        // Run all three distributions back to back so the variance
        // comparison (DETERMINISTIC < EXPONENTIAL < LOGNORMAL) is visible
        // in one execution, instead of manually editing DIST and recompiling.
        for (ServiceDist dist : ServiceDist.values()) {
            DIST = dist;
            runSimulation();
            Thread.sleep(1000);
        }
    }

    static void runSimulation() throws InterruptedException {

        // Reset per-run state since DIST changes between loop iterations
        executor = Executors.newSingleThreadExecutor();
        queueSlots = new Semaphore(QUEUE_CAPACITY);
        maxServiceSeen = 0;
        totalServed.set(0);
        totalRejected.set(0);
        totalTimeout.set(0);

        // ── 1. Circuit Breaker Configuration (same structure as M/M/1) ────
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
            // Trips OPEN if 50% or more of the last N calls failed outright
            .failureRateThreshold(50)
            // ALSO trip OPEN if 50% or more of the last N calls were "slow"
            // (lowered from 80 -> 50: in M/G/1 a handful of requests stuck
            // behind one slow lognormal request is already a real problem,
            // so we don't want to wait for an extreme 80% before reacting)
            .slowCallRateThreshold(50)
            // A call counts as "slow" if it takes longer than 20ms.
            // Roughly ~1.2x the mean service time (16.7ms) — sensitive
            // enough to catch queueing delay, not just raw service time.
            .slowCallDurationThreshold(Duration.ofMillis(20))
            // Evaluate the last 20 calls as a rolling window
            // (could also use TIME_BASED to look at calls in the last N seconds)
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            .slidingWindowSize(20)
            // Don't make any CB decisions until at least 10 calls have been recorded
            .minimumNumberOfCalls(10)
            // Once OPEN, stay OPEN for 3 seconds before moving to HALF_OPEN
            .waitDurationInOpenState(Duration.ofSeconds(3))
            // When in HALF_OPEN, allow exactly 3 probe requests through.
            // With failureRateThreshold=50% and 3 probes:
            //   1 of 3 fails (33%) -> below 50% -> circuit CLOSES
            //   2 of 3 fail  (66%) -> above 50% -> circuit goes back OPEN
            // 3 is a small sample — one unlucky probe is enough to close
            // the circuit even if the server hasn't fully recovered. A
            // production system would likely use 5-10 probes for a more
            // statistically reliable recovery decision.
            .permittedNumberOfCallsInHalfOpenState(3)
            .recordExceptions(RuntimeException.class, TimeoutException.class, ExecutionException.class)
            .ignoreExceptions(NoSuchElementException.class)
            .build();

        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(config);
        CircuitBreaker cb = registry.circuitBreaker("mg1-backend-" + DIST);

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
        // this will be used to time each call as well as check Burke's theorem at the end
        List<Long> departureTimestamps = new ArrayList<>();
        long simDuration = 15_000;
        long start = System.currentTimeMillis();
        int requestId = 0;

        System.out.println("=== M/G/1 Simulation starting (lambda=" + LAMBDA + ", mu=" + MU + ", dist=" + DIST + ") ===");
        printTheory();

        while (System.currentTimeMillis() - start < simDuration) {
            requestId++;
            final int id = requestId;

            // Arrivals are still Poisson — the G only changes service times
            long interArrivalMs = (long) (exponential(LAMBDA) * 1000);
            Thread.sleep(Math.max(1, interArrivalMs));

            // Wrap the call to the real queue with the circuit breaker.
            // processRequest() is what actually waits behind the single
            // server if it's still busy — this is the real queueing delay.
            try {
                String result = CircuitBreaker.decorateCheckedSupplier(cb, () -> processRequest(id)).get();
                served.incrementAndGet();
                // Record the time when the request was served for Burke's theorem check later
                departureTimestamps.add(System.currentTimeMillis());
            } catch (io.github.resilience4j.circuitbreaker.CallNotPermittedException e) {
                // Circuit is OPEN -- call rejected immediately (fail-fast)
                blocked.incrementAndGet();
            } catch (Throwable e) {
                // Call went through but failed (timeout/rejection);
                // CB records this as a failure in its sliding window
            }
        }

        // ── 4. Results ────────────────────────────────────────────────────
        System.out.println("\n=== Simulation complete ===");
        System.out.printf("Distribution: %s%n", DIST);
        System.out.printf("Served:  %d requests%n", served.get());
        System.out.printf("Blocked: %d requests (shed by circuit breaker)%n", blocked.get());
        System.out.printf("Rejected (queue full): %d%n", totalRejected.get());
        System.out.printf("Timed out (queueing delay): %d%n", totalTimeout.get());
        System.out.printf("Final CB state: %s%n", cb.getState());

        CircuitBreaker.Metrics m = cb.getMetrics();
        System.out.printf("Failure rate:   %.1f%%%n", m.getFailureRate());
        System.out.printf("Slow call rate: %.1f%%%n", m.getSlowCallRate());

        // DEBUG: see the actual longest service time observed
        System.out.printf("Max service time observed: %d ms%n", maxServiceSeen);

        checkBurkesTheorem(departureTimestamps);

        executor.shutdownNow();
    }

    /**
     * Submit a request to the single-server queue and wait for it to be
     * served. This is what makes queueing delay real: if the server is
     * still busy with a previous (possibly slow) request, this call sits
     * in the executor's internal queue until that one finishes.
     *
     * Throws RuntimeException on queue-full rejection or on timeout if the
     * wait (queueing delay + service time) exceeds CALL_TIMEOUT_MS.
     */
    static String processRequest(int requestId) throws Exception {
        if (!queueSlots.tryAcquire()) {
            totalRejected.incrementAndGet();
            throw new RuntimeException("Request #" + requestId + " REJECTED — queue full");
        }

        Future<String> future = executor.submit(() -> {
            long serviceMs = sampleServiceTimeMs();
            maxServiceSeen = Math.max(maxServiceSeen, serviceMs);
            Thread.sleep(serviceMs);
            queueSlots.release();
            totalServed.incrementAndGet();
            return "served in " + serviceMs + "ms";
        });

        try {
            return future.get(CALL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            queueSlots.release();
            totalTimeout.incrementAndGet();
            throw new RuntimeException("Request #" + requestId + " TIMEOUT after " + CALL_TIMEOUT_MS + "ms");
        }
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
        return Math.max(1, (long) (seconds * 1000));
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
        double rho = LAMBDA * MEAN_S;

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
        double lq = (LAMBDA * LAMBDA * eS2) / (2.0 * (1 - rho)); // will cause an issue if lambda > mu
        double wq = lq / LAMBDA;
        double w  = wq + MEAN_S;
        double l  = LAMBDA * w;

        System.out.printf("E[S]  (mean service time):  %.1f ms%n", MEAN_S * 1000);
        System.out.printf("E[S^2](2nd moment):         %.6f s^2%n", eS2);
        System.out.printf("Lq (mean queue length):     %.3f jobs%n", lq);
        System.out.printf("L  (mean jobs in system):   %.3f jobs%n", l);
        System.out.printf("Wq (mean wait in queue):    %.1f ms%n", wq * 1000);
        System.out.printf("W  (mean time in system):   %.1f ms%n", w  * 1000);
        System.out.println("---------------------------------------------------\n");
    }

    /**
     * Burke's theorem states that for a STABLE M/M/1 queue, the departure
     * process is itself Poisson with the same rate lambda as the arrivals.
     * This is what allows tandem M/M/1 modules to each be analyzed
     * independently (see Bradford & Jannu, "Circuit-breakers in tandem
     * services").
     *
     * Burke's theorem does NOT generally hold for M/G/1 — once service
     * times are no longer exponential (memoryless), the departure process
     * loses the Poisson property. We check this empirically by recording
     * inter-departure times and comparing their coefficient of variation
     * (CV = std dev / mean) against 1.0, which is what a true
     * Poisson/exponential process would produce.
     */
    static void checkBurkesTheorem(List<Long> departureTimestamps) {
        if (departureTimestamps.size() < 10) {
            System.out.println("Not enough departures recorded to check Burke's theorem.\n");
            return;
        }

        // Compute inter-departure times in seconds 
        List<Double> interDepartures = new ArrayList<>();
        for (int i = 1; i < departureTimestamps.size(); i++) {
            interDepartures.add((departureTimestamps.get(i) - departureTimestamps.get(i - 1)) / 1000.0);
        }

        double mean = interDepartures.stream().mapToDouble(d -> d).average().orElse(0);
        double variance = interDepartures.stream()
                .mapToDouble(d -> Math.pow(d - mean, 2))
                .average().orElse(0);
        double stdDev = Math.sqrt(variance);
        double cv = stdDev / mean;

        System.out.println("--- Burke's Theorem Check (departure process) ---");
        System.out.printf("Expected mean inter-departure (1/lambda): %.4f s%n", 1.0 / LAMBDA);
        System.out.printf("Observed mean inter-departure:            %.4f s%n", mean);
        System.out.printf("Observed std dev:                         %.4f s%n", stdDev);
        System.out.printf("Coefficient of variation (CV):            %.3f%n", cv);
        System.out.println("(CV = 1.0 would indicate a Poisson/exponential departure process,");
        System.out.println(" matching Burke's theorem. CV far from 1.0 means the departure");
        System.out.println(" process is NOT Poisson -- Burke's theorem does not hold for this");
        System.out.println(" service distribution, which is expected for M/G/1 with high variance.)");
        System.out.println("---------------------------------------------------\n");
    }
}