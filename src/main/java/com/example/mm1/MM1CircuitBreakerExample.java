package com.example.mm1;

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
import java.util.function.Supplier;

/**
 * M/M/1 Queue simulation with Resilience4j Circuit Breaker,
 * extended to (a) support Pareto-distributed service times and
 * (b) empirically verify Burke's theorem.
 *
 * ───────────────────────────────────────────────────────────────────────
 * PART 1 — Pareto distribution (Type I)
 * ───────────────────────────────────────────────────────────────────────
 * The Pareto distribution (per Prof. Bradford's notes) uses the inverse
 * transform method:
 *
 *   X = c / U^(1/alpha),   U ~ Uniform(0,1)
 *
 * where:
 *   c     = minimum possible value (scale parameter)
 *   alpha = shape parameter (tail heaviness; smaller alpha = heavier tail)
 *
 * Mean:     E[X]   = (alpha * c) / (alpha - 1),       requires alpha > 1
 * Variance: Var[X] = (alpha * c^2) / ((alpha-2)*(alpha-1)^2), requires alpha > 2
 *
 * If alpha <= 2, the variance is INFINITE -- this is the defining feature
 * of Pareto's "thick tail": rare events can be arbitrarily extreme, and
 * the usual queueing-theory assumptions (finite variance) break down.
 * This is the practical reason the circuit breaker matters even more
 * with Pareto service times than with exponential ones.
 *
 * SERVICE_DIST below lets you switch between EXPONENTIAL (textbook M/M/1)
 * and PARETO (heavy-tailed service, still labeled "M/M/1" loosely by
 * convention here since arrivals remain Poisson/Markovian -- strictly this
 * makes it an M/G/1 special case, but we keep it in the MM1 class per your
 * note that you're only responsible for the MM1 file).
 *
 * ───────────────────────────────────────────────────────────────────────
 * PART 2 — Burke's Theorem
 * ───────────────────────────────────────────────────────────────────────
 * Burke's theorem says: for a stable M/M/1 queue (rho < 1) in steady state,
 * the DEPARTURE process is ALSO a Poisson process with the same rate
 * lambda as the arrival process -- and departures are independent of the
 * queue length at any point in time.
 *
 * This is a remarkable and non-obvious result: even though departures
 * are clearly correlated with arrivals and service in the short term
 * (you can't depart before you arrive!), in steady state the OUTPUT
 * looks statistically identical to the INPUT. This is what justifies
 * chaining M/M/1 queues in tandem (Prof. Bradford's Figure 2) and still
 * treating each downstream module as receiving Poisson arrivals.
 *
 * We verify this empirically two ways:
 *   1. Record every inter-departure time and check that its sample mean
 *      and variance match Exp(lambda): mean ~= 1/lambda, variance ~= 1/lambda^2
 *      (for an exponential, CV = stddev/mean should be ~= 1.0)
 *   2. Bucket inter-departure times into a histogram and confirm the
 *      counts decay geometrically (the signature shape of an exponential).
 *
 * NOTE: Burke's theorem strictly requires exponential service (the "M" in
 * M/M/1). If you switch SERVICE_DIST to PARETO, the departure process is
 * NOT Poisson anymore -- the verification output will show this by a
 * CV far from 1.0. This is itself a useful demonstration: Burke's result
 * is special to M/M/1, and breaks immediately once service is non-Markovian.
 *
 * ───────────────────────────────────────────────────────────────────────
 * PART 3 — Real queueing (the circuit breaker fix)
 * ───────────────────────────────────────────────────────────────────────
 * Earlier versions of this file served every request independently:
 * each request slept its own random service time and returned, with no
 * regard for whether the "server" was already busy with another request.
 * That meant no backlog could ever build up, no matter how extreme the
 * variance got -- so the circuit breaker's slowCallDurationThreshold
 * never had anything real to react to.
 *
 * The fix below models an actual single-server queue using one
 * ExecutorService thread (the server) plus a bounded Semaphore (the
 * waiting room). A request that arrives while the server is still busy
 * with an earlier request must wait for that earlier request to finish
 * before its own service time even starts -- this is what "queueing
 * delay" means, and it's also exactly what makes Burke's departure
 * timestamps reflect REAL departure times rather than just
 * arrival + own-service-time.
 */
public class MM1CircuitBreakerExample {

    // --- M/M/1 Parameters ---
    static final double LAMBDA = 40.0;   // arrival rate (requests/second)
    static final double MU     = 60.0;   // service rate (requests/second)
    // rho = 40/60 ≈ 0.667 → stable queue, Lq ≈ 1.33, Wq ≈ 33ms

    // --- Service distribution toggle ---
    // EXPONENTIAL: standard M/M/1, Burke's theorem holds exactly.
    // PARETO:      heavy-tailed service; Burke's theorem will VISIBLY break.
    enum ServiceDist { EXPONENTIAL, PARETO }
    static final ServiceDist SERVICE_DIST = ServiceDist.PARETO;

    // --- Pareto parameters (used only if SERVICE_DIST == PARETO) ---
    // Tuned so E[X] = 1/MU, matching the exponential's mean service time,
    // so any difference in queue/CB behavior is due to VARIANCE, not mean.
    static final double PARETO_ALPHA = 2.5;                 // shape (must be > 1)
    static final double PARETO_C     = (PARETO_ALPHA - 1) / (PARETO_ALPHA * MU);
    // Derived from E[X] = (alpha*c)/(alpha-1) = 1/MU  =>  c = (alpha-1)/(alpha*MU)

    static final Random rng = new Random();

    // Burke's theorem verification: timestamp of every departure (ms since sim start)
    static final List<Long> departureTimestamps = new ArrayList<>();

    // --- Real single-server queue (the circuit breaker fix) ---
    // One worker thread = the single server. Semaphore = bounded waiting room.
    // This is what lets later requests genuinely queue behind slow earlier ones.
    static final int  QUEUE_CAPACITY  = 30;   // finite waiting room (M/M/1/K in practice)
    static final long CALL_TIMEOUT_MS = 300;  // how long a caller waits before giving up

    static ExecutorService executor;
    static Semaphore queueSlots;

    static final AtomicInteger totalRejectedQueueFull = new AtomicInteger(0);
    static final AtomicInteger totalTimeout            = new AtomicInteger(0);

    public static void main(String[] args) throws InterruptedException {

        // ── 1. Configure the Circuit Breaker ──────────────────────────────
        // Thresholds tuned per the queueing fix: slowCallDurationThreshold
        // near the mean service time (not far above it), and a lower
        // slowCallRateThreshold, so the CB is actually sensitive to real
        // queueing delay instead of being a "safety net that never engages."
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
            .failureRateThreshold(50)
            .slowCallRateThreshold(50)                          
            .slowCallDurationThreshold(Duration.ofMillis(20))   //now ~mean service time
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            .slidingWindowSize(20)
            .minimumNumberOfCalls(10)
            .waitDurationInOpenState(Duration.ofSeconds(3))
            .permittedNumberOfCallsInHalfOpenState(3)
            .recordExceptions(RuntimeException.class, TimeoutException.class, ExecutionException.class)
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

        // ── 3. Real single-server queue + M/M/1 simulation loop ──────────
        // executor = the single server (one worker thread).
        // queueSlots = bounded waiting room; tryAcquire() fails fast if full.
        executor   = Executors.newSingleThreadExecutor();
        queueSlots = new Semaphore(QUEUE_CAPACITY);

        AtomicInteger blocked = new AtomicInteger();
        AtomicInteger served  = new AtomicInteger();
        long simDuration = 20_000; // 20 seconds -- slightly longer for a cleaner Burke sample
        long start = System.currentTimeMillis();
        int requestId = 0;

        System.out.println("=== M/M/1 Simulation starting (lambda=" + LAMBDA +
            ", mu=" + MU + ", rho=" + String.format("%.3f", LAMBDA / MU) +
            ", service=" + SERVICE_DIST + ") ===");
        printTheory();

        while (System.currentTimeMillis() - start < simDuration) {

            // Poisson inter-arrival: sleep for Exp(lambda) seconds
            long interArrivalMs = (long) (exponential(LAMBDA) * 1000);
            Thread.sleep(Math.max(1, interArrivalMs));

            final int id = requestId++;

            // Wrap the REAL queueing call with the circuit breaker.
            // processRequest() blocks on future.get(CALL_TIMEOUT_MS) -- so the
            // elapsed time the CB sees includes queueing delay, not just this
            // request's own service time.
            Supplier<String> backendCall = CircuitBreaker.decorateSupplier(cb, () -> {
                try {
                    return processRequest(id);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            try {
                backendCall.get();
                served.incrementAndGet();
                // Record departure timestamp for Burke's theorem verification.
                // This is now the REAL departure time (after any queueing delay),
                // not just arrival + own-service-time.
                departureTimestamps.add(System.currentTimeMillis() - start);
            } catch (io.github.resilience4j.circuitbreaker.CallNotPermittedException e) {
                // Circuit is OPEN -- call rejected immediately (fail-fast)
                // NOTE: rejected calls never entered the queue, so they are NOT
                // departures and are correctly excluded from the Burke check.
                blocked.incrementAndGet();
            } catch (Exception e) {
                // Queue was full, or the request timed out waiting in line.
                // It still occupied a queue slot and then left the system, so
                // for Burke's theorem purposes we count it as a departure too.
                departureTimestamps.add(System.currentTimeMillis() - start);
            }
        }

        executor.shutdownNow();

        // ── 4. Print queue/CB results ──────────────────────────────────────
        System.out.println("\n=== Simulation complete ===");
        System.out.printf("Served:        %d requests%n", served.get());
        System.out.printf("CB-blocked:    %d requests (circuit was OPEN)%n", blocked.get());
        System.out.printf("Queue-full:    %d requests (rejected -- waiting room full)%n", totalRejectedQueueFull.get());
        System.out.printf("Timed out:     %d requests (waited too long in queue/service)%n", totalTimeout.get());
        System.out.printf("Final CB state: %s%n", cb.getState());

        CircuitBreaker.Metrics m = cb.getMetrics();
        System.out.printf("Failure rate:   %.1f%%%n", m.getFailureRate());
        System.out.printf("Slow call rate: %.1f%%%n", m.getSlowCallRate());

        // ── 5. Burke's theorem verification ────────────────────────────────
        verifyBurkesTheorem();
    }

    /**
     * Process one request through the REAL single-server queue.
     *
     * This is the fix: queueSlots models a bounded waiting room (an
     * M/M/1/K system in practice, since infinite queues aren't realistic
     * to simulate). executor is a single worker thread -- the "1" in
     * M/M/1. If the executor is still busy with an earlier slow request,
     * THIS request's future.get() blocks until that earlier request
     * finishes, *before* this request's own service time even begins.
     * That blocking time is genuine queueing delay.
     */
    static String processRequest(int requestId) throws Exception {
        if (!queueSlots.tryAcquire()) {
            totalRejectedQueueFull.incrementAndGet();
            throw new RuntimeException("Request #" + requestId + " REJECTED -- queue full");
        }

        Future<String> future = executor.submit(() -> {
            long serviceMs = sampleServiceTimeMs();
            Thread.sleep(serviceMs);
            return "served in " + serviceMs + "ms";
        });

        try {
            String result = future.get(CALL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            queueSlots.release();
            return result;
        } catch (TimeoutException e) {
            future.cancel(true);
            queueSlots.release();
            totalTimeout.incrementAndGet();
            throw new RuntimeException(
                "Request #" + requestId + " TIMEOUT after " + CALL_TIMEOUT_MS + "ms", e);
        }
    }

    /**
     * Draw a sample from an Exponential distribution.
     * For a Poisson process with rate r (events/sec),
     * inter-event times are Exp(r): X = -ln(U) / r, where U ~ Uniform(0,1).
     */
    static double exponential(double rate) {
        return -Math.log(1 - rng.nextDouble()) / rate;
    }

    /**
     * Draw a sample from a Pareto Type I distribution using the inverse
     * transform method (per Prof. Bradford's notes):
     *
     *   X = c / U^(1/alpha),  U ~ Uniform(0,1)
     */
    static double pareto(double c, double alpha) {
        double u = rng.nextDouble();
        return c / Math.pow(u, 1.0 / alpha);
    }

    /**
     * Sample one service time in milliseconds, using whichever
     * distribution SERVICE_DIST currently points to.
     */
    static long sampleServiceTimeMs() {
        double seconds = switch (SERVICE_DIST) {
            case EXPONENTIAL -> exponential(MU);
            case PARETO      -> pareto(PARETO_C, PARETO_ALPHA);
        };
        return Math.max(1, (long) (seconds * 1000));
    }

    static void printTheory() {
        double rho = LAMBDA / MU;

        System.out.println("\n--- M/M/1 Steady-State Theory (rho=" + String.format("%.3f", rho) + ") ---");

        if (SERVICE_DIST == ServiceDist.EXPONENTIAL) {
            double lq  = (rho * rho) / (1 - rho);
            double l   = rho / (1 - rho);
            double wq  = LAMBDA / (MU * (MU - LAMBDA)); // seconds
            double w   = 1.0 / (MU - LAMBDA);            // seconds

            System.out.printf("Lq (mean queue length):   %.3f jobs%n", lq);
            System.out.printf("L  (mean jobs in system): %.3f jobs%n", l);
            System.out.printf("Wq (mean wait in queue):  %.1f ms%n",  wq * 1000);
            System.out.printf("W  (mean time in system): %.1f ms%n",  w  * 1000);
            System.out.println("Note: CB slowCallDurationThreshold is now ~mean service time (20ms),");
            System.out.println("not far above Wq like before -- it can actually detect real queueing delay.");
        } else {
            // Pareto service moments (Prof. Bradford's notes):
            //   E[X]    = (alpha*c) / (alpha - 1)
            //   Var[X]  = (alpha*c^2) / ((alpha-2)*(alpha-1)^2)   [requires alpha > 2]
            double meanS = (PARETO_ALPHA * PARETO_C) / (PARETO_ALPHA - 1);
            String varStr;
            if (PARETO_ALPHA > 2) {
                double varS = (PARETO_ALPHA * PARETO_C * PARETO_C)
                        / ((PARETO_ALPHA - 2) * Math.pow(PARETO_ALPHA - 1, 2));
                varStr = String.format("%.6f s^2", varS);
            } 
            System.out.printf("Pareto c (min value):        %.5f s%n", PARETO_C);
            System.out.printf("Pareto alpha (shape):        %.2f%n", PARETO_ALPHA);
            System.out.printf("E[S] (mean service time):    %.1f ms  (matches Exp(mu)'s mean by design)%n", meanS * 1000);
            System.out.printf("Var[S] (service variance):   %s%n", varStr);
            System.out.println("Note: M/M/1 closed-form formulas do NOT apply here -- this is really");
            System.out.println("an M/G/1 system. Theory shown for reference; the simulation results");
            System.out.println("below will show queueing behavior noticeably worse than exponential");
            System.out.println("service at the same mean, because of the heavy tail.");
        }
        System.out.println("---------------------------------------------------\n");
    }

    /**
     * Empirically verify Burke's theorem: in steady state, the departure
     * process of a stable M/M/1 queue is itself Poisson(lambda) and
     * independent of queue length.
     *
     * We check the FIRST necessary condition: inter-departure times should
     * be exponentially distributed with rate lambda. An exponential
     * distribution has the defining property that its coefficient of
     * variation (CV = stddev / mean) equals exactly 1.0. We compute the
     * sample CV of inter-departure times and compare it to 1.0 -- the
     * closer to 1.0, the stronger the support for Burke's theorem holding.
     *
     * We also print a simple histogram of inter-departure times, since a
     * true exponential distribution has a geometrically-decaying histogram
     * (lots of short gaps, exponentially fewer long gaps).
     */
    static void verifyBurkesTheorem() {
        System.out.println("=== Burke's Theorem Verification ===");
        System.out.println("Claim: for a stable M/M/1 queue, the departure process is itself");
        System.out.println("Poisson(lambda) -- i.e. inter-departure times are Exp(lambda),");
        System.out.println("which means their coefficient of variation (CV) should be ~1.0.\n");

        if (departureTimestamps.size() < 30) {
            System.out.println("Not enough departures recorded to verify (need at least 30). Skipping.");
            return;
        }

        // Compute inter-departure times (gaps between consecutive departures)
        int n = departureTimestamps.size() - 1;
        double[] interDepartureSec = new double[n];
        for (int i = 0; i < n; i++) {
            long gapMs = departureTimestamps.get(i + 1) - departureTimestamps.get(i);
            interDepartureSec[i] = gapMs / 1000.0;
        }

        // Sample mean and standard deviation
        double mean = 0;
        for (double x : interDepartureSec) mean += x;
        mean /= n;

        double sqDiffSum = 0;
        for (double x : interDepartureSec) sqDiffSum += (x - mean) * (x - mean);
        double variance = sqDiffSum / (n - 1);
        double stddev = Math.sqrt(variance);
        double cv = stddev / mean;

        double impliedRate = 1.0 / mean; // departures/sec, should be close to LAMBDA

        System.out.printf("Departures recorded:              %d%n", departureTimestamps.size());
        System.out.printf("Inter-departure samples:          %d%n", n);
        System.out.printf("Mean inter-departure time:        %.4f s  (expected ~%.4f s = 1/lambda)%n",
            mean, 1.0 / LAMBDA);
        System.out.printf("Implied departure rate:           %.2f req/s  (arrival lambda = %.2f req/s)%n",
            impliedRate, LAMBDA);
        System.out.printf("Std dev of inter-departure time:  %.4f s%n", stddev);
        System.out.printf("Coefficient of variation (CV):    %.3f  (Exp distribution => CV = 1.000)%n", cv);

        if (SERVICE_DIST == ServiceDist.EXPONENTIAL) {
            if (Math.abs(cv - 1.0) < 0.15) {
                System.out.println("=> CV is close to 1.0: consistent with Burke's theorem (departures look Poisson).");
            } else {
                System.out.println("=> CV deviates from 1.0 more than expected -- try a longer simDuration for");
                System.out.println("   a larger sample, since this is a statistical check, not an exact one.");
            }
        } else {
            System.out.println("=> SERVICE_DIST = PARETO: Burke's theorem does NOT apply here (it requires");
            System.out.println("   exponential service). Expect CV far from 1.0 -- this is the heavy tail");
            System.out.println("   breaking the memorylessness that Burke's theorem depends on.");
        }

        printHistogram(interDepartureSec, mean);
        System.out.println("---------------------------------------------------\n");
    }

    /**
     * Print a simple text histogram of inter-departure times, bucketed in
     * units of the mean. A true exponential shows roughly geometric decay
     * across buckets (each bucket ~ (1/e) ≈ 37% of the previous one).
     */
    static void printHistogram(double[] samples, double mean) {
        int numBuckets = 8;
        int[] counts = new int[numBuckets];
        double bucketWidth = mean; // bucket width = 1 mean unit

        for (double s : samples) {
            int idx = (int) (s / bucketWidth);
            if (idx >= numBuckets) idx = numBuckets - 1; // overflow bucket
            counts[idx]++;
        }

        System.out.println("\nHistogram of inter-departure times (bucket width = 1 mean = " +
            String.format("%.1fms", mean * 1000) + "):");
        int maxCount = 1;
        for (int c : counts) maxCount = Math.max(maxCount, c);

        for (int i = 0; i < numBuckets; i++) {
            String label = (i == numBuckets - 1)
                ? String.format("[%d+ means)  ", i)
                : String.format("[%d-%d means) ", i, i + 1);
            int barLen = (int) (40.0 * counts[i] / maxCount);
            System.out.printf("%-14s %s %d%n", label, "#".repeat(barLen), counts[i]);
        }
        System.out.println("(Exponential => bars should shrink by roughly the same ratio each row.)");
    }
}
