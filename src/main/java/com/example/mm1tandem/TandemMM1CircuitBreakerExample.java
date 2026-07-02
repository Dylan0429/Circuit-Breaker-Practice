package com.example.mm1tandem;

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
 * Tandem sequence of M/M/1 modules with Resilience4j circuit breakers
 *
 * ───────────────────────────────────────────────────────────────────────
 * What "tandem" means here
 * ───────────────────────────────────────────────────────────────────────
 * Modules are chained IN SERIES:
 *
 *   [Poisson source]
 *       │ lambda arrivals/s
 *       ▼
 *   [Module 0: CB → Queue → Service]
 *       │ departures (or nothing, if CB blocked)
 *       ▼
 *   [Module 1: CB → Queue → Service]
 *       │ departures (or nothing, if CB blocked)
 *       ▼
 *      ...
 *       ▼
 *   [Module N-1: CB → Queue → Service]
 *       │ final output
 *       ▼
 *   [System exit]
 *
 * Only module 0 receives external Poisson arrivals. Every downstream
 * module i+1 receives ONLY the requests that successfully DEPARTED from
 * module i -- meaning requests that were CB-blocked, queue-full, or timed
 * out at module i never reach module i+1 at all.
 *
 *
 * ───────────────────────────────────────────────────────────────────────
 * Burke's theorem in the tandem context
 * ───────────────────────────────────────────────────────────────────────
 * Burke's theorem is what justifies calling each downstream module M/M/1
 * rather than G/G/1: if module i is a stable M/M/1 queue in steady state,
 * its output IS Poisson(lambda) -- so module i+1 receives Poisson arrivals
 * at the same rate as the original source, and the tandem is still a chain
 * of M/M/1 queues rather than a general G/G/1 chain.
 *
 * We check Burke's theorem empirically at each module by measuring the CV
 * of inter-departure times from that module. In a healthy tandem:
 *   - Module 0 should show CV ~1.0 (departures are Poisson(lambda))
 *   - Module 1 should ALSO show CV ~1.0 (receives Poisson from module 0)
 *   - ...and so on down the chain
 * If a module's CB trips and disturbs its output, the next module will
 * receive a non-Poisson stream -- visible as CV far from 1.0 in module i's
 * Burke check AND as degraded throughput at module i+1.
 *
 * ───────────────────────────────────────────────────────────────────────
 * How requests flow through the tandem in this code
 * ───────────────────────────────────────────────────────────────────────
 * The main simulation loop drives arrivals according to Poisson(lambda).
 * For each arriving request, it passes it through modules 0, 1, 2, ...
 * in order. If module i REJECTS the request (CB blocked, queue full, or
 * timeout), the request does NOT proceed to module i+1 -- it exits the
 * system at that point. This is the "closed tandem loss system" described
 * in the paper: module i+1 only receives jobs from module i.
 */
public class TandemMM1CircuitBreakerExample {

    // --- System-wide parameters ---
    static final int    NUM_MODULES     = 10;      // chain length (start small to see cascade clearly)
    static final double LAMBDA          = 40.0;   // external arrival rate into module 0
    static final double MU              = 60.0;   // service rate -- same for all modules
    // rho per module = 40/60 ~= 0.667 if no load is shed upstream

    static final long   SIM_DURATION_MS = 20_000;

    // --- Service distribution toggle ---
    enum ServiceDist { EXPONENTIAL, PARETO }
    static final ServiceDist SERVICE_DIST = ServiceDist.PARETO;

    // --- Pareto parameters ---
    static final double PARETO_ALPHA = 2.5;
    static final double PARETO_C     = (PARETO_ALPHA - 1) / (PARETO_ALPHA * MU);

    // --- Real single-server queue parameters ---
    static final int  QUEUE_CAPACITY  = 30;
    static final long CALL_TIMEOUT_MS = 300;

    static final Random rng = new Random();

    public static void main(String[] args) throws InterruptedException {

        System.out.println("=== Tandem M/M/1 Simulation (Figure 2) ===");
        System.out.printf("Chain length: %d modules | lambda: %.1f req/s | mu: %.1f req/s | rho: %.3f%n",
            NUM_MODULES, LAMBDA, MU, LAMBDA / MU);
        System.out.printf("Service distribution: %s%n%n", SERVICE_DIST);

        // ── 1. Build the tandem chain ─────────────────────────────────────
        // Modules are created once and reused across all arriving requests.
        // Each has its own CB, queue, and server -- but they are wired in
        // series: a request must pass through module 0 before module 1, etc.
        List<Mm1Module> chain = new ArrayList<>();
        for (int i = 0; i < NUM_MODULES; i++) {
            chain.add(new Mm1Module(i, LAMBDA, MU));
        }

        // ── 2. Single arrival loop -- passes each request through the chain ─
        // The arrival loop is single-threaded: one request arrives, travels
        // through all modules in order, then the next request arrives.
        // This correctly models the tandem semantics: module i+1 cannot
        // receive a request before module i finishes with it.
        long start = System.currentTimeMillis();
        int requestId = 0;

        while (System.currentTimeMillis() - start < SIM_DURATION_MS) {

            // Poisson inter-arrival into module 0 (external source)
            long interArrivalMs = (long) (exponential(LAMBDA) * 1000);
            Thread.sleep(Math.max(1, interArrivalMs));

            final int reqId = requestId++;

            // Pass the request through each module in order.
            // If any module rejects it, break -- it does not reach downstream.
            boolean rejected = false;
            for (Mm1Module module : chain) {
                boolean passed = module.process(reqId);
                if (!passed) {
                    // Record which module dropped it for cascade analysis
                    module.droppedHere.incrementAndGet();
                    rejected = true;
                    break;
                }
                // If the module served it, its departure feeds the next module.
                // We don't need to do anything special here -- the next
                // iteration of the for loop IS the "hand-off" to module i+1.
            }

            // If it made it through all modules, it's a full system success.
            if (!rejected) {
                chain.get(chain.size() - 1).fullSystemSuccess.incrementAndGet();
            }
        }

        // Shut down all module executors cleanly
        for (Mm1Module module : chain) {
            module.executor.shutdownNow();
        }

        // ── 3. Per-module reports ──────────────────────────────────────────
        for (Mm1Module module : chain) {
            module.printModuleReport();
        }

        // ── 4. System-level summary ────────────────────────────────────────
        System.out.println("\n=== System-Level Tandem Summary ===");
        System.out.printf("Total requests into module 0:  %d%n", requestId);

        for (Mm1Module module : chain) {
            int totalIn  = module.served.get() + module.blocked.get()
                         + module.totalRejectedQueueFull.get() + module.totalTimeout.get();
            int totalOut = module.served.get();
            double throughput = totalIn == 0 ? 0 : 100.0 * totalOut / totalIn;
            System.out.printf("  Module %d: in=%-4d out=%-4d dropped-here=%-4d throughput=%.1f%%%n",
                module.id, totalIn, totalOut, module.droppedHere.get(), throughput);
        }

        int finalOutput = chain.get(chain.size() - 1).served.get();
        System.out.printf("%nFull-system throughput: %d / %d requests (%.1f%%)%n",
            finalOutput, requestId,
            requestId == 0 ? 0.0 : 100.0 * finalOutput / requestId);

        System.out.println("\nKey tandem observation:");
        System.out.println("If an upstream module's CB trips, downstream modules receive FEWER");
        System.out.println("arrivals -- their effective lambda drops, so their CB may stay CLOSED");
        System.out.println("even though the system as a whole is shedding load.");
        System.out.println("If a downstream module is slow, upstream modules see no direct effect");
        System.out.println("(there is no backpressure in this model -- rejected jobs exit silently).");
    }

    // ── Shared exponential sampler (for the main arrival loop) ────────────
    static double exponential(double rate) {
        return -Math.log(1 - rng.nextDouble()) / rate;
    }

    /**
     * One module in the tandem chain.
     *
     * process() returns true if the request successfully departed this
     * module (and should proceed to the next), false if it was rejected
     * at any point (CB blocked, queue full, or timeout).
     */
    static class Mm1Module {
        final int id;
        final double lambda;   // the ORIGINAL external lambda -- used for Burke's expected rate
        final double mu;
        final CircuitBreaker cb;
        final Random rng = new Random();

        final AtomicInteger served                = new AtomicInteger();
        final AtomicInteger blocked               = new AtomicInteger();
        final AtomicInteger totalRejectedQueueFull = new AtomicInteger();
        final AtomicInteger totalTimeout           = new AtomicInteger();
        // droppedHere: requests that made it THIS FAR but were rejected here
        // (did not make it further down the chain)
        final AtomicInteger droppedHere           = new AtomicInteger();
        // fullSystemSuccess: only set on the LAST module -- requests that
        // passed every module in the chain
        final AtomicInteger fullSystemSuccess     = new AtomicInteger();

        final ExecutorService executor  = Executors.newSingleThreadExecutor();
        final Semaphore       queueSlots = new Semaphore(QUEUE_CAPACITY);

        // Departure timestamps for Burke's theorem check.
        // In the tandem context, these are the times this module released a
        // job to the NEXT module -- i.e. the inter-arrival times seen by
        // module i+1 should match the inter-departure times of module i.
        final List<Long> departureTimestamps = new ArrayList<>();
        long moduleStart; // set when first request arrives at this module

        Mm1Module(int id, double lambda, double mu) {
            this.id     = id;
            this.lambda = lambda;
            this.mu     = mu;

            // Same CB config as the single-module fix.
            // In the tandem case, each module's CB is still independent --
            // but their EFFECTS are coupled: module 0 tripping OPEN reduces
            // the actual arrival rate seen by module 1, even though module 1's
            // CB knows nothing about module 0's state.
            CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .slowCallRateThreshold(50)
                .slowCallDurationThreshold(Duration.ofMillis(20))
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(20)
                .minimumNumberOfCalls(10)
                .waitDurationInOpenState(Duration.ofSeconds(3))
                .permittedNumberOfCallsInHalfOpenState(3)
                .recordExceptions(RuntimeException.class, TimeoutException.class, ExecutionException.class)
                .ignoreExceptions(NoSuchElementException.class)
                .build();

            CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(config);
            this.cb = registry.circuitBreaker("tandem-module-" + id);

            this.cb.getEventPublisher()
                .onStateTransition(e -> System.out.printf(
                    "[Module %d][CB] %s -> %s%n", id,
                    e.getStateTransition().getFromState(),
                    e.getStateTransition().getToState()))
                .onCallNotPermitted(e ->
                    System.out.printf("[Module %d][CB] REJECTED -- circuit OPEN%n", id));
        }

        /**
         * Process one request through this module's CB + queue + server.
         *
         * Returns TRUE  if the request successfully departed (proceed to next module).
         * Returns FALSE if the request was rejected at any point (stop here).
         *
         * This is the tandem hand-off mechanism: the return value tells the
         * main loop whether to continue to module i+1 or drop the request.
         */
        boolean process(int requestId) {
            if (moduleStart == 0) moduleStart = System.currentTimeMillis();
            long callStart = System.currentTimeMillis() - moduleStart;

            Supplier<String> backendCall = CircuitBreaker.decorateSupplier(cb, () -> {
                try {
                    return processRequest(requestId);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            try {
                backendCall.get();
                served.incrementAndGet();
                // Record departure time -- this is what module i+1 sees as
                // an "arrival" in the tandem chain. Burke's theorem predicts
                // these should form a Poisson(lambda) process.
                departureTimestamps.add(System.currentTimeMillis() - moduleStart);
                return true;   // request departs -- hand off to next module
            } catch (io.github.resilience4j.circuitbreaker.CallNotPermittedException e) {
                blocked.incrementAndGet();
                return false;  // CB blocked -- request exits the system here
            } catch (Exception e) {
                // Queue full or timeout -- also exits here
                departureTimestamps.add(System.currentTimeMillis() - moduleStart);
                return false;
            }
        }

        /**
         * Real single-server queue: bounded semaphore + single executor thread.
         * Identical to the single-module and parallel-module implementations.
         */
        String processRequest(int requestId) throws Exception {
            if (!queueSlots.tryAcquire()) {
                totalRejectedQueueFull.incrementAndGet();
                throw new RuntimeException("Module " + id + " #" + requestId + " REJECTED -- queue full");
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
                    "Module " + id + " #" + requestId + " TIMEOUT after " + CALL_TIMEOUT_MS + "ms", e);
            }
        }

        double exponential(double rate) {
            return -Math.log(1 - rng.nextDouble()) / rate;
        }

        double pareto(double c, double alpha) {
            double u = rng.nextDouble();
            return c / Math.pow(u, 1.0 / alpha);
        }

        long sampleServiceTimeMs() {
            double seconds = switch (SERVICE_DIST) {
                case EXPONENTIAL -> exponential(mu);
                case PARETO      -> pareto(PARETO_C, PARETO_ALPHA);
            };
            return Math.max(1, (long) (seconds * 1000));
        }

        void printModuleReport() {
            int totalIn = served.get() + blocked.get()
                        + totalRejectedQueueFull.get() + totalTimeout.get();

            System.out.printf("%n--- Module %d Report ---%n", id);
            System.out.printf("Requests received:  %d%n", totalIn);
            System.out.printf("Served (passed on): %d%n", served.get());
            System.out.printf("CB-blocked:         %d%n", blocked.get());
            System.out.printf("Queue-full:         %d%n", totalRejectedQueueFull.get());
            System.out.printf("Timed out:          %d%n", totalTimeout.get());
            System.out.printf("Final CB state:     %s%n", cb.getState());
            System.out.printf("Slow call rate:     %.1f%%%n", cb.getMetrics().getSlowCallRate());

            if (id > 0) {
                System.out.printf("Note: this module received FEWER than lambda=%.0f req/s%n", lambda);
                System.out.printf("because upstream modules shed some load before it arrived here.%n");
            }

            verifyBurkesTheorem();
        }

        /**
         * Burke's theorem check for this module's departure process.
         *
         * In the tandem context this has extra meaning: the CV of this
         * module's inter-departure times is exactly what module i+1 sees
         * as the CV of its inter-arrival times. If CV ~= 1.0 here, module
         * i+1 is justified in treating its arrivals as Poisson (M/M/1).
         * If CV >> 1.0 here, module i+1 is actually an M/G/1 or G/G/1
         * system, even if its service times are exponential.
         */
        void verifyBurkesTheorem() {
            if (departureTimestamps.size() < 30) {
                System.out.printf("Burke check: not enough departures (%d) to evaluate.%n",
                    departureTimestamps.size());
                return;
            }

            int n = departureTimestamps.size() - 1;
            double[] gaps = new double[n];
            for (int i = 0; i < n; i++) {
                gaps[i] = (departureTimestamps.get(i + 1) - departureTimestamps.get(i)) / 1000.0;
            }

            double mean = 0;
            for (double g : gaps) mean += g;
            mean /= n;

            double sqDiff = 0;
            for (double g : gaps) sqDiff += (g - mean) * (g - mean);
            double cv = Math.sqrt(sqDiff / (n - 1)) / mean;

            System.out.printf(
                "Burke check: mean inter-departure=%.4fs (expect ~%.4fs = 1/lambda), CV=%.3f (expect ~1.0)%s%n",
                mean, 1.0 / lambda, cv,
                Math.abs(cv - 1.0) < 0.15
                    ? "  [consistent -- next module sees ~Poisson arrivals]"
                    : "  [deviates -- next module sees non-Poisson arrivals]");
        }
    }
}