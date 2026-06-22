import java.util.Random;
import java.util.function.Supplier;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

public class GG1CircuitBreakerExample {
    
    static final double LAMBDA = 40.0;
    static final double MU = 60.0;
    static final double MEAN_IA = 1.0 / LAMBDA;
    static final double MEAN_S = 1.0 / MU;
    
    static final double CV_A = 0.5;
    static final double STD_IA = CV_A * MEAN_IA;
    
    static final double CV_S = 2.0;
    static final double SIGMA_LN = Math.sqrt(Math.log(1 + CV_S * CV_S));
    static final double MU_LN = Math.log(MEAN_S) - (SIGMA_LN * SIGMA_LN) / 2.0;
    
    static final Random rng = new Random();
    static final AtomicInteger served = new AtomicInteger(0);
    static final AtomicInteger blocked = new AtomicInteger(0);
    static final AtomicInteger failed = new AtomicInteger(0);

    public static void main(String[] args) throws InterruptedException {
        
        System.out.println("=== G/G/1 QUEUE SIMULATION with CIRCUIT BREAKER ===\n");
        
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(10)
                .failureRateThreshold(50)
                .slowCallRateThreshold(80)
                .slowCallDurationThreshold(Duration.ofMillis(200))
                .waitDurationInOpenState(Duration.ofSeconds(3))
                .permittedNumberOfCallsInHalfOpenState(3)
                .build();

        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(config);
        CircuitBreaker cb = registry.circuitBreaker("gg1");

        System.out.println("📢 Listening for circuit breaker events:\n");
        cb.getEventPublisher()
            .onStateTransition(e -> System.out.println(
                "   ⚡ Circuit: " + 
                e.getStateTransition().getFromState() + " → " + 
                e.getStateTransition().getToState()))
            .onCallNotPermitted(e -> 
                System.out.println("   🚫 Request BLOCKED"))
            .onError(e -> System.out.println(
                "   ❌ Error: " + e.getElapsedDuration().toMillis() + "ms"))
            .onFailureRateExceeded(e -> System.out.println(
                "   🔥 Failure rate exceeded: " + e.getFailureRate() + "%"))
            .onSlowCallRateExceeded(e -> System.out.println(
                "   🐢 Slow call rate exceeded: " + e.getSlowCallRate() + "%"));

        System.out.println("\n--- Kingman Approximation ---");
        printKingman();

        System.out.println("\n--- Running for 15 seconds ---\n");

        long startTime = System.currentTimeMillis();
        long duration = 15000;
        int requestCount = 0;

        while (System.currentTimeMillis() - startTime < duration) {
            
            long interArrivalMs = (long)(normalInterArrival() * 1000);
            Thread.sleep(Math.max(1, interArrivalMs));
            
            requestCount++;
            final int requestId = requestCount;

            Supplier<String> call = CircuitBreaker.decorateSupplier(cb, () -> {
                long serviceMs = lognormalServiceMs();
                
                try {
                    Thread.sleep(serviceMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                
                if (serviceMs > 200) {
                    failed.incrementAndGet();
                    throw new RuntimeException("Timeout: " + serviceMs + "ms");
                }
                
                served.incrementAndGet();
                return "✅ Done in " + serviceMs + "ms";
            });

            try {
                String result = call.get();
                System.out.printf("   Request %3d: %s%n", requestId, result);
            } catch (io.github.resilience4j.circuitbreaker.CallNotPermittedException e) {
                blocked.incrementAndGet();
                System.out.printf("   Request %3d: 🚫 BLOCKED%n", requestId);
            } catch (Exception e) {
                System.out.printf("   Request %3d: ❌ FAILED%n", requestId);
            }
        }

        System.out.println("\n=== FINAL STATS ===\n");
        System.out.printf("   Total requests:      %d%n", requestCount);
        System.out.printf("   ✅ Served:            %d%n", served.get());
        System.out.printf("   🚫 Blocked:           %d%n", blocked.get());
        System.out.printf("   ❌ Failed:            %d%n", failed.get());

        System.out.println("\n--- Circuit Breaker Metrics ---");
        CircuitBreaker.Metrics metrics = cb.getMetrics();
        System.out.printf("   Failure rate:        %.1f%%%n", metrics.getFailureRate());
        System.out.printf("   Slow call rate:      %.1f%%%n", metrics.getSlowCallRate());
        System.out.printf("   State:               %s%n", cb.getState());
        System.out.printf("   Calls in window:     %d%n", metrics.getNumberOfBufferedCalls());
        
        System.out.println("\n✅ Simulation complete!");
    }

    static double normalInterArrival() {
        double u1 = rng.nextDouble();
        double u2 = rng.nextDouble();
        double z = Math.sqrt(-2.0 * Math.log(u1)) * Math.cos(2.0 * Math.PI * u2);
        double sample = MEAN_IA + STD_IA * z;
        return Math.max(0.001, sample);
    }

    static long lognormalServiceMs() {
        double u1 = rng.nextDouble();
        double u2 = rng.nextDouble();
        double z = Math.sqrt(-2.0 * Math.log(u1)) * Math.cos(2.0 * Math.PI * u2);
        double seconds = Math.exp(MU_LN + SIGMA_LN * z);
        return Math.max(1, (long)(seconds * 1000));
    }

    static void printKingman() {
        double rho = LAMBDA / MU;
        double ca2 = CV_A * CV_A;
        double cs2 = CV_S * CV_S;
        double wqMm1 = rho / (MU * (1 - rho));
        double kingmanWq = wqMm1 * ((ca2 + cs2) / 2.0);
        double lq = LAMBDA * kingmanWq;
        double w = kingmanWq + MEAN_S;
        double l = LAMBDA * w;

        System.out.printf("   ρ (utilization):        %.3f%n", rho);
        System.out.printf("   ca² (arrival var):      %.2f%n", ca2);
        System.out.printf("   cs² (service var):      %.2f%n", cs2);
        System.out.printf("   Variability multiplier: %.2fx%n", (ca2 + cs2) / 2.0);
        System.out.printf("   Lq (queue length):      %.3f jobs%n", lq);
        System.out.printf("   L  (system jobs):       %.3f jobs%n", l);
        System.out.printf("   Wq (wait time):         %.1f ms%n", kingmanWq * 1000);
        System.out.printf("   W  (system time):       %.1f ms%n", w * 1000);
    }
}