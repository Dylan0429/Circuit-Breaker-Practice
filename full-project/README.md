# Queue Simulations + Resilience4j Circuit Breaker

M/M/1, M/G/1, and G/G/1 queueing theory simulations, each protected by a Resilience4j circuit breaker.

## Prerequisites
- Java 17+
- Maven 3.8+

## Build
```bash
mvn package
```

## Run each simulation

```bash
# M/M/1 — Poisson arrivals, Exponential service (default)
java -cp target/queue-simulations-1.0-SNAPSHOT.jar com.example.mm1.MM1CircuitBreakerExample

# M/G/1 — Poisson arrivals, General service (change DIST in the file)
java -cp target/queue-simulations-1.0-SNAPSHOT.jar com.example.mg1.MG1CircuitBreakerExample

# G/G/1 — General arrivals, General service
java -cp target/queue-simulations-1.0-SNAPSHOT.jar com.example.gg1.GG1CircuitBreakerExample
```

## Clear Maven everytime if alterations to code are made
```bash
mvn clean package
```

## Queue model comparison

| Model | Arrivals | Service | Theory used | Key formula |
|-------|----------|---------|-------------|-------------|
| M/M/1 | Poisson | Exponential | Exact | Lq = ρ²/(1−ρ) |
| M/G/1 | Poisson | Any | Exact (P-K) | Lq = λ²E[S²] / 2(1−ρ) |
| G/G/1 | Any | Any | Approximation (Kingman) | Wq ≈ Wq_MM1 × (ca²+cs²)/2 |

## Key insight: variability makes queues worse

For the same utilization ρ, queue length Lq increases with variance:

```
Deterministic service < Exponential service < Lognormal service
     (M/D/1)               (M/M/1)              (M/G/1 / G/G/1)
```

The circuit breaker responds to this by tripping OPEN when slow calls (caused
by high-variance service) accumulate beyond the configured threshold.

## M/M/1 steady-state formulas (ρ = λ/μ < 1)

| Symbol | Formula | Meaning |
|--------|---------|---------|
| ρ | λ/μ | Utilization |
| Lq | ρ²/(1−ρ) | Mean queue length |
| Wq | λ/(μ(μ−λ)) | Mean wait in queue |
| W | 1/(μ−λ) | Mean time in system |

## M/G/1 Pollaczek-Khinchine formula

```
Lq = (λ² × E[S²]) / (2 × (1 − ρ))
```

## G/G/1 Kingman approximation

```
Wq ≈ (ρ / (μ(1−ρ))) × ((ca² + cs²) / 2)
```
where ca² and cs² are the squared coefficients of variation for arrivals and service.
