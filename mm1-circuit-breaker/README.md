# M/M/1 Queue + Resilience4j Circuit Breaker

A simulation of an M/M/1 queueing system protected by a Resilience4j circuit breaker.

## What this demonstrates

- **M/M/1 queue**: single server, Poisson arrivals (rate λ), exponential service times (rate μ)
- **Circuit breaker**: wraps backend calls and trips OPEN when slow/failed calls exceed thresholds
- **The connection**: as ρ = λ/μ → 1, queue length and wait times explode — the circuit breaker detects this via slow call rate and sheds load before the system falls over

## Prerequisites

- Java 17+
- Maven 3.8+

## Run locally

```bash
mvn package
java -jar target/mm1-circuit-breaker-1.0-SNAPSHOT.jar
```

## Key M/M/1 formulas

| Symbol | Formula | Meaning |
|--------|---------|---------|
| ρ | λ / μ | Utilization (must be < 1 for stability) |
| Lq | ρ² / (1 − ρ) | Mean queue length |
| L | ρ / (1 − ρ) | Mean jobs in system |
| Wq | λ / (μ(μ − λ)) | Mean wait time in queue |
| W | 1 / (μ − λ) | Mean time in system |

Default config: λ = 40 req/s, μ = 60 req/s → ρ ≈ 0.667, Wq ≈ 33ms

## Circuit breaker config

| Setting | Value | Why |
|---------|-------|-----|
| `failureRateThreshold` | 50% | Trip if half of recent calls fail |
| `slowCallRateThreshold` | 80% | Trip if most calls are slow (ρ → 1 indicator) |
| `slowCallDurationThreshold` | 200ms | Calls longer than this count as "slow" |
| `slidingWindowSize` | 20 | Rolling window of last 20 calls |
| `minimumNumberOfCalls` | 10 | Need 10 samples before evaluating |
| `waitDurationInOpenState` | 3s | Recovery time before probing again |
| `permittedNumberOfCallsInHalfOpenState` | 3 | Test calls before re-closing |
