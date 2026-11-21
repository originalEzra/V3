# PricePulse V3

A high-concurrency multi-user real-time market monitoring and one-shot price alert platform built with Java 21, Spring Boot 3.5, MyBatis, Flyway, MySQL 8.4, Redis 7, RocketMQ 5.3, Resilience4j, and Vue 3 + TypeScript.

## Architecture Highlights

- **Sharded Lock-Free Tick Pipeline (`market`)**: Routes incoming `QuoteTick` events across `pricepulse.lanes=16` single-threaded `ThreadPoolExecutor` lanes (`quote-lane-0`..`15`, `ArrayBlockingQueue(1024)`) using bit-mixed symbol hashing, smooth blocking backpressure, and per-stock `(lastEventTs, lastSeq)` monotonic regression protection.
- **High-Throughput Reads & 1m K-Line Aggregation**: Writes live prices in-memory and to Redis Hash (`quote:latest:{symbol}`), aggregates 1-minute OHLC bars in memory for batch `INSERT ... ON DUPLICATE KEY UPDATE` into `kline_1m`, and protects `/api/quotes/*` and `/api/klines/*` via Cache-Aside with `__NULL__` penetration sentinels, `SET NX EX 5` mutex single-flight breakdown protection, and jittered TTLs.
- **Redis ZSet Pre-Filter + Single-Thread Maintainer + Circuit Breaker (`rule`)**: Splits rules into integer-cent `rules:above:{stockId}` and `rules:below:{stockId}` ZSets, guards Lua matching with sentinel key `pricepulse:v3:index:ready`, keeps Redis synchronized via `@TransactionalEventListener(AFTER_COMMIT)` -> single-threaded `rule-index-maintainer` (DB re-query + 30s full reconciliation), and fails over via Resilience4j to a covering-index `UNION ALL` MySQL fallback (`idx_match`) with a strict Recovery Gate before switching back.
- **Optimistic Lock Trigger + Transactional Outbox + RocketMQ (`alert`)**: Completes one-shot rules atomically with `WHERE status='ACTIVE' AND version=#{version}`, inserts `alert_triggers` + `outbox_events` in the same local transaction, claims batches with `FOR UPDATE SKIP LOCKED` + per-lease `locked_by` fencing, retries indefinitely with `1s..30s` capped exponential backoff during broker outages, consumes via bounded RocketMQ concurrency (`4..8` threads) with `trigger_id` idempotency, and pushes alerts over WebSocket.

## Quick Start

### 1. Start Infrastructure (MySQL 8.4, Redis 7, RocketMQ 5.3)
```bash
docker compose -f compose.yaml up -d
docker compose -f compose.yaml ps
```

### 2. Run Backend & Frontend
```bash
# Backend (http://127.0.0.1:8080)
JAVA_HOME=/opt/homebrew/opt/openjdk PATH="/opt/homebrew/opt/openjdk/bin:$PATH" mvn spring-boot:run

# Frontend (http://127.0.0.1:5173)
cd frontend && npm install && npm run dev
```

### 3. Run Tests, Benchmarks & Fault Drills
```bash
# Standard unit & integration suite (20 tests)
JAVA_HOME=/opt/homebrew/opt/openjdk PATH="/opt/homebrew/opt/openjdk/bin:$PATH" mvn test

# Phase 6 A/B benchmarks & EXPLAIN ANALYZE on 5,000 rules
JAVA_HOME=/opt/homebrew/opt/openjdk PATH="/opt/homebrew/opt/openjdk/bin:$PATH" mvn -Dtest=Phase6Benchmark test

# Phase 6 HA fault drills (requires live backend on :8080 and Docker Compose)
python3 scripts/phase6_fault_drills.py
```

---

## Phase 6 A/B Benchmark Results

Measured with [Phase6Benchmark.java](src/test/java/com/pricepulse/benchmark/Phase6Benchmark.java) against live MySQL 8.4.7, Redis 7.4.6, and RocketMQ 5.3.2:

1. **High-Concurrency Rule Matching A/B**: 100 stocks × 50 rules (**5,000 rules** total), deterministic replay of **20,000 ticks** (200 waves × 100 stocks, 128 in-flight dispatch concurrency, **500** one-shot triggers per run).
2. **Multi-User Concurrent Read A/B**: **100 concurrent threads** × 200 requests (**20,000 HTTP requests** total: 45% hot quotes, 45% hot 60-bar K-lines, 10% random unknown symbols `INVALID_00..49`, cold read-cache start).

| Scenario | Baseline (`Off / DB Fallback`) | Optimized (`On / Redis`) | Delta / Gain |
| --- | --- | --- | --- |
| **Rule Matching** (`force-db-fallback`: `true` vs `false`, 5,000 rules / 20,000 ticks) | **5,973.28 TPS** · P50 `2.521 ms` · P95 `154.247 ms` · P99 `183.164 ms` (`3.348 s`, `500` triggers) | **7,701.58 TPS** · P50 `1.954 ms` · P95 `123.062 ms` · P99 `150.125 ms` (`2.597 s`, `500` triggers) | **1.29x TPS** · P95 **-20.2%** · `500 == 500` parity (offloads 20k matching `SELECT`s from MySQL) |
| **Concurrent Reads** (`read-cache.enabled`: `false` vs `true`, 100 threads / 20,000 reqs) | **4,749.88 QPS** · P50 `17.534 ms` · P95 `46.950 ms` · P99 `68.742 ms` (`4.211 s`) · `29,009` app DB queries (`29,043` MySQL `Com_select`) | **9,181.67 QPS** · P50 `10.179 ms` · P95 `16.632 ms` · P99 `22.389 ms` (`2.178 s`) · `113` app DB queries (`123` MySQL `Com_select`) | **1.93x QPS** · P95 **-64.6%** · **-99.61%** app DB queries (`-99.58%` `Com_select`) |

### `EXPLAIN ANALYZE` on 5,000 Rules (`UNION ALL` Fallback Query)

```text
-> Append  (cost=0.92 rows=2) (actual time=0.0135..0.0135 rows=0 loops=1)
    -> Stream results  (cost=0.46 rows=1) (actual time=0.00729..0.00729 rows=0 loops=1)
        -> Filter: ((alert_rules.direction = 'ABOVE') and (alert_rules.`status` = 'ACTIVE') and (alert_rules.stock_id = 1) and (alert_rules.threshold_price <= 100.00))  (cost=0.46 rows=1) (actual time=0.00658..0.00658 rows=0 loops=1)
            -> Covering index range scan on alert_rules using idx_match over (stock_id = 1 AND status = 'ACTIVE' AND direction = 'ABOVE' AND threshold_price <= 100.00)  (cost=0.46 rows=1) (actual time=0.00621..0.00621 rows=0 loops=1)
    -> Stream results  (cost=0.46 rows=1) (actual time=0.00192..0.00192 rows=0 loops=1)
        -> Filter: ((alert_rules.direction = 'BELOW') and (alert_rules.`status` = 'ACTIVE') and (alert_rules.stock_id = 1) and (alert_rules.threshold_price >= 100.00))  (cost=0.46 rows=1) (actual time=0.00188..0.00188 rows=0 loops=1)
            -> Covering index range scan on alert_rules using idx_match over (stock_id = 1 AND status = 'ACTIVE' AND direction = 'BELOW' AND 100.00 <= threshold_price)  (cost=0.46 rows=1) (actual time=0.00179..0.00179 rows=0 loops=1)
```

Both branches execute a **Covering index range scan on `alert_rules` using `idx_match`** with zero full table scan.

---

## High-Availability Fault Drills

Executed end-to-end against a packaged `price-pulse-v3-0.0.1-SNAPSHOT.jar` instance via [phase6_fault_drills.py](scripts/phase6_fault_drills.py):

| Fault Drill | Injected Fault & Scope | Observed Behaviour & Verified Metrics |
| --- | --- | --- |
| **1. Redis Crash & Empty Rebuild** | `docker compose stop redis` -> trigger Rule 1 (`AAPL`) & create Rule 2 (`MSFT`) -> `docker compose start redis` (non-persistent empty restart) | Circuit breaker opened in **0.05 s** (`DB_FALLBACK`, `redisIndexReady=false`); Rule 1 triggered & delivered (`1/1/1`). After empty Redis restart, Recovery Gate rebuilt ZSets from MySQL, set sentinel `pricepulse:v3:index:ready=1`, and switched back to `REDIS` in **0.63 s**; Rule 2 matched on Redis (`redisMatchCount` `100 -> 200`, `fallbackMatchCount` unchanged, final totals `2/2/2`). |
| **2. RocketMQ Broker Outage & Catch-Up** | `docker compose stop rocketmq-broker` -> crash storm triggering **20** rules -> hold outage past 8 attempts (`1s..30s` capped backoff) -> `docker compose start rocketmq-broker` | Held all **20** events in `outbox_events(PENDING)` with `0` notifications across **125.1 s** outage up to **`max_attempts=9`**. After broker restart, Outbox caught up all 20 events and consumer persisted **20/20** distinct notifications in **30.03 s** (**0** lost, **0** duplicates). |
| **3. 500-Rule Crash Storm & Pool Health** | 100 stocks × 5 `BELOW` rules (**500** rules) -> `POST /api/simulator/crash-storm?dropRatio=0.05` | Storm HTTP completed in **492.8 ms**; full E2E pipeline drained in **2.35 s**. Exact 3-table parity: `alert_rules(COMPLETED)=500` (`ACTIVE=0`), `alert_triggers=500`, `outbox_events(SENT)=500` (`PENDING=0`), `notifications=500` (`0` missing). HikariCP (`max=20`): **0** connection timeouts and **0** pool error log lines. |

---

## Test Environment

- **Host**: Apple M4 (10 cores, 16 GB RAM), macOS 27.2.1 (`26B131`)
- **Runtime**: OpenJDK 25.0.2 (compiled targeting Java 21), Spring Boot 3.5.16, Resilience4j 2.3.0
- **Infrastructure (`compose.yaml`)**: MySQL `8.4.7`, Redis `7.4.6-alpine` (`--save "" --appendonly no`), Apache RocketMQ `5.3.2` (`namesrv` + `broker`)
- **Concurrency & Pool Settings**: `pricepulse.lanes=16`, `lane-queue-capacity=1024`, `hikari.maximum-pool-size=20`, `outbox.poll-interval-ms=200` (`batch-size=50`, `initial-backoff=1s`, `max-backoff=30s`), RocketMQ consumer threads `[4, 8]`
