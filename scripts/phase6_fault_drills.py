#!/usr/bin/env python3
"""
Phase 6 Automated High-Availability Fault Drills for PricePulse V3.

Drills:
  1. Redis Crash -> MySQL Fallback -> Empty-Redis Rebuild & Smooth Recovery
  2. RocketMQ Broker Crash -> Outbox Backlog Past Retry Window -> Auto Catch-Up
  3. 500-Rule One-Click Crash Storm -> 3-Table Parity & HikariCP Health
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


class DrillFailure(RuntimeError):
    pass


class FaultDrillRunner:
    def __init__(
        self,
        base_url: str,
        compose_file: str,
        db_user: str,
        db_password: str,
        db_name: str,
        backend_log: str | None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.compose_file = compose_file
        self.db_user = db_user
        self.db_password = db_password
        self.db_name = db_name
        self.backend_log = Path(backend_log) if backend_log else None

    # ------------------------------------------------------------------
    # HTTP / Docker / DB / Redis helpers
    # ------------------------------------------------------------------

    def http_json(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        timeout: float = 30.0,
    ) -> tuple[int, Any]:
        url = f"{self.base_url}{path}"
        data = None
        headers: dict[str, str] = {}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8")
                parsed = json.loads(raw) if raw else None
                return resp.status, parsed
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            raise DrillFailure(f"{method} {path} returned HTTP {exc.code}: {raw}") from exc
        except urllib.error.URLError as exc:
            raise DrillFailure(f"{method} {path} failed: {exc}") from exc

    def compose(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        cmd = ["docker", "compose", "-f", self.compose_file, *args]
        proc = subprocess.run(cmd, text=True, capture_output=True)
        if check and proc.returncode != 0:
            raise DrillFailure(
                f"Command {' '.join(cmd)} failed (rc={proc.returncode}): {proc.stderr.strip()}"
            )
        return proc

    def mysql_scalar(self, sql: str) -> str:
        proc = self.compose(
            "exec",
            "-T",
            "-e",
            f"MYSQL_PWD={self.db_password}",
            "mysql",
            "mysql",
            "-h",
            "127.0.0.1",
            f"-u{self.db_user}",
            "-N",
            "-B",
            self.db_name,
            "-e",
            sql,
        )
        return proc.stdout.strip()

    def mysql_int(self, sql: str) -> int:
        val = self.mysql_scalar(sql)
        return int(val) if val and val.upper() != "NULL" else 0

    def mysql_exec(self, sql: str) -> None:
        self.compose(
            "exec",
            "-T",
            "-e",
            f"MYSQL_PWD={self.db_password}",
            "mysql",
            "mysql",
            "-h",
            "127.0.0.1",
            f"-u{self.db_user}",
            self.db_name,
            "-e",
            sql,
        )

    def redis_cli(self, *args: str) -> str:
        proc = self.compose("exec", "-T", "redis", "redis-cli", *args)
        return proc.stdout.strip()

    def delete_redis_patterns(self, *patterns: str) -> None:
        for pat in patterns:
            keys_raw = self.redis_cli("--raw", "KEYS", pat)
            keys = [k for k in keys_raw.splitlines() if k]
            if keys:
                self.redis_cli("DEL", *keys)

    def get_status(self) -> dict[str, Any]:
        _, status = self.http_json("GET", "/api/system/status")
        return status

    def get_actuator_metric(self, name: str) -> float:
        _, payload = self.http_json("GET", f"/actuator/metrics/{name}")
        measurements = payload.get("measurements", [])
        if not measurements:
            raise DrillFailure(f"No measurements returned for metric {name}: {payload}")
        return float(measurements[0]["value"])

    def wait_until(self, desc: str, timeout_s: float, poll_s: float, predicate) -> Any:
        deadline = time.monotonic() + timeout_s
        last_err: str | None = None
        while time.monotonic() < deadline:
            try:
                res = predicate()
                if res:
                    return res
                last_err = None
            except Exception as exc:  # noqa: BLE001
                last_err = str(exc)
            time.sleep(poll_s)
        detail = f" (last error: {last_err})" if last_err else ""
        raise DrillFailure(f"Timed out after {timeout_s:.1f}s waiting for: {desc}{detail}")

    def ensure_services_running(self) -> None:
        self.compose("start", "redis", check=False)
        self.compose("start", "rocketmq-broker", check=False)

    def wait_for_redis_recovery_state(self, timeout_s: float = 30.0) -> dict[str, Any]:
        return self.wait_until(
            "matchingMode=REDIS, redisIndexReady=true, circuitBreakerState=CLOSED",
            timeout_s=timeout_s,
            poll_s=0.5,
            predicate=lambda: (
                s
                if (
                    (s := self.get_status()).get("matchingMode") == "REDIS"
                    and s.get("redisIndexReady") is True
                    and s.get("circuitBreakerState") == "CLOSED"
                )
                else None
            ),
        )

    def wait_maintainer_drained(self, extra_sleep_s: float = 0.6, timeout_s: float = 30.0) -> None:
        self.wait_until(
            "maintainerQueueSize == 0",
            timeout_s=timeout_s,
            poll_s=0.2,
            predicate=lambda: self.get_status().get("maintainerQueueSize") == 0,
        )
        if extra_sleep_s > 0:
            time.sleep(extra_sleep_s)

    def clean_state(self) -> None:
        self.http_json("POST", "/api/simulator/stop")
        self.mysql_exec(
            "DELETE FROM notifications; "
            "DELETE FROM outbox_events; "
            "DELETE FROM alert_triggers; "
            "DELETE FROM alert_rules;"
        )
        self.delete_redis_patterns("rules:*")
        self.wait_maintainer_drained(extra_sleep_s=0.3)

    # ------------------------------------------------------------------
    # Drill 1: Redis Crash -> MySQL Fallback -> Empty-Redis Rebuild
    # ------------------------------------------------------------------

    def run_drill_1(self) -> dict[str, Any]:
        print("\n=== Drill 1: Redis Crash -> MySQL Fallback -> Empty-Redis Rebuild ===")
        self.clean_state()
        self.wait_for_redis_recovery_state(timeout_s=30.0)

        self.http_json("POST", "/api/simulator/emit")
        _, aapl_quote = self.http_json("GET", "/api/quotes/AAPL")
        aapl_price = float(aapl_quote["price"])
        aapl_id = int(aapl_quote["stockId"])
        rule1_threshold = round(aapl_price * 0.98, 2)

        _, rule1 = self.http_json(
            "POST",
            "/api/rules",
            {
                "userId": 1,
                "symbol": "AAPL",
                "direction": "BELOW",
                "thresholdPrice": rule1_threshold,
            },
        )
        rule1_id = int(rule1["id"])
        self.wait_maintainer_drained(extra_sleep_s=0.6)

        status_before = self.get_status()
        assert status_before["matchingMode"] == "REDIS", status_before
        assert status_before["redisIndexReady"] is True, status_before
        assert status_before["circuitBreakerState"] == "CLOSED", status_before

        ready_val = self.redis_cli("GET", "pricepulse:v3:index:ready")
        if ready_val != "1":
            raise DrillFailure(f"Expected sentinel '1', got {ready_val!r}")
        rule1_score = self.redis_cli("ZSCORE", f"rules:below:{aapl_id}", str(rule1_id))
        if not rule1_score:
            raise DrillFailure(f"Rule 1 ({rule1_id}) missing in Redis ZSet rules:below:{aapl_id}")
        print(f"Pre-crash verified: REDIS mode, Rule 1 id={rule1_id} (AAPL <= {rule1_threshold:.2f}, score={rule1_score})")

        # Stop Redis
        self.compose("stop", "redis")
        stop_ts = time.monotonic()

        # Trip circuit breaker via emit
        fallback_before = int(status_before["fallbackMatchCount"])
        self.http_json("POST", "/api/simulator/emit")
        status_open = self.wait_until(
            "circuitBreakerState=OPEN and matchingMode=DB_FALLBACK",
            timeout_s=15.0,
            poll_s=0.3,
            predicate=lambda: (
                s
                if (
                    (s := self.get_status()).get("matchingMode") == "DB_FALLBACK"
                    and s.get("redisIndexReady") is False
                    and s.get("circuitBreakerState") == "OPEN"
                    and int(s.get("fallbackMatchCount", 0)) > fallback_before
                )
                else None
            ),
        )
        print(
            f"Circuit breaker OPEN in {time.monotonic() - stop_ts:.2f}s: "
            f"matchingMode={status_open['matchingMode']}, fallbackMatchCount={status_open['fallbackMatchCount']}"
        )

        # Trigger Rule 1 while Redis is DOWN
        self.http_json("POST", "/api/simulator/crash-storm?dropRatio=0.05")
        self.wait_until(
            "Rule 1 triggered & notified via MySQL fallback",
            timeout_s=20.0,
            poll_s=0.3,
            predicate=lambda: (
                self.mysql_int("SELECT COUNT(*) FROM alert_triggers") == 1
                and self.mysql_int("SELECT COUNT(*) FROM outbox_events WHERE status = 'SENT'") == 1
                and self.mysql_int("SELECT COUNT(*) FROM notifications") == 1
                and self.mysql_scalar(f"SELECT status FROM alert_rules WHERE id = {rule1_id}") == "COMPLETED"
            ),
        )
        print("Rule 1 triggered and delivered end-to-end while Redis was stopped.")

        # Create Rule 2 on MSFT while Redis is still DOWN
        _, msft_quote = self.http_json("GET", "/api/quotes/MSFT")
        msft_price = float(msft_quote["price"])
        msft_id = int(msft_quote["stockId"])
        rule2_threshold = round(msft_price * 0.98, 2)
        _, rule2 = self.http_json(
            "POST",
            "/api/rules",
            {
                "userId": 2,
                "symbol": "MSFT",
                "direction": "BELOW",
                "thresholdPrice": rule2_threshold,
            },
        )
        rule2_id = int(rule2["id"])
        print(f"Created Rule 2 id={rule2_id} (MSFT <= {rule2_threshold:.2f}) during Redis outage.")

        # Restart Redis (empty due to --save "" --appendonly no)
        start_ts = time.monotonic()
        self.compose("start", "redis")
        recovered_status = self.wait_for_redis_recovery_state(timeout_s=30.0)
        recovery_s = time.monotonic() - start_ts

        # Verify empty Redis was rebuilt accurately
        sentinel_after = self.redis_cli("GET", "pricepulse:v3:index:ready")
        rule2_score = self.redis_cli("ZSCORE", f"rules:below:{msft_id}", str(rule2_id))
        rule1_score_after = self.redis_cli("ZSCORE", f"rules:below:{aapl_id}", str(rule1_id))
        if sentinel_after != "1":
            raise DrillFailure(f"Sentinel missing after Redis rebuild: {sentinel_after!r}")
        if not rule2_score:
            raise DrillFailure(f"Rule 2 ({rule2_id}) missing in Redis after rebuild")
        if rule1_score_after:
            raise DrillFailure(f"Completed Rule 1 ({rule1_id}) unexpectedly present in Redis: {rule1_score_after}")

        # Verify post-recovery matching runs on Redis and triggers Rule 2
        redis_before = int(recovered_status["redisMatchCount"])
        fallback_snapshot = int(recovered_status["fallbackMatchCount"])
        self.http_json("POST", "/api/simulator/crash-storm?dropRatio=0.05")

        self.wait_until(
            "Rule 2 triggered on Redis after recovery",
            timeout_s=20.0,
            poll_s=0.3,
            predicate=lambda: (
                self.mysql_int("SELECT COUNT(*) FROM alert_triggers") == 2
                and self.mysql_int("SELECT COUNT(*) FROM outbox_events WHERE status = 'SENT'") == 2
                and self.mysql_int("SELECT COUNT(*) FROM notifications") == 2
            ),
        )
        final_status = self.get_status()
        if int(final_status["redisMatchCount"]) <= redis_before:
            raise DrillFailure(f"redisMatchCount did not increase: before={redis_before}, after={final_status['redisMatchCount']}")
        if int(final_status["fallbackMatchCount"]) != fallback_snapshot:
            raise DrillFailure(
                f"fallbackMatchCount changed after recovery: before={fallback_snapshot}, after={final_status['fallbackMatchCount']}"
            )

        print(
            f"Drill 1 PASSED: recovery_time={recovery_s:.2f}s, "
            f"redisMatchCount {redis_before}->{final_status['redisMatchCount']}, "
            f"fallbackMatchCount unchanged at {fallback_snapshot}, totals=2/2/2."
        )
        return {
            "recoverySeconds": round(recovery_s, 2),
            "redisMatchesDelta": int(final_status["redisMatchCount"]) - redis_before,
            "triggers": 2,
            "notifications": 2,
        }

    # ------------------------------------------------------------------
    # Drill 2: RocketMQ Broker Crash -> Outbox Backlog -> Auto Catch-Up
    # ------------------------------------------------------------------

    def run_drill_2(self) -> dict[str, Any]:
        print("\n=== Drill 2: RocketMQ Broker Crash -> Outbox Backlog Past Retry Window -> Auto Catch-Up ===")
        # 1. Baseline trigger so topic & consumer group routes are warmed
        self.clean_state()
        self.http_json("POST", "/api/simulator/emit")
        _, aapl_quote = self.http_json("GET", "/api/quotes/AAPL")
        self.http_json(
            "POST",
            "/api/rules",
            {
                "userId": 1,
                "symbol": "AAPL",
                "direction": "BELOW",
                "thresholdPrice": round(float(aapl_quote["price"]) * 0.98, 2),
            },
        )
        self.wait_maintainer_drained(extra_sleep_s=0.5)
        self.http_json("POST", "/api/simulator/crash-storm?dropRatio=0.05")
        self.wait_until(
            "Baseline RocketMQ notification",
            timeout_s=20.0,
            poll_s=0.3,
            predicate=lambda: (
                self.mysql_int("SELECT COUNT(*) FROM notifications") == 1
                and self.mysql_int("SELECT COUNT(*) FROM outbox_events WHERE status = 'PENDING'") == 0
            ),
        )
        print("Baseline RocketMQ publish+consume verified.")

        # 2. Clean state and create 20 BELOW rules across first 20 stocks
        self.clean_state()
        self.http_json("POST", "/api/simulator/emit")
        _, stocks = self.http_json("GET", "/api/stocks")
        stocks = sorted(stocks, key=lambda s: int(s["id"]))[:20]
        for idx, stock in enumerate(stocks):
            sym = stock["symbol"]
            _, quote = self.http_json("GET", f"/api/quotes/{urllib.parse.quote(sym)}")
            threshold = round(float(quote["price"]) * 0.98, 2)
            self.http_json(
                "POST",
                "/api/rules",
                {
                    "userId": (idx % 5) + 1,
                    "symbol": sym,
                    "direction": "BELOW",
                    "thresholdPrice": threshold,
                },
            )
        self.wait_maintainer_drained(extra_sleep_s=0.6)
        assert self.mysql_int("SELECT COUNT(*) FROM alert_rules WHERE status = 'ACTIVE'") == 20

        # 3. Stop RocketMQ broker
        self.compose("stop", "rocketmq-broker")
        outage_start = time.monotonic()
        time.sleep(3.0)

        # 4. Trigger 20 rules while broker is DOWN
        self.http_json("POST", "/api/simulator/crash-storm?dropRatio=0.05")
        self.wait_until(
            "20 triggers persisted in alert_triggers and outbox_events(PENDING)",
            timeout_s=20.0,
            poll_s=0.3,
            predicate=lambda: (
                self.mysql_int("SELECT COUNT(*) FROM alert_triggers") == 20
                and self.mysql_int("SELECT COUNT(*) FROM outbox_events WHERE status = 'PENDING'") == 20
            ),
        )
        if self.mysql_int("SELECT COUNT(*) FROM outbox_events WHERE status = 'SENT'") != 0:
            raise DrillFailure("Unexpected SENT outbox rows while broker is stopped")
        if self.mysql_int("SELECT COUNT(*) FROM notifications") != 0:
            raise DrillFailure("Unexpected notifications while broker is stopped")
        print("20 triggers accumulated in outbox_events(PENDING) with 0 SENT and 0 notifications.")

        # 5. Keep broker stopped until MAX(attempts) >= 9 (proves continuous capped backoff > 8 attempts and > 30s cap)
        max_attempts = 0
        deadline = time.monotonic() + 240.0
        while time.monotonic() < deadline:
            pending = self.mysql_int("SELECT COUNT(*) FROM outbox_events WHERE status = 'PENDING'")
            notif_cnt = self.mysql_int("SELECT COUNT(*) FROM notifications")
            max_attempts = self.mysql_int("SELECT COALESCE(MAX(attempts), 0) FROM outbox_events WHERE status = 'PENDING'")
            elapsed = time.monotonic() - outage_start
            if pending != 20 or notif_cnt != 0:
                raise DrillFailure(f"Unexpected state during outage: pending={pending}, notifications={notif_cnt}")
            print(f"  [Outage {elapsed:5.1f}s] pending={pending}, max_attempts={max_attempts}, notifications={notif_cnt}")
            if max_attempts >= 9:
                break
            time.sleep(5.0)
        if max_attempts < 9:
            raise DrillFailure(f"Timed out waiting for max_attempts >= 9 (got {max_attempts})")
        outage_seconds = time.monotonic() - outage_start

        # 6. Restart RocketMQ broker and measure catch-up
        catchup_start = time.monotonic()
        self.compose("start", "rocketmq-broker")
        self.wait_until(
            "all 20 outbox events SENT and 20 notifications persisted",
            timeout_s=120.0,
            poll_s=0.5,
            predicate=lambda: (
                self.mysql_int("SELECT COUNT(*) FROM outbox_events WHERE status = 'PENDING'") == 0
                and self.mysql_int("SELECT COUNT(*) FROM outbox_events WHERE status = 'SENT'") == 20
                and self.mysql_int("SELECT COUNT(*) FROM notifications") == 20
            ),
        )
        catchup_seconds = time.monotonic() - catchup_start

        # 7. Verify 0 lost and 0 duplicate notifications
        total_notif = self.mysql_int("SELECT COUNT(*) FROM notifications")
        distinct_notif = self.mysql_int("SELECT COUNT(DISTINCT trigger_id) FROM notifications")
        missing_notif = self.mysql_int(
            "SELECT COUNT(*) FROM alert_triggers t "
            "LEFT JOIN notifications n ON n.trigger_id = t.id "
            "WHERE n.id IS NULL"
        )
        if total_notif != 20 or distinct_notif != 20 or missing_notif != 0:
            raise DrillFailure(
                f"Notification integrity mismatch: total={total_notif}, distinct={distinct_notif}, missing={missing_notif}"
            )

        print(
            f"Drill 2 PASSED: outage={outage_seconds:.1f}s, max_attempts={max_attempts}, "
            f"catchup={catchup_seconds:.2f}s, notifications=20/20 (0 lost, 0 duplicate)."
        )
        return {
            "outageSeconds": round(outage_seconds, 1),
            "maxAttempts": max_attempts,
            "catchupSeconds": round(catchup_seconds, 2),
            "notifications": total_notif,
        }

    # ------------------------------------------------------------------
    # Drill 3: 500-Rule One-Click Crash Storm & HikariCP Health
    # ------------------------------------------------------------------

    def run_drill_3(self) -> dict[str, Any]:
        print("\n=== Drill 3: 500-Rule One-Click Crash Storm & HikariCP Health ===")
        self.clean_state()
        self.wait_for_redis_recovery_state(timeout_s=30.0)
        assert self.mysql_int("SELECT COUNT(*) FROM alert_rules") == 0

        self.http_json("POST", "/api/simulator/emit")
        _, stocks = self.http_json("GET", "/api/stocks")
        if len(stocks) != 100:
            raise DrillFailure(f"Expected 100 stocks, got {len(stocks)}")

        ratios = (0.990, 0.985, 0.980, 0.975, 0.970)
        created = 0
        for stock in stocks:
            sym = stock["symbol"]
            _, quote = self.http_json("GET", f"/api/quotes/{urllib.parse.quote(sym)}")
            price = float(quote["price"])
            for idx, ratio in enumerate(ratios):
                threshold = round(price * ratio, 2)
                self.http_json(
                    "POST",
                    "/api/rules",
                    {
                        "userId": idx + 1,
                        "symbol": sym,
                        "direction": "BELOW",
                        "thresholdPrice": threshold,
                    },
                )
                created += 1

        self.wait_maintainer_drained(extra_sleep_s=1.0, timeout_s=30.0)
        active_rules = self.mysql_int("SELECT COUNT(*) FROM alert_rules WHERE status = 'ACTIVE'")
        if created != 500 or active_rules != 500:
            raise DrillFailure(f"Expected 500 active rules, got created={created}, active={active_rules}")

        hikari_max = self.get_actuator_metric("hikaricp.connections.max")
        timeouts_before = self.get_actuator_metric("hikaricp.connections.timeout")

        log_offset_before = 0
        if self.backend_log and self.backend_log.exists():
            log_offset_before = self.backend_log.stat().st_size

        storm_start = time.monotonic()
        _, storm_resp = self.http_json("POST", "/api/simulator/crash-storm?dropRatio=0.05", timeout=60.0)
        storm_http_ms = (time.monotonic() - storm_start) * 1000.0
        print(f"Crash storm HTTP returned in {storm_http_ms:.1f}ms: {storm_resp}")

        self.wait_until(
            "500 triggers, 500 SENT outbox events, 500 notifications",
            timeout_s=60.0,
            poll_s=0.3,
            predicate=lambda: (
                self.mysql_int("SELECT COUNT(*) FROM alert_triggers") == 500
                and self.mysql_int("SELECT COUNT(*) FROM outbox_events WHERE status = 'SENT'") == 500
                and self.mysql_int("SELECT COUNT(*) FROM outbox_events WHERE status = 'PENDING'") == 0
                and self.mysql_int("SELECT COUNT(*) FROM notifications") == 500
            ),
        )
        pipeline_drain_s = time.monotonic() - storm_start
        timeouts_after = self.get_actuator_metric("hikaricp.connections.timeout")
        timeouts_delta = timeouts_after - timeouts_before

        completed_rules = self.mysql_int("SELECT COUNT(*) FROM alert_rules WHERE status = 'COMPLETED'")
        remaining_active = self.mysql_int("SELECT COUNT(*) FROM alert_rules WHERE status = 'ACTIVE'")
        triggers_total = self.mysql_int("SELECT COUNT(*) FROM alert_triggers")
        triggers_distinct_rule = self.mysql_int("SELECT COUNT(DISTINCT rule_id) FROM alert_triggers")
        outbox_total = self.mysql_int("SELECT COUNT(*) FROM outbox_events")
        outbox_sent = self.mysql_int("SELECT COUNT(*) FROM outbox_events WHERE status = 'SENT'")
        outbox_distinct_agg = self.mysql_int("SELECT COUNT(DISTINCT aggregate_id) FROM outbox_events")
        notif_total = self.mysql_int("SELECT COUNT(*) FROM notifications")
        notif_distinct_trig = self.mysql_int("SELECT COUNT(DISTINCT trigger_id) FROM notifications")
        missing_notifs = self.mysql_int(
            "SELECT COUNT(*) FROM alert_triggers t "
            "LEFT JOIN notifications n ON n.trigger_id = t.id "
            "WHERE n.id IS NULL"
        )

        if (
            completed_rules != 500
            or remaining_active != 0
            or triggers_total != 500
            or triggers_distinct_rule != 500
            or outbox_total != 500
            or outbox_sent != 500
            or outbox_distinct_agg != 500
            or notif_total != 500
            or notif_distinct_trig != 500
            or missing_notifs != 0
        ):
            raise DrillFailure(
                f"3-table parity failed: completed={completed_rules}, active={remaining_active}, "
                f"triggers={triggers_total}/{triggers_distinct_rule}, "
                f"outbox={outbox_total}/sent={outbox_sent}/agg={outbox_distinct_agg}, "
                f"notif={notif_total}/{notif_distinct_trig}, missing={missing_notifs}"
            )
        if timeouts_delta != 0:
            raise DrillFailure(f"HikariCP connection timeout occurred during storm: delta={timeouts_delta}")

        log_errors = 0
        if self.backend_log and self.backend_log.exists():
            with self.backend_log.open("r", encoding="utf-8", errors="replace") as fh:
                fh.seek(log_offset_before)
                chunk = fh.read()
            pattern = re.compile(
                r"SQLTransientConnectionException|Connection is not available|HikariPool.*(Timeout|timed out|Exception|Error)",
                re.IGNORECASE,
            )
            matches = pattern.findall(chunk)
            log_errors = len(matches)
            if log_errors > 0:
                raise DrillFailure(f"Detected {log_errors} HikariCP/connection error lines in backend log")

        print(
            f"Drill 3 PASSED: storm_http={storm_http_ms:.1f}ms, e2e_drain={pipeline_drain_s:.2f}s, "
            f"triggers=500, outbox(SENT)=500, notifications=500, "
            f"hikariMax={int(hikari_max)}, hikariTimeoutsDelta={int(timeouts_delta)}, logPoolErrors={log_errors}."
        )
        return {
            "stormHttpMs": round(storm_http_ms, 1),
            "pipelineDrainSeconds": round(pipeline_drain_s, 2),
            "triggers": triggers_total,
            "outboxSent": outbox_sent,
            "notifications": notif_total,
            "hikariMaxPoolSize": int(hikari_max),
            "hikariTimeoutsDelta": int(timeouts_delta),
            "logPoolErrors": log_errors,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run PricePulse V3 Phase 6 fault drills")
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--compose-file", default="compose.yaml")
    parser.add_argument("--db-user", default="pricepulse")
    parser.add_argument("--db-password", default="pricepulse_local")
    parser.add_argument("--db-name", default="price_pulse")
    parser.add_argument("--backend-log", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runner = FaultDrillRunner(
        base_url=args.base_url,
        compose_file=args.compose_file,
        db_user=args.db_user,
        db_password=args.db_password,
        db_name=args.db_name,
        backend_log=args.backend_log,
    )
    try:
        runner.ensure_services_running()
        _, health = runner.http_json("GET", "/actuator/health")
        if health.get("status") != "UP":
            raise DrillFailure(f"Backend health is not UP: {health}")

        r1 = runner.run_drill_1()
        r2 = runner.run_drill_2()
        r3 = runner.run_drill_3()
        runner.clean_state()

        summary = {"drill1": r1, "drill2": r2, "drill3": r3}
        print("\n=== PHASE6 FAULT DRILLS SUMMARY ===")
        print(json.dumps(summary, indent=2))
        return 0
    finally:
        runner.ensure_services_running()


if __name__ == "__main__":
    sys.exit(main())
