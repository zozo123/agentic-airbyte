#!/usr/bin/env python3
"""
seed.py — populate the *source* ClickHouse with real, deterministic data.

This is the "system of record" the harness is asked to move. We generate a
realistic web-analytics event stream (page views, carts, purchases) so the
end-to-end proof moves data that actually means something, not toy rows.

Deterministic: a fixed RNG seed => identical bytes on every run => the
destination checksum is reproducible and auditable.
"""
import argparse
import json
import random
import sys
import urllib.parse
import urllib.request

EVENT_TYPES = ["page_view", "search", "add_to_cart", "checkout", "purchase"]
COUNTRIES = ["US", "DE", "IL", "GB", "BR", "IN", "JP", "CA", "FR", "AU"]
DEVICES = ["desktop", "mobile", "tablet"]
CHANNELS = ["organic", "paid_search", "email", "social", "referral"]


def ch(host, port, sql, body=None):
    url = f"http://{host}:{port}/?query={urllib.parse.quote(sql)}"
    data = body.encode() if body else None
    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read().decode()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8123)
    ap.add_argument("--database", default="analytics")
    ap.add_argument("--rows", type=int, default=50000)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rng = random.Random(args.seed)

    ch(args.host, args.port, f"CREATE DATABASE IF NOT EXISTS {args.database}")
    ch(args.host, args.port, f"DROP TABLE IF EXISTS {args.database}.events")
    ch(
        args.host,
        args.port,
        f"""CREATE TABLE {args.database}.events (
            event_id    UInt64,
            user_id     UInt32,
            session_id  UInt32,
            event_type  LowCardinality(String),
            channel     LowCardinality(String),
            device      LowCardinality(String),
            country     LowCardinality(String),
            url         String,
            revenue     Decimal(12, 2),
            ts          DateTime
        ) ENGINE = MergeTree ORDER BY (ts, event_id)""",
    )

    # Build a JSONEachRow body — this is exactly the shape Airbyte sources emit.
    base_ts = 1_717_200_000  # 2024-06-01T00:00:00Z, fixed for reproducibility
    lines = []
    for i in range(args.rows):
        et = rng.choices(EVENT_TYPES, weights=[60, 15, 12, 8, 5])[0]
        revenue = round(rng.uniform(8.0, 480.0), 2) if et == "purchase" else 0.0
        rec = {
            "event_id": i + 1,
            "user_id": rng.randint(1, 9000),
            "session_id": rng.randint(1, 30000),
            "event_type": et,
            "channel": rng.choice(CHANNELS),
            "device": rng.choices(DEVICES, weights=[45, 45, 10])[0],
            "country": rng.choice(COUNTRIES),
            "url": "/p/" + str(rng.randint(100, 999)),
            "revenue": revenue,
            "ts": base_ts + i * 7,
        }
        lines.append(json.dumps(rec))

    body = "\n".join(lines)
    ch(
        args.host,
        args.port,
        f"INSERT INTO {args.database}.events FORMAT JSONEachRow",
        body=body,
    )

    count = ch(
        args.host, args.port, f"SELECT count() FROM {args.database}.events"
    ).strip()
    rev = ch(
        args.host,
        args.port,
        f"SELECT toString(sum(revenue)) FROM {args.database}.events",
    ).strip()
    print(
        f"[seed] {args.database}.events ready: rows={count} total_revenue={rev}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
