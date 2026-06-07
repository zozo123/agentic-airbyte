#!/usr/bin/env python3
"""
etl.py — the movement engine that runs *inside the sandbox worker*.

It mirrors Airbyte's source -> destination contract end to end:

  DISCOVER  ask the source for its schema (a catalog / stream)
  READ      pull RECORD messages from the source in batches (JSONEachRow)
  WRITE     load those records into the destination, typed
  VERIFY    prove src and dst agree (row count, revenue sum, per-type tally,
            and a content checksum) — this is the actual "proof"
  ANALYTICS run a query *on the destination* so we know the moved data is live
  STATE     emit a final state + metrics.json the harness can reason over

Standard pipe = ClickHouse (source) -> DuckDB (destination). Same protocol
Airbyte uses; here it's the lightweight custom-CDK path so the whole thing
runs self-contained in seconds inside a borrowed box.
"""
import hashlib
import json
import os
import sys
import time
import urllib.parse
import urllib.request

import duckdb

T0 = time.time()


def phase(name):
    # Crabbox reads these markers to attach timings/evidence to each step.
    print(f"::CRABBOX_PHASE::{name}", flush=True)


def alog(level, msg, **extra):
    # Airbyte-protocol-flavored structured log line.
    rec = {"type": "LOG", "log": {"level": level, "message": msg}}
    if extra:
        rec["log"].update(extra)
    print(json.dumps(rec), flush=True)


def ch(host, port, sql, body=None, settings=None):
    q = sql
    params = {"query": q}
    if settings:
        params.update(settings)
    url = f"http://{host}:{port}/?{urllib.parse.urlencode(params)}"
    data = body.encode() if body else None
    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=300) as r:
        return r.read().decode()


CH_TO_DUCK = {
    "UInt8": "BIGINT", "UInt16": "BIGINT", "UInt32": "BIGINT", "UInt64": "BIGINT",
    "Int8": "BIGINT", "Int16": "BIGINT", "Int32": "BIGINT", "Int64": "BIGINT",
    "Float32": "DOUBLE", "Float64": "DOUBLE",
    "String": "VARCHAR", "DateTime": "TIMESTAMP", "Date": "DATE",
}


def map_type(ch_type):
    t = ch_type
    if t.startswith("LowCardinality("):
        t = t[len("LowCardinality("):-1]
    if t.startswith("Nullable("):
        t = t[len("Nullable("):-1]
    if t.startswith("Decimal"):
        return "DECIMAL(18,2)"
    return CH_TO_DUCK.get(t, "VARCHAR")


def main():
    cfg = json.load(open(os.path.join(os.path.dirname(__file__), "config.json")))
    src, dst, sync = cfg["source"], cfg["destination"], cfg["sync"]
    host, port = src["host"], src["port"]
    db, stream = src["database"], src["stream"]
    fq = f"{db}.{stream}"

    # ---- DISCOVER -----------------------------------------------------------
    phase("discover")
    desc = ch(host, port, f"DESCRIBE TABLE {fq} FORMAT JSONEachRow")
    columns = []  # [(name, ch_type, duck_type)]
    for line in desc.strip().splitlines():
        row = json.loads(line)
        columns.append((row["name"], row["type"], map_type(row["type"])))
    alog("INFO", f"discovered stream '{stream}'",
         catalog=[{"name": n, "source_type": ct, "dest_type": dt} for n, ct, dt in columns])

    src_count = int(ch(host, port, f"SELECT count() FROM {fq}").strip())
    src_rev = ch(host, port, f"SELECT toString(round(sum(revenue),2)) FROM {fq}").strip()
    alog("INFO", f"source has {src_count} records, total_revenue={src_rev}")

    # Per-column SELECT: render decimals/timestamps as clean JSON scalars so the
    # destination cast is exact and lossless.
    select_exprs = []
    for name, ct, _ in columns:
        base = ct[len("LowCardinality("):-1] if ct.startswith("LowCardinality(") else ct
        if base.startswith("Decimal") or base.startswith("DateTime") or base.startswith("Date"):
            select_exprs.append(f"toString({name}) AS {name}")
        else:
            select_exprs.append(name)
    select_list = ", ".join(select_exprs)

    # ---- WRITE (prepare destination) ---------------------------------------
    phase("write_setup")
    os.makedirs(os.path.dirname(dst["path"]) or ".", exist_ok=True)
    if os.path.exists(dst["path"]):
        os.remove(dst["path"])
    con = duckdb.connect(dst["path"])
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {dst['schema']}")
    ddl_cols = ", ".join(f"{n} {dt}" for n, _, dt in columns)
    dest_fq = f"{dst['schema']}.{dst['table']}"
    con.execute(f"DROP TABLE IF EXISTS {dest_fq}")
    con.execute(f"CREATE TABLE {dest_fq} ({ddl_cols})")
    col_names = [n for n, _, _ in columns]
    # read_json column spec — explicit types make the source->dest cast exact.
    json_cols = ", ".join(f"'{n}': '{dt}'" for n, _, dt in columns)
    batch_file = os.path.abspath(os.path.join("reports", ".batch.jsonl"))

    # ---- READ + WRITE (stream batches) --------------------------------------
    phase("sync")
    batch = sync["batch_size"]
    moved = 0
    bytes_in = 0
    read_t = 0.0
    write_t = 0.0
    while moved < src_count:
        rt = time.time()
        page = ch(
            host, port,
            f"SELECT {select_list} FROM {fq} ORDER BY {src['primary_key']} "
            f"LIMIT {batch} OFFSET {moved} FORMAT JSONEachRow",
            settings={"output_format_json_quote_64bit_integers": 0},
        )
        read_t += time.time() - rt
        page = page.strip()
        if not page:
            break
        bytes_in += len(page.encode())
        n_rows = page.count("\n") + 1
        # Persist the RECORD batch as JSONL (exactly Airbyte record messages),
        # then bulk-load it with DuckDB's vectorized json reader.
        with open(batch_file, "w") as f:
            f.write(page)
        wt = time.time()
        con.execute(
            f"INSERT INTO {dest_fq} ({', '.join(col_names)}) "
            f"SELECT {', '.join(col_names)} FROM read_json('{batch_file}', "
            f"format='newline_delimited', columns={{{json_cols}}})"
        )
        write_t += time.time() - wt
        moved += n_rows
        alog("INFO", f"synced {moved}/{src_count} records")
    con.commit()
    if os.path.exists(batch_file):
        os.remove(batch_file)

    # ---- VERIFY -------------------------------------------------------------
    phase("verify")
    dst_count = con.execute(f"SELECT count(*) FROM {dest_fq}").fetchone()[0]
    dst_rev = con.execute(
        f"SELECT printf('%.2f', sum(revenue)) FROM {dest_fq}"
    ).fetchone()[0]

    # per-event_type tally on both sides
    src_tally = {}
    for line in ch(
        host, port,
        f"SELECT event_type, toString(count()) c FROM {fq} GROUP BY event_type "
        f"ORDER BY event_type FORMAT JSONEachRow",
    ).strip().splitlines():
        r = json.loads(line)
        src_tally[r["event_type"]] = int(r["c"])
    dst_tally = {
        et: c for et, c in con.execute(
            f"SELECT event_type, count(*) FROM {dest_fq} GROUP BY event_type ORDER BY event_type"
        ).fetchall()
    }

    # content checksum: hash the sorted (event_id, event_type, revenue) tuples
    def checksum_src():
        h = hashlib.sha256()
        for line in ch(
            host, port,
            f"SELECT event_id, event_type, toDecimalString(revenue, 2) r FROM {fq} "
            f"ORDER BY event_id FORMAT JSONEachRow",
            settings={"output_format_json_quote_64bit_integers": 0},
        ).strip().splitlines():
            r = json.loads(line)
            h.update(f"{r['event_id']}|{r['event_type']}|{r['r']}\n".encode())
        return h.hexdigest()

    def checksum_dst():
        h = hashlib.sha256()
        for eid, et, rev in con.execute(
            f"SELECT event_id, event_type, printf('%.2f', revenue) FROM {dest_fq} ORDER BY event_id"
        ).fetchall():
            h.update(f"{eid}|{et}|{rev}\n".encode())
        return h.hexdigest()

    cs_src, cs_dst = checksum_src(), checksum_dst()
    checks = {
        "row_count": {"src": src_count, "dst": dst_count, "ok": src_count == dst_count},
        "revenue_sum": {"src": src_rev, "dst": dst_rev, "ok": src_rev == dst_rev},
        "per_type_tally": {"src": src_tally, "dst": dst_tally, "ok": src_tally == dst_tally},
        "content_sha256": {"src": cs_src, "dst": cs_dst, "ok": cs_src == cs_dst},
    }
    all_ok = all(c["ok"] for c in checks.values())

    # ---- ANALYTICS on the destination (prove it's live & queryable) --------
    phase("analytics")
    top_countries = con.execute(
        f"""SELECT country,
                   count(*) AS events,
                   round(sum(revenue), 2) AS revenue
            FROM {dest_fq}
            GROUP BY country ORDER BY revenue DESC LIMIT 5"""
    ).fetchall()
    top_countries = [
        {"country": c, "events": e, "revenue": float(r)} for c, e, r in top_countries
    ]
    funnel = dict(con.execute(
        f"SELECT event_type, count(*) FROM {dest_fq} GROUP BY event_type"
    ).fetchall())

    # ---- METRICS / STATE ----------------------------------------------------
    phase("emit")
    duration = round(time.time() - T0, 3)
    metrics = {
        "status": "SUCCEEDED" if all_ok else "FAILED",
        "source": {"type": src["type"], "stream": fq, "host": host},
        "destination": {"type": dst["type"], "table": dest_fq, "path": dst["path"]},
        "sync_mode": sync["mode"],
        "records_moved": moved,
        "bytes_read": bytes_in,
        "batches": (moved + batch - 1) // batch,
        "duration_seconds": duration,
        "sync_seconds": round(read_t + write_t, 3),
        "throughput_rows_per_sec": round(moved / (read_t + write_t), 1) if (read_t + write_t) else None,
        "read_seconds": round(read_t, 3),
        "write_seconds": round(write_t, 3),
        "checks": checks,
        "all_checks_passed": all_ok,
        "analytics": {"top_countries_by_revenue": top_countries, "funnel": funnel},
        "schema": [{"name": n, "source_type": ct, "dest_type": dt} for n, ct, dt in columns],
    }
    out = os.path.join("reports", "metrics.json")
    os.makedirs("reports", exist_ok=True)
    json.dump(metrics, open(out, "w"), indent=2)

    print(json.dumps({"type": "STATE", "state": {"records_moved": moved, "status": metrics["status"]}}), flush=True)
    alog("INFO" if all_ok else "ERROR",
         f"sync {metrics['status']}: moved {moved} rows in {duration}s "
         f"({metrics['throughput_rows_per_sec']} rows/s); checks_passed={all_ok}")
    con.close()
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
