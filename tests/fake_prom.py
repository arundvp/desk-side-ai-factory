#!/usr/bin/env python3
"""fake_prom.py — Prometheus HTTP API stand-in for offline tests of analyze.py.

Every query is parsed with promql_parser (a syntax error returns HTTP 400, like Prometheus).
Values come from a synthetic model of vLLM on one Spark driven by the stage schedule in
stages.json: throughput saturates at max-num-seqs=64, requests queue above it, the GPU
clock drops at high load, and DCGM returns nothing (the GB10 case).
usage: fake_prom.py <port> <stages.json>
"""
import http.server
import json
import re
import sys
import urllib.parse

import promql_parser

PORT = int(sys.argv[1])
STAGES = json.load(open(sys.argv[2]))["stages"]
QUERIES = []


def conc(t):
    for s in STAGES:
        if s["start"] <= t <= s["end"]:
            return s.get("concurrency") or (s.get("rate_rps") or 0) * 20
    return 0


def gen_rate(c):
    c = min(c, 64)
    return 900.0 * c / (c + 20) if c else 0.0


def model(key, c, q=0.5):
    run = min(c, 64)
    g = gen_rate(c)
    itl = (run / g) if g else 0.0
    queue = max(0, c - 64) * 0.4
    vals = {
        "generation_tokens": g, "prompt_tokens": g * 3, "request_success": g / 400,
        "ttft": (0.3 + 0.01 * run) * (1 + q) + queue, "itl": itl * (1 + 0.3 * q),
        "e2e": (0.3 + 0.01 * run) + queue + 400 * itl, "queue": queue * (0.5 + q),
        "running": run, "waiting": max(0, c - 64), "kv": min(0.97, 0.15 + run / 100) if c else 0.0,
        "preemptions": 3.0 / 600 if c >= 128 else 0.0,
        "prefix_hits": g * 0.3, "prefix_queries": g * 3,
        "gpu_util": min(97.0, 25 + 1.2 * run) if c else 3.0, "gpu_power": 35 + 0.5 * run if c else 12.0,
        "gpu_temp": 52 + run / 3, "sm_clock_max": 3003.0, "sm_clock": 3003.0 if c < 64 else 2400.0,
        "reasons": 0.0 if c < 64 else 4.0, "mem": (26 - run * 0.05) * 2 ** 30, "cpu": 0.12,
        "target": float(c), "inflight": float(c),
    }
    return vals.get(key)


KEYS = [
    ("DCGM_FI", None), ("generation_tokens", "generation_tokens"), ("prefix_cache_hits", "prefix_hits"),
    ("prefix_cache_queries", "prefix_queries"), ("prompt_tokens", "prompt_tokens"),
    ("request_success", "request_success"), ("time_to_first_token", "ttft"), ("ttft", "ttft"),
    ("inter_token_latency", "itl"), ("itl_seconds", "itl"), ("e2e", "e2e"), ("queue", "queue"),
    ("num_requests_running", "running"), ("num_requests_waiting", "waiting"), ("kv_cache_usage", "kv"),
    ("num_preemptions", "preemptions"), ("gpu_utilization", "gpu_util"), ("gpu_power", "gpu_power"),
    ("gpu_temperature", "gpu_temp"), ("gpu_sm_clock_max", "sm_clock_max"), ("gpu_sm_clock", "sm_clock"),
    ("clock_event_reasons", "reasons"), ("node_memory_available", "mem"), ("node_cpu_busy", "cpu"),
    ("loadgen_stage_concurrency", "target"), ("loadgen_inflight", "inflight"),
]


def key_of(expr):
    for needle, key in KEYS:
        if needle in expr:
            return key, True
    return None, False


def evaluate(expr, t):
    """Value of expr at time t, or None for 'no data'."""
    key, found = key_of(expr)
    if not found or key is None:
        return None
    div = 2 ** 30 if expr.rstrip().endswith("/ 2^30") else 1
    m = re.match(r"histogram_quantile\(([0-9.]+),", expr)
    if m:
        w = re.search(r"\[(\d+)s\]", expr)
        n = int(w.group(1)) if w else 60
        c = conc(t - n / 2)
        return model(key, c, float(m.group(1))) if c else None
    m = re.match(r"(avg|min|max)_over_time\(\((.*)\)\[(\d+)s:5s\]\)$", expr)
    if m:
        fn, n = m.group(1), int(m.group(3))
        xs = [model(key, conc(t - k)) for k in range(0, n + 1, 5)]
        xs = [x for x in xs if x is not None]
        if not xs:
            return None
        v = {"avg": sum(xs) / len(xs), "min": min(xs), "max": max(xs)}[fn]
        return v / div
    m = re.search(r"increase\([^\[]*\[(\d+)s\]\)", expr)
    if m:
        n = int(m.group(1))
        xs = [model(key, conc(t - k)) or 0.0 for k in range(0, n, 5)]
        return sum(xs) / max(1, len(xs)) * n
    v = model(key, conc(t))
    return None if v is None else v / div


class H(http.server.BaseHTTPRequestHandler):
    def _send(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(u.query)
        if u.path == "/":
            return self._send(200, {})
        expr = qs.get("query", [""])[0]
        QUERIES.append(expr)
        try:
            promql_parser.parse(expr)
        except Exception as e:  # noqa: BLE001
            return self._send(400, {"status": "error", "errorType": "bad_data", "error": str(e)})
        node = {"node": "spark-1"} if "by (node)" in expr else {}
        if u.path == "/api/v1/query":
            t = float(qs["time"][0])
            v = evaluate(expr, t)
            res = [] if v is None else [{"metric": node, "value": [t, repr(v)]}]
            if "by (node)" in expr and v is None and "num_requests_running" in expr:
                res = [{"metric": node, "value": [t, "64"]}]
            return self._send(200, {"status": "success", "data": {"resultType": "vector", "result": res}})
        if u.path == "/api/v1/query_range":
            start, end, step = float(qs["start"][0]), float(qs["end"][0]), float(qs["step"][0])
            vals = []
            t = start
            while t <= end:
                v = evaluate(expr, t)
                if v is not None:
                    vals.append([t, repr(v)])
                t += step
            res = [{"metric": node, "values": vals}] if vals else []
            return self._send(200, {"status": "success", "data": {"resultType": "matrix", "result": res}})
        return self._send(404, {"status": "error"})

    def log_message(self, *_a):
        pass


if __name__ == "__main__":
    http.server.ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()
