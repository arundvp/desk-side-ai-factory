#!/usr/bin/env python3
"""analyze.py — turn a load run into numbers: per-stage server, GPU and node metrics from
Prometheus, client statistics from the load generator, findings, charts and a report.

Reads   <run>/stages.json (written by loadgen.py or the vllm bench sweep job)
        <run>/summary.json and requests.csv if present (loadgen client view)
        <run>/c*.json if present (vllm bench results)
Writes  <run>/analysis/report.md        stage tables + findings
        <run>/analysis/stages.csv       one row per stage, every metric
        <run>/analysis/timeseries.csv   5 s samples of the key series for the whole run
        <run>/analysis/*.png            charts (only if matplotlib is installed)

Standard library only (matplotlib optional). Prometheus must be reachable, e.g.:
  kubectl -n monitoring port-forward svc/kps-prometheus 9090:9090

usage: analyze.py <run-dir> [--prom http://127.0.0.1:9090] [--trim 0.15] [--compare <other-run-dir>]
"""
import argparse
import csv
import json
import math
import os
import sys
import urllib.parse
import urllib.request

STEP = 5


class Prom:
    def __init__(self, url):
        self.url = url.rstrip("/")

    def _get(self, path, params):
        q = urllib.parse.urlencode(params)
        with urllib.request.urlopen(f"{self.url}{path}?{q}", timeout=60) as r:
            body = json.load(r)
        if body.get("status") != "success":
            raise RuntimeError(f"prometheus error for {params}: {body}")
        return body["data"]

    def scalar(self, expr, at):
        """First sample of an instant query, as float or None (NaN/empty -> None)."""
        data = self._get("/api/v1/query", {"query": expr, "time": f"{at:.3f}"})
        res = data.get("result", [])
        if data.get("resultType") == "scalar":
            return _f(res[1])
        if not res:
            return None
        return _f(res[0]["value"][1])

    def vector(self, expr, at):
        data = self._get("/api/v1/query", {"query": expr, "time": f"{at:.3f}"})
        return [(r["metric"], _f(r["value"][1])) for r in data.get("result", [])]

    def series(self, expr, start, end, step=STEP):
        data = self._get("/api/v1/query_range", {"query": expr, "start": f"{start:.3f}", "end": f"{end:.3f}",
                                                 "step": str(step)})
        out = []
        for r in data.get("result", []):
            out.append((r["metric"], [(float(t), _f(v)) for t, v in r["values"]]))
        return out


def _f(v):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(x) or math.isinf(x) else x


def fmt(v, nd=2, scale=1.0):
    return "–" if v is None else f"{v * scale:.{nd}f}"


# ----------------------------------------------------------------------------------------
def stage_metrics(p, st, trim, nodes_re):
    s0 = st["start"] + max(10.0, trim * (st["end"] - st["start"]))
    e = st["end"]
    if e - s0 < 15:
        s0 = st["start"]
    d = max(STEP, int(e - s0))
    w = f"{d}s"
    sel = f'{{node=~"{nodes_re}"}}' if nodes_re else ""

    def hq(q, m):
        return p.scalar(f"histogram_quantile({q}, sum by (le) (increase({m}_bucket[{w}])))", e)

    def over(fn, expr):
        return p.scalar(f"{fn}_over_time(({expr})[{w}:{STEP}s])", e)

    m = {
        "stage": st["name"], "concurrency": st.get("concurrency"), "rate_rps": st.get("rate_rps"),
        "window_start": round(s0, 1), "window_end": round(e, 1), "window_s": d,
        "gen_tok_s": _div(p.scalar(f"sum(increase(vllm:generation_tokens_total[{w}]))", e), d),
        "prompt_tok_s": _div(p.scalar(f"sum(increase(vllm:prompt_tokens_total[{w}]))", e), d),
        "req_s": _div(p.scalar(f"sum(increase(vllm:request_success_total[{w}]))", e), d),
        "ttft_p50": hq(0.5, "vllm:time_to_first_token_seconds"),
        "ttft_p95": hq(0.95, "vllm:time_to_first_token_seconds"),
        "ttft_p99": hq(0.99, "vllm:time_to_first_token_seconds"),
        "itl_p50": hq(0.5, "vllm:inter_token_latency_seconds"),
        "itl_p95": hq(0.95, "vllm:inter_token_latency_seconds"),
        "e2e_p50": hq(0.5, "vllm:e2e_request_latency_seconds"),
        "e2e_p95": hq(0.95, "vllm:e2e_request_latency_seconds"),
        "queue_p95": hq(0.95, "vllm:request_queue_time_seconds"),
        "running_avg": over("avg", "sum(vllm:num_requests_running)"),
        "running_max": over("max", "sum(vllm:num_requests_running)"),
        "waiting_avg": over("avg", "sum(vllm:num_requests_waiting)"),
        "waiting_max": over("max", "sum(vllm:num_requests_waiting)"),
        "kv_avg": over("avg", "max(vllm:kv_cache_usage_perc)"),
        "kv_max": over("max", "max(vllm:kv_cache_usage_perc)"),
        "preemptions": p.scalar(f"sum(increase(vllm:num_preemptions_total[{w}]))", e),
        "prefix_hit": _div(p.scalar(f"sum(increase(vllm:prefix_cache_hits_total[{w}]))", e),
                           p.scalar(f"sum(increase(vllm:prefix_cache_queries_total[{w}]))", e)),
        "gpu_util_avg": over("avg", f"avg(spark:gpu_utilization:percent{sel})"),
        "gpu_util_max": over("max", f"max(spark:gpu_utilization:percent{sel})"),
        "gpu_power_avg": over("avg", f"avg(spark:gpu_power:watts{sel})"),
        "gpu_power_max": over("max", f"max(spark:gpu_power:watts{sel})"),
        "gpu_temp_max": over("max", f"max(spark:gpu_temperature:celsius{sel})"),
        "sm_clock_avg": over("avg", f"avg(spark:gpu_sm_clock:mhz{sel})"),
        "sm_clock_min": over("min", f"min(spark:gpu_sm_clock:mhz{sel})"),
        "sm_clock_max_spec": over("max", f"max(spark:gpu_sm_clock_max:mhz{sel})"),
        "clock_reasons_max": over("max", f"max(spark:gpu_clock_event_reasons:bitmask{sel})"),
        "mem_avail_min_gib": _div(over("min", f"min(spark:node_memory_available:bytes{sel})"), 2 ** 30),
        "cpu_busy_avg": over("avg", f"avg(spark:node_cpu_busy:ratio{sel})"),
        "dcgm_series": p.scalar(
            'count({__name__=~"DCGM_FI_DEV_GPU_UTIL|DCGM_FI_DEV_POWER_USAGE"'
            + (f', node=~"{nodes_re}"' if nodes_re else "") + "})", e),
    }
    # per-replica split: a Kubernetes Service balances TCP connections, not requests, so clients that keep
    # connections open can pin most of the load on one replica
    run_by = {mm.get("node", "?"): v for mm, v in p.vector(
        f"avg_over_time((sum by (node) (vllm:num_requests_running{sel}))[{w}:{STEP}s])", e) if v is not None}
    done_by = {mm.get("node", "?"): v for mm, v in p.vector(
        f"sum by (node) (increase(vllm:request_success_total{sel}[{w}]))", e) if v is not None}
    m["running_by_node"] = " ".join(f"{n}:{v:.1f}" for n, v in sorted(run_by.items()))
    m["done_by_node"] = " ".join(f"{n}:{v:.0f}" for n, v in sorted(done_by.items()))
    tot = sum(run_by.values())
    m["busiest_replica_share"] = (max(run_by.values()) / tot) if len(run_by) > 1 and tot > 0 else None
    if m["itl_p50"]:
        m["user_tok_s_p50"] = 1.0 / m["itl_p50"]
    else:
        m["user_tok_s_p50"] = None
    return m


def _div(a, b):
    if a is None or not b:
        return None
    return a / b


# ----------------------------------------------------------------------------------------
def findings(rows, client):
    out = []
    load_rows = [r for r in rows if (r["concurrency"] or r["rate_rps"]) and r["gen_tok_s"]]
    # A stage is "carry-over" when the server ran far more requests than the stage offered, e.g. a
    # cool-down at c1 while 90 requests from c128 still drain. Its server-side rates belong to the
    # earlier stage, so it is left out of the throughput curve.
    carry = [r for r in load_rows if r["concurrency"] and (r["running_avg"] or 0) > 1.25 * r["concurrency"] + 1]
    load_rows = [r for r in load_rows if r not in carry]
    key = "concurrency" if any(r["concurrency"] for r in load_rows) else "rate_rps"
    srt = sorted([r for r in load_rows if r[key]], key=lambda r: r[key])
    # de-duplicate warm-up/cool-down stages at the same load level: keep the busier one
    by_level = {}
    for r in srt:
        if r[key] not in by_level or (r["gen_tok_s"] or 0) > (by_level[r[key]]["gen_tok_s"] or 0):
            by_level[r[key]] = r
    curve = [by_level[k] for k in sorted(by_level)]

    if curve:
        best = max(curve, key=lambda r: r["gen_tok_s"] or 0)
        out.append(f"Peak generation throughput {fmt(best['gen_tok_s'], 0)} tok/s at {key.replace('_', ' ')} "
                   f"{best[key]} (stage {best['stage']}); per-user decode there {fmt(best['user_tok_s_p50'], 1)} tok/s "
                   f"(ITL p50 {fmt(best['itl_p50'], 3)} s).")
    knee = None
    for a, b in zip(curve, curve[1:]):
        if a["gen_tok_s"] and b["gen_tok_s"] and a["itl_p50"] and b["itl_p50"]:
            gain = b["gen_tok_s"] / a["gen_tok_s"] - 1
            lat = b["itl_p50"] / a["itl_p50"] - 1
            if gain < 0.10:   # throughput stopped rising: any further load can only add latency
                knee = (a, b, gain, lat)
                break
    if knee:
        a, b, gain, lat = knee
        out.append(f"Knee between {key.replace('_', ' ')} {a[key]} and {b[key]}: throughput {gain:+.0%}, ITL p50 "
                   f"{lat:+.0%}, TTFT p95 {fmt(a['ttft_p95'])} -> {fmt(b['ttft_p95'])} s. "
                   f"Load beyond {a[key]} buys latency, not throughput.")
    elif len(curve) >= 3:
        out.append("No clear knee: throughput still rose at the highest load tested; push concurrency further.")

    if carry:
        out.append("Left out of the throughput curve (server still draining earlier stages): " + ", ".join(
            f"{r['stage']} (offered {r['concurrency']}, server ran {fmt(r['running_avg'], 0)} on average)" for r in carry) + ".")

    seq_cap = float(os.environ.get("VLLM_MAX_NUM_SEQS", "64"))   # --max-num-seqs of the server under test
    queued, kvfull, busy, idle_wait, capped, hot, lowmem = [], [], [], [], [], [], []
    for r in rows:
        st = r["stage"]
        if (r["waiting_avg"] or 0) >= 0.5:
            kv_bound = (r["kv_max"] or 0) >= 0.9
            plateau = (r["running_max"] or 0) >= 0.95 * seq_cap
            long_pr = bool(r["req_s"]) and bool(r["prompt_tok_s"]) and r["prompt_tok_s"] / r["req_s"] > 4096
            cause = "KV cache full" if kv_bound else "scheduler full (running at the max-num-seqs cap)" if plateau \
                else "prefill token budget (max-num-batched-tokens): long prompts are admitted one or two per step" \
                if long_pr else "admission backlog"
            queued.append(f"{st} (waiting avg {fmt(r['waiting_avg'], 1)}, queue p95 {fmt(r['queue_p95'], 1)} s "
                          f"of TTFT p95 {fmt(r['ttft_p95'], 1)} s, {cause})")
        if (r["kv_max"] or 0) >= 0.9 or (r["preemptions"] or 0) >= 1:
            kvfull.append(f"{st} ({fmt(r['kv_max'], 0, 100)}% max, {fmt(r['preemptions'], 0)} preemptions)")
        if (r["gpu_util_avg"] or 0) >= 90:
            busy.append(f"{st} ({fmt(r['gpu_util_avg'], 0)}%)")
        elif r["gpu_util_avg"] is not None and r["gpu_util_avg"] < 50 and (r["waiting_avg"] or 0) >= 0.5:
            idle_wait.append(f"{st} ({fmt(r['gpu_util_avg'], 0)}% busy)")
        spec = r["sm_clock_max_spec"]
        limiter = int(r["clock_reasons_max"] or 0) & 0xEC   # SW power cap, HW slowdown, SW/HW thermal, power brake
        if limiter and spec and r["sm_clock_avg"] and (r["gpu_util_avg"] or 0) >= 70 and r["sm_clock_avg"] < 0.85 * spec:
            capped.append(f"{st} ({fmt(r['sm_clock_avg'], 0)} MHz, {r['sm_clock_avg'] / spec:.0%} of {spec:.0f}, "
                          f"event bits {int(r['clock_reasons_max'] or 0):#x})")
        if (r["gpu_temp_max"] or 0) >= 85:
            hot.append(f"{st} ({fmt(r['gpu_temp_max'], 0)} C)")
        if r["mem_avail_min_gib"] is not None and r["mem_avail_min_gib"] < 8:
            lowmem.append(f"{st} ({fmt(r['mem_avail_min_gib'], 1)} GiB)")
    if queued:
        # vLLM's queue-time histogram tops out near 2 min, so it cannot apportion very long TTFTs.
        # Use prompt length instead: with long prompts, prefill after admission is a big share of TTFT.
        long_prompt = [r["stage"] for r in rows if (r["waiting_avg"] or 0) >= 0.5 and r["req_s"] and r["prompt_tok_s"]
                       and r["prompt_tok_s"] / r["req_s"] > 4096]
        tail = "Time to first token there is mostly queue wait." if not long_prompt else (
            "Time to first token there is queue wait plus a long prefill after admission ("
            + ", ".join(long_prompt) + " average over 4k prompt tokens per request, processed in "
            "max-num-batched-tokens chunks shared with other requests).")
        out.append("Requests queued in " + "; ".join(queued) + ". " + tail)
    if kvfull:
        out.append("KV cache near full in " + ", ".join(kvfull) +
                   ": lower max-model-len or concurrency, or raise gpu-memory-utilization.")
    if busy:
        out.append("GPU 'busy' >= 90% in " + ", ".join(busy) + ". This counter is the share of time any kernel is "
                   "running, not how much of the GPU is used, so LLM decode reads near 100% even at concurrency 1; "
                   "use power (see the table) against a PyTorch burn run to judge how close the GPU is to its limit.")
    if idle_wait:
        out.append("Requests waited while the GPU was under 50% busy in " + ", ".join(idle_wait) +
                   ": the limit is the scheduler (max-num-seqs / max-num-batched-tokens), not the GPU.")
    if capped:
        out.append("SM clock averaged well under its maximum while busy in " + "; ".join(capped) +
                   ": power or thermal capping.")
    if hot:
        out.append("GPU temperature reached " + ", ".join(hot) + ".")
    if lowmem:
        out.append("Unified memory headroom on the vLLM node fell to " + ", ".join(lowmem) + ".")
    errs, gaps = [], []
    for c in client or []:
        if c.get("error_rate", 0) > 0.01:
            errs.append(f"{c['stage']} {c['error_rate']:.1%} ({c['errors']} of {c['requests']})")
        srv = next((r for r in rows if r["stage"] == c["stage"]), None)
        if srv and srv["gen_tok_s"] and c.get("out_tok_per_s") and \
                abs(c["out_tok_per_s"] - srv["gen_tok_s"]) / srv["gen_tok_s"] > 0.15:
            gaps.append(f"{c['stage']} {c['out_tok_per_s']:.0f} vs {srv['gen_tok_s']:.0f}")
    if errs:
        out.append("Client errors: " + ", ".join(errs) + "; see the status column in requests.csv.")
    if gaps:
        out.append("Client and server token rates differ by more than 15% (client vs server tok/s): " + ", ".join(gaps) +
                   ". The client counts a request's tokens in the stage that sent it (even if it finished later); the server "
                   "counts tokens generated inside the stage window, so trust the server figure at high concurrency.")
    skew = [r for r in rows if r.get("busiest_replica_share") is not None and r["busiest_replica_share"] > 0.7
            and (r["running_avg"] or 0) >= 2]
    if skew:
        out.append("Load is unbalanced across vLLM replicas (average running requests per node): " + "; ".join(
            f'{r["stage"]} {r["running_by_node"]} ({r["busiest_replica_share"]:.0%} on the busiest)' for r in skew) +
            ". The Kubernetes Service picks a pod per TCP connection, not per request, and the load generator reuses "
            "connections, so most requests can land on one replica; numbers for these stages show that replica, not "
            "the pair. Re-run with LOADGEN_NO_KEEPALIVE=1 (06-load.sh) for per-request balancing.")
    hits = [(r["stage"], r["prefix_hit"]) for r in rows if r["prefix_hit"] is not None]
    if hits and max(h for _, h in hits) - min(h for _, h in hits) > 0.2:
        out.append("Prefix cache hit ratio by stage: " + ", ".join(f"{s} {h:.0%}" for s, h in hits) + ".")
    if rows and all((r["dcgm_series"] or 0) == 0 for r in rows):
        out.append("DCGM returned no GPU utilization/power series on the vLLM node; GPU numbers come from the "
                   "nvidia-smi exporter (expected on GB10).")
    return out


# ----------------------------------------------------------------------------------------
SERIES = [
    ("load_target", "max(loadgen_stage_concurrency)"),
    ("client_inflight", "sum(loadgen_inflight)"),
    ("running", "sum(vllm:num_requests_running)"),
    ("waiting", "sum(vllm:num_requests_waiting)"),
    ("gen_tok_s", "sum(spark:vllm_generation_tokens:rate30s)"),
    ("prompt_tok_s", "sum(spark:vllm_prompt_tokens:rate30s)"),
    ("ttft_p95_s", "max(spark:vllm_ttft_seconds:p95_1m)"),
    ("itl_p95_s", "max(spark:vllm_itl_seconds:p95_1m)"),
    ("kv_usage", "max(vllm:kv_cache_usage_perc)"),
    ("gpu_util_pct", "avg(spark:gpu_utilization:percent{SEL})"),
    ("gpu_power_w", "avg(spark:gpu_power:watts{SEL})"),
    ("gpu_temp_c", "max(spark:gpu_temperature:celsius{SEL})"),
    ("sm_clock_mhz", "avg(spark:gpu_sm_clock:mhz{SEL})"),
    ("mem_avail_gib", "min(spark:node_memory_available:bytes{SEL}) / 2^30"),
]


def timeseries(p, start, end, nodes_re):
    sel = f'{{node=~"{nodes_re}"}}' if nodes_re else ""
    cols = {}
    for name, expr in SERIES:
        expr = expr.replace("{SEL}", sel)
        try:
            res = p.series(expr, start, end)
        except Exception:  # noqa: BLE001 — a missing series must not stop the report
            res = []
        cols[name] = dict(res[0][1]) if res else {}
    ts = sorted({t for c in cols.values() for t in c})
    return ts, cols


def charts(outdir, stages, ts, cols, rows, key):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return ["matplotlib not installed: charts skipped (pip install --user matplotlib, then rerun)"]
    made = []
    t0 = stages[0]["start"]
    x = [(t - t0) / 60 for t in ts]

    def col(name):
        return [cols[name].get(t) for t in ts]

    panels = [
        ("Load", [("client_inflight", "client in flight"), ("running", "vLLM running"), ("waiting", "vLLM waiting"),
                  ("load_target", "target concurrency")], "requests"),
        ("Throughput", [("gen_tok_s", "generated tok/s"), ("prompt_tok_s", "prompt tok/s")], "tokens/s"),
        ("Latency p95", [("ttft_p95_s", "TTFT p95"), ("itl_p95_s", "ITL p95")], "seconds"),
        ("GPU", [("gpu_util_pct", "util %"), ("gpu_temp_c", "temp C")], "% / C"),
        ("GPU power and clock", [("gpu_power_w", "power W"), ("sm_clock_mhz", "SM clock MHz / 10")], "W, MHz/10"),
        ("Unified memory", [("mem_avail_gib", "available GiB")], "GiB"),
    ]
    fig, axes = plt.subplots(len(panels), 1, figsize=(13, 2.3 * len(panels)), sharex=True)
    for ax, (title, lines, ylabel) in zip(axes, panels):
        for name, label in lines:
            y = col(name)
            if name == "sm_clock_mhz":
                y = [v / 10 if v is not None else None for v in y]
            if any(v is not None for v in y):
                ax.plot(x, [v if v is not None else float("nan") for v in y], label=label, linewidth=1.4)
        for i, st in enumerate(stages):
            a, b = (st["start"] - t0) / 60, (st["end"] - t0) / 60
            if i % 2 == 0:
                ax.axvspan(a, b, color="0.92", zorder=0)
        ax.set_title(title, loc="left", fontsize=10)
        ax.set_ylabel(ylabel, fontsize=8)
        ax.grid(alpha=0.3)
        ax.legend(fontsize=7, loc="upper left", bbox_to_anchor=(1.005, 1.0), frameon=False)
    for st in stages:
        axes[0].text(((st["start"] + st["end"]) / 2 - t0) / 60, 1.02, st["name"], fontsize=7, ha="center",
                     va="bottom", transform=axes[0].get_xaxis_transform())
    axes[-1].set_xlabel("minutes since run start (shaded bands = alternate stages)")
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, "timeline.png"), dpi=110)
    plt.close(fig)
    made.append("timeline.png")

    best = {}
    for r in rows:
        if r.get(key) and r["gen_tok_s"] and (r[key] not in best or r["gen_tok_s"] > best[r[key]]["gen_tok_s"]):
            best[r[key]] = r
    curve = [best[k] for k in sorted(best)]
    if len(curve) >= 3:
        fig, ax1 = plt.subplots(figsize=(8, 4.5))
        xs = [r[key] for r in curve]
        ax1.plot(xs, [r["gen_tok_s"] for r in curve], marker="o", label="generated tok/s (all users)")
        ax1.set_xlabel(key.replace("_", " "))
        ax1.set_ylabel("tokens/s, all users")
        ax1.set_xscale("log", base=2)
        ax2 = ax1.twinx()
        ax2.plot(xs, [r["user_tok_s_p50"] for r in curve], marker="s", color="tab:orange", label="per-user tok/s (1/ITL p50)")
        ax2.plot(xs, [r["ttft_p95"] for r in curve], marker="^", color="tab:red", label="TTFT p95 (s)")
        ax2.set_ylabel("per-user tok/s, TTFT s")
        lines = ax1.get_legend_handles_labels()[0] + ax2.get_legend_handles_labels()[0]
        labels = ax1.get_legend_handles_labels()[1] + ax2.get_legend_handles_labels()[1]
        ax1.legend(lines, labels, fontsize=8, loc="center left", frameon=False)
        ax1.grid(alpha=0.3)
        ax1.set_title("Throughput vs per-user speed as load rises", loc="left", fontsize=10)
        fig.tight_layout()
        fig.savefig(os.path.join(outdir, "scaling.png"), dpi=110)
        plt.close(fig)
        made.append("scaling.png")
    return made


# ----------------------------------------------------------------------------------------
STAGE_COLS = [
    ("stage", "stage", None), ("load", "load", None), ("gen_tok_s", "gen tok/s", 0), ("req_s", "req/s", 2),
    ("ttft_p50", "TTFT p50 s", 2), ("ttft_p95", "TTFT p95 s", 2), ("itl_p50", "ITL p50 s", 3),
    ("user_tok_s_p50", "user tok/s", 1), ("queue_p95", "queue p95 s", 2), ("running_max", "run max", 0),
    ("waiting_max", "wait max", 0), ("kv_max", "KV max %", 0),
]
GPU_COLS = [
    ("stage", "stage", None), ("gpu_util_avg", "GPU util %", 0), ("gpu_power_avg", "power W", 1),
    ("gpu_power_max", "power max W", 1), ("gpu_temp_max", "temp max C", 0), ("sm_clock_avg", "SM MHz avg", 0),
    ("sm_clock_min", "SM MHz min", 0), ("mem_avail_min_gib", "mem avail min GiB", 1), ("cpu_busy_avg", "CPU busy %", 0),
]


def md_table(rows, cols):
    head = "| " + " | ".join(c[1] for c in cols) + " |"
    sep = "| " + " | ".join("---" for _ in cols) + " |"
    lines = [head, sep]
    for r in rows:
        cells = []
        for k, _label, nd in cols:
            v = r.get(k)
            if k == "load":
                v = f"c={r['concurrency']}" if r.get("concurrency") else (f"{r['rate_rps']} rps" if r.get("rate_rps") else "")
                cells.append(v)
            elif nd is None:
                cells.append(str(v if v is not None else ""))
            elif k in ("kv_max", "cpu_busy_avg"):
                cells.append(fmt(v, nd, 100))
            else:
                cells.append(fmt(v, nd))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def compare(rows, other_csv):
    with open(other_csv) as fh:
        other = {r["stage"]: r for r in csv.DictReader(fh)}
    keys = [("gen_tok_s", "gen tok/s", 0), ("ttft_p95", "TTFT p95 s", 2), ("itl_p50", "ITL p50 s", 3),
            ("e2e_p95", "E2E p95 s", 2), ("gpu_util_avg", "GPU util %", 0), ("gpu_power_avg", "power W", 1)]
    lines = ["| stage | " + " | ".join(f"{k[1]} (this / other / Δ)" for k in keys) + " |",
             "| --- | " + " | ".join("---" for _ in keys) + " |"]
    for r in rows:
        o = other.get(r["stage"])
        if not o:
            continue
        cells = []
        for k, _l, nd in keys:
            a, b = r.get(k), _f(o.get(k))
            delta = f"{(a / b - 1):+.0%}" if a is not None and b else "–"
            cells.append(f"{fmt(a, nd)} / {fmt(b, nd)} / {delta}")
        lines.append(f"| {r['stage']} | " + " | ".join(cells) + " |")
    return "\n".join(lines) if len(lines) > 2 else "No stages with matching names in the other run."


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("run")
    ap.add_argument("--prom", default=os.environ.get("PROM_URL", "http://127.0.0.1:9090"))
    ap.add_argument("--trim", type=float, default=0.15, help="fraction of each stage skipped at its start")
    ap.add_argument("--compare", help="another analyzed run directory to compare stage by stage")
    args = ap.parse_args()

    with open(os.path.join(args.run, "stages.json")) as fh:
        meta = json.load(fh)
    stages = meta["stages"]
    if not stages:
        sys.exit("stages.json has no stages")
    client = None
    if os.path.exists(os.path.join(args.run, "summary.json")):
        with open(os.path.join(args.run, "summary.json")) as fh:
            client = json.load(fh)
    outdir = os.path.join(args.run, "analysis")
    os.makedirs(outdir, exist_ok=True)
    p = Prom(args.prom)

    t_first, t_last = stages[0]["start"], stages[-1]["end"]
    nodes = sorted({m.get("node", "") for m, _ in p.vector(
        f"max_over_time(sum by (node) (vllm:num_requests_running)[{int(t_last - t_first) + 60}s:{STEP}s])", t_last)} - {""})
    nodes_re = "|".join(nodes)
    print(f"run {args.run}: {len(stages)} stages, vLLM on {', '.join(nodes) or 'unknown node'}", flush=True)

    rows = []
    for st in stages:
        rows.append(stage_metrics(p, st, args.trim, nodes_re))
        r = rows[-1]
        print(f"  {r['stage']:<16} gen {fmt(r['gen_tok_s'], 0):>6} tok/s  TTFT p95 {fmt(r['ttft_p95']):>6} s  "
              f"ITL p50 {fmt(r['itl_p50'], 3):>6} s  GPU {fmt(r['gpu_util_avg'], 0):>3}%  "
              f"{fmt(r['gpu_power_avg'], 0):>4} W", flush=True)

    with open(os.path.join(outdir, "stages.csv"), "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    ts, cols = timeseries(p, t_first - 30, t_last + 30, nodes_re)
    with open(os.path.join(outdir, "timeseries.csv"), "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["time"] + [n for n, _ in SERIES])
        for t in ts:
            w.writerow([f"{t:.0f}"] + ["" if cols[n].get(t) is None else f"{cols[n][t]:.4g}" for n, _ in SERIES])

    key = "concurrency" if any(s.get("concurrency") for s in stages) else "rate_rps"
    made = charts(outdir, stages, ts, cols, rows, key)
    notes = findings(rows, client)

    rep = [f"# Load analysis — {os.path.basename(os.path.normpath(args.run))}", "",
           f"Profile `{meta.get('profile', '?')}`, model `{meta.get('model', '?')}`, vLLM on "
           f"{', '.join(nodes) or 'unknown'}. Each stage is measured from {args.trim:.0%} into the stage "
           f"(at least 10 s) to its end, so the ramp between stages is excluded.", "", "## Findings", ""]
    rep += [f"- {n}" for n in notes] or ["- No saturation signals in this run."]
    rep += ["", "## Server (vLLM, from Prometheus)", "", md_table(rows, STAGE_COLS), "",
            "## GPU and node (vLLM node; DCGM where present, else nvidia-smi)", "", md_table(rows, GPU_COLS), ""]
    if client:
        rep += ["## Client (load generator)", "",
                "| stage | ok | errors | req/s | out tok/s | TTFT p50 s | TTFT p95 s | TPOT p50 s | E2E p95 s |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |"]
        for c in client:
            rep.append(f"| {c['stage']} | {c['ok']} | {c['errors']} | {fmt(c['req_per_s'])} | {fmt(c['out_tok_per_s'], 1)} | "
                       f"{fmt(c['ttft_p50_s'])} | {fmt(c['ttft_p95_s'])} | {fmt(c['tpot_p50_s'], 4)} | {fmt(c['e2e_p95_s'])} |")
        rep.append("")
    if args.compare:
        other = os.path.join(args.compare, "analysis", "stages.csv")
        rep += [f"## Compared with {os.path.basename(os.path.normpath(args.compare))}", "",
                compare(rows, other) if os.path.exists(other) else f"{other} not found — analyze that run first.", ""]
    rep += ["## Files", ""] + [f"- `{m}`" if m.endswith(".png") else f"- {m}" for m in made] + \
           ["- `stages.csv` — every metric per stage", "- `timeseries.csv` — 5 s samples for the whole run"]
    with open(os.path.join(outdir, "report.md"), "w") as fh:
        fh.write("\n".join(rep) + "\n")

    print("\nFindings:")
    for n in notes:
        print(f"  - {n}")
    print(f"\nANALYSIS_DONE {outdir}/report.md", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
