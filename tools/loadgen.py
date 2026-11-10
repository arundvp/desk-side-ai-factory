#!/usr/bin/env python3
"""loadgen.py — staged load generator for an OpenAI-compatible chat endpoint (vLLM).

Runs a profile: an ordered list of stages, each holding a fixed concurrency (closed loop:
N workers, back to back) or a fixed arrival rate (open loop: Poisson arrivals) for a set
time. Every request streams, so the client measures time to first token (TTFT), time per
output token (TPOT) and end-to-end latency. While it runs it serves Prometheus metrics
(active stage, target concurrency, in-flight, counters) so Grafana can line the load up
with vLLM and GPU metrics.

Writes to --out:
  stages.json    stage names with start/end epoch seconds (input for analyze.py)
  requests.csv   one row per request
  summary.json   per-stage client-side statistics (also printed as a table)

Needs Python 3.10+ and aiohttp (both are in the vLLM image).

usage: loadgen.py --base-url http://vllm.llm.svc:8000 --model gpt-oss-120b --profile ramp --out DIR
       loadgen.py --list-profiles
"""
import argparse
import asyncio
import csv
import json
import math
import os
import random
import statistics
import sys
import time
import uuid

try:
    import aiohttp
except ImportError:  # --list-profiles works without it
    aiohttp = None

# ----------------------------------------------------------------------------------------
# Profiles. Prompt sizes are approximate token counts (common English words ≈ 1 token each
# in gpt-oss's o200k tokenizer); requests.csv records the exact prompt_tokens vLLM reports.
MIX_CHAT = [{"weight": 70, "tokens": 256}, {"weight": 25, "tokens": 1024}, {"weight": 5, "tokens": 4096}]

PROFILES = {
    "smoke": {
        "description": "2 short stages to check the pipeline end to end (1 min)",
        "stages": [
            {"name": "c1", "duration_s": 30, "concurrency": 1, "max_tokens": 128, "prompt_mix": MIX_CHAT},
            {"name": "c4", "duration_s": 30, "concurrency": 4, "max_tokens": 128, "prompt_mix": MIX_CHAT},
        ],
    },
    "ramp": {
        "description": "closed-loop concurrency ramp 1 -> 128 past max-num-seqs=64 (~18 min)",
        "stages": [
            {"name": "warmup-c1", "duration_s": 60, "concurrency": 1},
            {"name": "c4", "duration_s": 120, "concurrency": 4},
            {"name": "c16", "duration_s": 150, "concurrency": 16},
            {"name": "c32", "duration_s": 150, "concurrency": 32},
            {"name": "c64", "duration_s": 180, "concurrency": 64},
            {"name": "c96", "duration_s": 180, "concurrency": 96},
            {"name": "c128", "duration_s": 180, "concurrency": 128},
            {"name": "cooldown-c1", "duration_s": 60, "concurrency": 1},
        ],
        "defaults": {"max_tokens": 512, "prompt_mix": MIX_CHAT},
    },
    "open-loop": {
        "description": "Poisson arrivals at rising rates: latency vs offered load (~15 min)",
        "stages": [
            {"name": "r0.25", "duration_s": 180, "rate_rps": 0.25},
            {"name": "r0.5", "duration_s": 180, "rate_rps": 0.5},
            {"name": "r1", "duration_s": 180, "rate_rps": 1.0},
            {"name": "r2", "duration_s": 180, "rate_rps": 2.0},
            {"name": "r3", "duration_s": 180, "rate_rps": 3.0},
        ],
        "defaults": {"max_tokens": 384, "prompt_mix": MIX_CHAT, "max_inflight": 256},
    },
    "prefill": {
        "description": "long prompts, short answers: TTFT and prefill throughput (~8 min)",
        "stages": [
            {"name": "long8k-c4", "duration_s": 150, "concurrency": 4},
            {"name": "long8k-c16", "duration_s": 150, "concurrency": 16},
            {"name": "long8k-c32", "duration_s": 150, "concurrency": 32},
        ],
        "defaults": {"max_tokens": 64, "prompt_mix": [{"weight": 1, "tokens": 8192}]},
    },
    "kv-pressure": {
        "description": "long prompts and long answers at high concurrency: fills the KV cache (~10 min + up to 15 min drain)",
        "stages": [
            {"name": "kv-c16", "duration_s": 180, "concurrency": 16},
            {"name": "kv-c48", "duration_s": 240, "concurrency": 48},
            {"name": "kv-c64", "duration_s": 180, "concurrency": 64},
        ],
        "defaults": {"max_tokens": 1024, "prompt_mix": [{"weight": 1, "tokens": 16384}]},
        # ~17.4k tokens per request: only ~29 fit in a 505k-token KV cache, decode runs ~0.3 s/token
        # at this context, so queued requests take 10-15 min end to end. A 600 s client timeout
        # would turn that queueing into errors.
        "timeout_s": 1800,
    },
    "spike": {
        "description": "steady load, a 3x-over-capacity burst, recovery (~6 min)",
        "stages": [
            {"name": "steady-c8", "duration_s": 120, "concurrency": 8},
            {"name": "spike-c200", "duration_s": 90, "concurrency": 200},
            {"name": "recover-c8", "duration_s": 150, "concurrency": 8},
        ],
        "defaults": {"max_tokens": 384, "prompt_mix": MIX_CHAT},
    },
    "prefix": {
        "description": "same 2k-token system prompt on every request: prefix-cache effect (~6 min)",
        "stages": [
            {"name": "unique-c16", "duration_s": 180, "concurrency": 16, "shared_prefix_tokens": 0},
            # same total prompt size as the unique stage: 2048 shared + 256 unique tokens
            {"name": "shared2k-c16", "duration_s": 180, "concurrency": 16, "shared_prefix_tokens": 2048,
             "prompt_mix": [{"weight": 1, "tokens": 256}]},
        ],
        "defaults": {"max_tokens": 256, "prompt_mix": [{"weight": 1, "tokens": 2304}]},
    },
    "soak": {
        "description": "30 minutes at concurrency 32: stability, thermals, memory drift",
        "stages": [{"name": "soak-c32", "duration_s": 1800, "concurrency": 32}],
        "defaults": {"max_tokens": 512, "prompt_mix": MIX_CHAT},
    },
}

STAGE_DEFAULTS = {
    "max_tokens": 512,
    "prompt_mix": MIX_CHAT,
    "ignore_eos": False,
    "shared_prefix_tokens": 0,
    "reasoning_effort": "low",
    "temperature": 0.7,
    "max_inflight": 512,
}

WORDS = (
    "system service cluster node memory network latency request token model server queue cache "
    "storage power thermal clock budget schedule capacity rollout incident review metric alert "
    "dashboard replica region failover database index query shard batch stream buffer window "
    "policy owner action status signal trend baseline target error retry timeout limit quota "
    "planning design report analysis summary detail customer support release update version "
    "kernel driver firmware device module process thread worker job task pipeline stage step "
    "the a of and to in for on with by from at as is are was be this that these those it its"
).split()


def make_text(n_tokens, rng):
    return " ".join(rng.choice(WORDS) for _ in range(max(1, n_tokens)))


def pick_tokens(mix, rng):
    total = sum(m["weight"] for m in mix)
    r = rng.uniform(0, total)
    for m in mix:
        r -= m["weight"]
        if r <= 0:
            return m["tokens"]
    return mix[-1]["tokens"]


def pct(values, q):
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * q
    f, c = math.floor(k), math.ceil(k)
    return s[f] if f == c else s[f] + (s[c] - s[f]) * (k - f)


# ----------------------------------------------------------------------------------------
class State:
    def __init__(self):
        self.stage_index = -1
        self.stage_name = ""
        self.stage_concurrency = 0
        self.stage_rate = 0.0
        self.inflight = 0
        self.req_total = {}       # (stage, status) -> count
        self.out_tokens = 0
        self.prompt_tokens = 0
        self.ttft_sum = {}
        self.ttft_count = {}
        self.records = []
        self.dropped = {}

    def metrics_text(self):
        lines = [
            "# TYPE loadgen_stage_index gauge", f"loadgen_stage_index {self.stage_index}",
            "# TYPE loadgen_stage_concurrency gauge", f"loadgen_stage_concurrency {self.stage_concurrency}",
            "# TYPE loadgen_stage_rate_rps gauge", f"loadgen_stage_rate_rps {self.stage_rate}",
            "# TYPE loadgen_inflight gauge", f"loadgen_inflight {self.inflight}",
            "# TYPE loadgen_stage_active gauge",
        ]
        if self.stage_name:
            lines.append(f'loadgen_stage_active{{stage="{self.stage_name}"}} 1')
        lines.append("# TYPE loadgen_requests_total counter")
        for (stage, status), n in sorted(self.req_total.items()):
            lines.append(f'loadgen_requests_total{{stage="{stage}",status="{status}"}} {n}')
        lines += ["# TYPE loadgen_output_tokens_total counter", f"loadgen_output_tokens_total {self.out_tokens}",
                  "# TYPE loadgen_prompt_tokens_total counter", f"loadgen_prompt_tokens_total {self.prompt_tokens}",
                  "# TYPE loadgen_ttft_seconds summary"]
        for stage in sorted(self.ttft_count):
            lines.append(f'loadgen_ttft_seconds_sum{{stage="{stage}"}} {self.ttft_sum[stage]:.6f}')
            lines.append(f'loadgen_ttft_seconds_count{{stage="{stage}"}} {self.ttft_count[stage]}')
        lines.append("# TYPE loadgen_dropped_total counter")
        for stage, n in sorted(self.dropped.items()):
            lines.append(f'loadgen_dropped_total{{stage="{stage}"}} {n}')
        return "\n".join(lines) + "\n"


async def metrics_server(state, port):
    async def handle(reader, writer):
        try:
            await reader.readuntil(b"\r\n\r\n")
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            pass
        body = state.metrics_text().encode()
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\n"
                     + f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode() + body)
        try:
            await writer.drain()
        finally:
            writer.close()
    return await asyncio.start_server(handle, "0.0.0.0", port)


# ----------------------------------------------------------------------------------------
async def one_request(session, args, stage, state, rng, shared_prefix):
    n_prompt = pick_tokens(stage["prompt_mix"], rng)
    rid = uuid.uuid4().hex[:12]
    user = (f"Request {rid}. Read the operations notes below and write a detailed, structured "
            f"summary of at least 600 words with findings, risks and next steps.\n\n" + make_text(n_prompt, rng))
    messages = []
    if shared_prefix:
        messages.append({"role": "system", "content": shared_prefix})
    messages.append({"role": "user", "content": user})
    body = {
        "model": args.model,
        "messages": messages,
        "max_tokens": stage["max_tokens"],
        "temperature": stage["temperature"],
        "stream": True,
        "stream_options": {"include_usage": True},
    }
    if stage.get("reasoning_effort"):
        body["reasoning_effort"] = stage["reasoning_effort"]
    if stage.get("ignore_eos"):
        body["ignore_eos"] = True

    rec = {"stage": stage["name"], "request_id": rid, "start": time.time(), "target_prompt_tokens": n_prompt,
           "status": "ok", "ttft_s": "", "e2e_s": "", "tpot_s": "", "prompt_tokens": "", "completion_tokens": "",
           "chunks": 0, "finish_reason": "", "error": ""}
    t0 = time.perf_counter()
    first = None
    last = None
    chunks = 0
    usage = None
    finish = ""
    state.inflight += 1
    try:
        async with session.post(f"{args.base_url}/v1/chat/completions", json=body,
                                timeout=aiohttp.ClientTimeout(total=args.timeout)) as resp:
            if resp.status != 200:
                rec["status"] = f"http_{resp.status}"
                rec["error"] = (await resp.text())[:200].replace("\n", " ")
            else:
                async for raw in resp.content:
                    line = raw.decode("utf-8", "replace").strip()
                    if not line.startswith("data:"):
                        continue
                    data = line[5:].strip()
                    if data == "[DONE]":
                        break
                    try:
                        obj = json.loads(data)
                    except json.JSONDecodeError:
                        continue
                    if obj.get("usage"):
                        usage = obj["usage"]
                    for ch in obj.get("choices") or []:
                        d = ch.get("delta") or {}
                        if d.get("content") or d.get("reasoning_content") or d.get("reasoning") or d.get("tool_calls"):
                            now = time.perf_counter()
                            if first is None:
                                first = now
                            last = now
                            chunks += 1
                        if ch.get("finish_reason"):
                            finish = ch["finish_reason"]
                    if obj.get("error"):
                        rec["status"] = "stream_error"
                        rec["error"] = str(obj["error"])[:200]
    except asyncio.TimeoutError:
        rec["status"] = "timeout"
    except aiohttp.ClientError as e:
        rec["status"] = "client_error"
        rec["error"] = str(e)[:200]
    finally:
        state.inflight -= 1

    end = time.perf_counter()
    if rec["status"] == "ok" and first is None:
        rec["status"] = "no_tokens"
    rec["e2e_s"] = round(end - t0, 4)
    rec["chunks"] = chunks
    rec["finish_reason"] = finish
    if first is not None:
        rec["ttft_s"] = round(first - t0, 4)
    if usage:
        rec["prompt_tokens"] = usage.get("prompt_tokens", "")
        rec["completion_tokens"] = usage.get("completion_tokens", "")
        ct = usage.get("completion_tokens") or 0
        state.out_tokens += ct
        state.prompt_tokens += usage.get("prompt_tokens") or 0
        if first is not None and last is not None and ct > 1:
            rec["tpot_s"] = round((last - first) / (ct - 1), 5)
    key = (stage["name"], rec["status"])
    state.req_total[key] = state.req_total.get(key, 0) + 1
    if rec["ttft_s"] != "":
        state.ttft_sum[stage["name"]] = state.ttft_sum.get(stage["name"], 0.0) + rec["ttft_s"]
        state.ttft_count[stage["name"]] = state.ttft_count.get(stage["name"], 0) + 1
    state.records.append(rec)


async def run_stage(session, args, stage, idx, state, rng):
    state.stage_index = idx
    state.stage_name = stage["name"]
    state.stage_concurrency = stage.get("concurrency", 0)
    state.stage_rate = stage.get("rate_rps", 0.0)
    shared = make_text(stage["shared_prefix_tokens"], random.Random(42)) if stage["shared_prefix_tokens"] else ""
    start = time.time()
    deadline = time.perf_counter() + stage["duration_s"]
    tasks = set()

    if "concurrency" in stage:
        async def worker(wid):
            wrng = random.Random(rng.random() + wid)
            while time.perf_counter() < deadline:
                await one_request(session, args, stage, state, wrng, shared)
        tasks = {asyncio.create_task(worker(i)) for i in range(stage["concurrency"])}
        # workers stop issuing at the deadline; requests already in flight finish in the background
        await asyncio.sleep(max(0.0, deadline - time.perf_counter()))
    else:
        rate = stage["rate_rps"]
        while True:
            now = time.perf_counter()
            if now >= deadline:
                break
            await asyncio.sleep(min(rng.expovariate(rate), deadline - now))
            if time.perf_counter() >= deadline:
                break
            if state.inflight >= stage["max_inflight"]:
                state.dropped[stage["name"]] = state.dropped.get(stage["name"], 0) + 1
                continue
            tasks.add(asyncio.create_task(one_request(session, args, stage, state, random.Random(rng.random()), shared)))
    end = time.time()
    return {"index": idx, "name": stage["name"], "start": round(start, 3), "end": round(end, 3),
            "duration_s": stage["duration_s"], "concurrency": stage.get("concurrency"),
            "rate_rps": stage.get("rate_rps"), "max_tokens": stage["max_tokens"],
            "prompt_mix": stage["prompt_mix"], "ignore_eos": stage["ignore_eos"],
            "shared_prefix_tokens": stage["shared_prefix_tokens"]}, tasks


def summarize(records, stages):
    out = []
    for st in stages:
        rs = [r for r in records if r["stage"] == st["name"]]
        ok = [r for r in rs if r["status"] == "ok"]
        dur = st["end"] - st["start"]
        ttft = [r["ttft_s"] for r in ok if r["ttft_s"] != ""]
        e2e = [r["e2e_s"] for r in ok]
        tpot = [r["tpot_s"] for r in ok if r["tpot_s"] != ""]
        toks = sum(int(r["completion_tokens"] or 0) for r in ok)
        ptoks = [int(r["prompt_tokens"]) for r in ok if r["prompt_tokens"] != ""]
        out.append({
            "stage": st["name"], "requests": len(rs), "ok": len(ok), "errors": len(rs) - len(ok),
            "error_rate": round((len(rs) - len(ok)) / len(rs), 4) if rs else 0.0,
            "req_per_s": round(len(ok) / dur, 3) if dur > 0 else 0.0,
            "out_tok_per_s": round(toks / dur, 1) if dur > 0 else 0.0,
            "mean_prompt_tokens": round(statistics.mean(ptoks), 1) if ptoks else None,
            "ttft_p50_s": pct(ttft, 0.5), "ttft_p95_s": pct(ttft, 0.95), "ttft_p99_s": pct(ttft, 0.99),
            "tpot_p50_s": pct(tpot, 0.5), "tpot_p95_s": pct(tpot, 0.95),
            "e2e_p50_s": pct(e2e, 0.5), "e2e_p95_s": pct(e2e, 0.95),
            "per_user_tok_per_s_p50": round(1 / pct(tpot, 0.5), 1) if tpot and pct(tpot, 0.5) else None,
        })
    return out


def print_table(summary):
    def f(v, nd=2):
        return "-" if v is None else f"{v:.{nd}f}"
    hdr = f"{'stage':<16}{'ok':>6}{'err':>5}{'req/s':>8}{'out tok/s':>11}{'TTFT p50':>10}{'TTFT p95':>10}" \
          f"{'TPOT p50':>10}{'user tok/s':>11}{'E2E p95':>9}"
    print(hdr)
    print("-" * len(hdr))
    for s in summary:
        print(f"{s['stage']:<16}{s['ok']:>6}{s['errors']:>5}{f(s['req_per_s']):>8}{f(s['out_tok_per_s'], 1):>11}"
              f"{f(s['ttft_p50_s']):>10}{f(s['ttft_p95_s']):>10}{f(s['tpot_p50_s'], 4):>10}"
              f"{f(s['per_user_tok_per_s_p50'], 1):>11}{f(s['e2e_p95_s']):>9}", flush=True)


def resolve_profile(args):
    if args.profile_file:
        with open(args.profile_file) as fh:
            prof = json.load(fh)
    else:
        if args.profile not in PROFILES:
            sys.exit(f"unknown profile {args.profile}; choose from {', '.join(PROFILES)}")
        prof = PROFILES[args.profile]
    if prof.get("timeout_s"):
        args.timeout = max(args.timeout, float(prof["timeout_s"]))
    defaults = {**STAGE_DEFAULTS, **prof.get("defaults", {})}
    stages = []
    for s in prof["stages"]:
        st = {**defaults, **s}
        if args.time_scale != 1.0:
            st["duration_s"] = max(10, int(st["duration_s"] * args.time_scale))
        if ("concurrency" in st) == ("rate_rps" in st):
            sys.exit(f"stage {st.get('name')}: set exactly one of concurrency or rate_rps")
        stages.append(st)
    return stages


async def main_async(args):
    stages = resolve_profile(args)
    os.makedirs(args.out, exist_ok=True)
    state = State()
    srv = await metrics_server(state, args.metrics_port) if args.metrics_port else None
    rng = random.Random(args.seed)
    headers = {"Authorization": f"Bearer {args.api_key}"} if args.api_key else {}
    if args.no_keepalive:
        # a new TCP connection per request, so a Kubernetes Service (kube-proxy) picks a replica per request
        conn = aiohttp.TCPConnector(limit=0, force_close=True)
        print("loadgen: keep-alive off (one connection per request)", flush=True)
    else:
        conn = aiohttp.TCPConnector(limit=0, keepalive_timeout=60)
    total = sum(s["duration_s"] for s in stages)
    print(f"loadgen: {args.base_url} model={args.model} profile={args.profile_file or args.profile} "
          f"stages={len(stages)} ~{total / 60:.1f} min out={args.out}", flush=True)

    async with aiohttp.ClientSession(connector=conn, headers=headers) as session:
        # wait for the server
        for _ in range(120):
            try:
                async with session.get(f"{args.base_url}/health", timeout=aiohttp.ClientTimeout(total=5)) as r:
                    if r.status == 200:
                        break
            except aiohttp.ClientError:
                pass
            except asyncio.TimeoutError:
                pass
            await asyncio.sleep(5)
        else:
            sys.exit("server /health never returned 200")

        done = []
        pending = set()
        for i, st in enumerate(stages):
            label = f"{st['concurrency']} concurrent" if "concurrency" in st else f"{st['rate_rps']} req/s"
            print(f"[{time.strftime('%H:%M:%S')}] stage {i + 1}/{len(stages)} {st['name']}: {label}, "
                  f"{st['duration_s']}s, max_tokens={st['max_tokens']}", flush=True)
            info, tasks = await run_stage(session, args, st, i, state, rng)
            done.append(info)
            pending |= tasks
            with open(os.path.join(args.out, "stages.json"), "w") as fh:
                json.dump({"profile": args.profile_file or args.profile, "model": args.model,
                           "base_url": args.base_url, "stages": done}, fh, indent=2)
        state.stage_name = "drain"
        state.stage_concurrency = 0
        state.stage_rate = 0.0
        if pending:
            print(f"draining {state.inflight} in-flight requests (up to {args.timeout:.0f}s)", flush=True)
            await asyncio.wait(pending, timeout=args.timeout + 5)
            for t in pending:
                t.cancel()

    fields = ["stage", "request_id", "start", "status", "ttft_s", "tpot_s", "e2e_s", "prompt_tokens",
              "completion_tokens", "target_prompt_tokens", "chunks", "finish_reason", "error"]
    with open(os.path.join(args.out, "requests.csv"), "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in sorted(state.records, key=lambda r: r["start"]):
            w.writerow(r)
    summary = summarize(state.records, done)
    with open(os.path.join(args.out, "summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2)
    print()
    print_table(summary)
    errors = sum(s["errors"] for s in summary)
    print(f"\nLOADGEN_DONE out={args.out} requests={len(state.records)} errors={errors}", flush=True)
    if srv:
        state.stage_index = -1
        await asyncio.sleep(args.linger)
        srv.close()
    return 0


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--base-url", default=os.environ.get("LOADGEN_BASE_URL", "http://vllm.llm.svc.cluster.local:8000"))
    p.add_argument("--model", default=os.environ.get("LOADGEN_MODEL", "gpt-oss-120b"))
    p.add_argument("--profile", default=os.environ.get("LOADGEN_PROFILE", "ramp"))
    p.add_argument("--profile-file", default=os.environ.get("LOADGEN_PROFILE_FILE"))
    p.add_argument("--out", default=os.environ.get("LOADGEN_OUT", "./loadgen-out"))
    p.add_argument("--api-key", default=os.environ.get("VLLM_API_KEY", ""))
    p.add_argument("--timeout", type=float, default=float(os.environ.get("LOADGEN_TIMEOUT", "600")))
    p.add_argument("--metrics-port", type=int, default=int(os.environ.get("LOADGEN_METRICS_PORT", "9500")))
    p.add_argument("--time-scale", type=float, default=float(os.environ.get("LOADGEN_TIME_SCALE", "1.0")),
                   help="multiply every stage duration (0.25 = quick rehearsal)")
    p.add_argument("--linger", type=float, default=float(os.environ.get("LOADGEN_LINGER", "20")),
                   help="seconds to keep serving metrics after the run so the last scrape lands")
    p.add_argument("--no-keepalive", action="store_true",
                   default=os.environ.get("LOADGEN_NO_KEEPALIVE", "0") not in ("", "0", "false"),
                   help="open a new connection per request (per-request balancing behind a Kubernetes Service)")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--list-profiles", action="store_true")
    args = p.parse_args()
    if args.list_profiles:
        for name, prof in PROFILES.items():
            mins = sum(s["duration_s"] for s in prof["stages"]) / 60
            print(f"{name:<12} {mins:5.1f} min  {prof['description']}")
        return 0
    if aiohttp is None:
        sys.exit("aiohttp is required (pip install aiohttp, or run inside the vLLM image)")
    args.base_url = args.base_url.rstrip("/")
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    sys.exit(main())
