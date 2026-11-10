#!/usr/bin/env bash
# run_e2e.sh — offline end-to-end test of the lab's tools (no cluster, no GPU):
#   1. nvsmi_exporter.py against a fake nvidia-smi
#   2. loadgen.py (ramp profile, compressed) against a fake vLLM server, incl. its /metrics
#   3. analyze.py against a fake Prometheus that syntax-checks every query
# Needs python3 with aiohttp, promql-parser (and matplotlib for the charts).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
T="$(mktemp -d)"
pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT
ok() { printf 'ok    %s\n' "$*"; }
no() { printf 'FAIL  %s\n' "$*"; exit 1; }
wait_port() { for _ in $(seq 1 50); do curl -s -o /dev/null "http://127.0.0.1:$1/" && return 0; sleep 0.2; done; return 1; }

# 1 --- nvidia-smi exporter ---------------------------------------------------------
NVSMI_BIN="$ROOT/tests/fake-nvidia-smi" NVSMI_PORT=19401 NVSMI_INTERVAL=1 python3 "$ROOT/tools/nvsmi_exporter.py" >"$T/nvsmi.log" 2>&1 &
pids+=($!)
wait_port 19401 || no "nvsmi exporter did not listen"
sleep 2
curl -s http://127.0.0.1:19401/metrics > "$T/nvsmi.prom"
grep -q '^spark_nvsmi_up 1' "$T/nvsmi.prom" || no "spark_nvsmi_up != 1"
grep -q '^spark_nvsmi_gpu_utilization_percent{gpu="0",uuid="GPU-11111111-2222-3333-4444-555555555555",model="NVIDIA GB10"} 96' "$T/nvsmi.prom" || no "utilization metric"
grep -q '^spark_nvsmi_clocks_event_reasons_active{.*} 4' "$T/nvsmi.prom" || no "clock event reasons parsed from hex"
grep -q '^spark_nvsmi_pstate{.*} 0' "$T/nvsmi.prom" || no "pstate parsed"
grep -q 'spark_nvsmi_field_supported{field="memory.used",gpu="0"} 0' "$T/nvsmi.prom" || no "memory.used reported N/A"
grep -q '^spark_nvsmi_memory_used_mib' "$T/nvsmi.prom" && no "N/A field exported as a value"
grep -q '^spark_nvsmi_process_used_memory_mib{process="python3"} 95212' "$T/nvsmi.prom" || no "process memory summed by name"
python3 - "$T/nvsmi.prom" <<'EOF' || no "exposition format"
import sys
from prometheus_client.parser import text_string_to_metric_families
fams = list(text_string_to_metric_families(open(sys.argv[1]).read()))
assert len(fams) >= 10, len(fams)
EOF
ok "nvsmi_exporter: $(grep -c '^spark_nvsmi' "$T/nvsmi.prom") samples, N/A fields skipped, exposition parses"

# 2 --- load generator against a fake vLLM ---------------------------------------------
python3 "$ROOT/tests/fake_vllm.py" 18080 >"$T/fake_vllm.log" 2>&1 &
pids+=($!)
wait_port 18080 || no "fake vLLM did not listen"
python3 "$ROOT/tools/loadgen.py" --base-url http://127.0.0.1:18080 --model gpt-oss-120b \
  --profile ramp --time-scale 0.06 --out "$T/run" --metrics-port 19500 --linger 1 > "$T/loadgen.log" 2>&1 &
lg=$!
sleep 15
curl -s http://127.0.0.1:19500/metrics > "$T/loadgen.prom" || true
wait "$lg" || { cat "$T/loadgen.log"; no "loadgen exited non-zero"; }
grep -q 'LOADGEN_DONE' "$T/loadgen.log" || no "no LOADGEN_DONE line"
grep -q '^loadgen_stage_concurrency ' "$T/loadgen.prom" || no "loadgen metrics endpoint"
grep -q 'loadgen_requests_total{stage=' "$T/loadgen.prom" || no "loadgen request counters"
python3 - "$T/run" <<'EOF' || no "loadgen outputs"
import csv, json, sys, os
d = sys.argv[1]
st = json.load(open(os.path.join(d, "stages.json")))["stages"]
assert [s["name"] for s in st] == ["warmup-c1", "c4", "c16", "c32", "c64", "c96", "c128", "cooldown-c1"], st
rows = list(csv.DictReader(open(os.path.join(d, "requests.csv"))))
assert rows and all(r["status"] == "ok" for r in rows), {r["status"] for r in rows}
assert all(float(r["ttft_s"]) > 0 for r in rows)
summ = {s["stage"]: s for s in json.load(open(os.path.join(d, "summary.json")))}
assert summ["c128"]["ttft_p95_s"] > summ["c16"]["ttft_p95_s"], "queueing above 64 should raise TTFT"
assert summ["c64"]["out_tok_per_s"] > summ["c4"]["out_tok_per_s"]
print(f"{len(rows)} requests, c128 TTFT p95 {summ['c128']['ttft_p95_s']:.2f}s vs c16 {summ['c16']['ttft_p95_s']:.2f}s")
EOF
ok "loadgen: 8 stages, all requests ok, queueing visible above max-num-seqs, metrics served"

# 3 --- analysis against a fake Prometheus ---------------------------------------------
python3 "$ROOT/tests/fake_prom.py" 19090 "$T/run/stages.json" >"$T/fake_prom.log" 2>&1 &
pids+=($!)
wait_port 19090 || no "fake Prometheus did not listen"
python3 "$ROOT/tools/analyze.py" "$T/run" --prom http://127.0.0.1:19090 > "$T/analyze.log" 2>&1 \
  || { cat "$T/analyze.log"; no "analyze.py failed"; }
R="$T/run/analysis/report.md"
[[ -f "$R" ]] || no "no report.md"
grep -q '^## Findings' "$R" || no "report has no findings section"
grep -q 'Peak generation throughput' "$R" || no "peak throughput finding"
grep -q 'Knee between concurrency' "$R" || no "knee finding"
grep -q 'Requests queued in c96' "$R" && grep -q 'scheduler full' "$R" || no "queueing finding"
grep -q 'SM clock averaged well under' "$R" || no "clock-capping finding"
grep -q 'DCGM returned no GPU' "$R" || no "DCGM-absent finding"
grep -q '| c128 |' "$R" || no "stage row in table"
[[ -s "$T/run/analysis/stages.csv" && -s "$T/run/analysis/timeseries.csv" ]] || no "CSV outputs"
if python3 -c 'import matplotlib' 2>/dev/null; then
  [[ -s "$T/run/analysis/timeline.png" && -s "$T/run/analysis/scaling.png" ]] || no "charts"
fi
# compare mode: a run against itself has 0% deltas
cp -r "$T/run" "$T/run2"
python3 "$ROOT/tools/analyze.py" "$T/run2" --prom http://127.0.0.1:19090 --compare "$T/run" > /dev/null 2>&1 || no "compare mode"
grep -q '+0%' "$T/run2/analysis/report.md" || no "compare deltas"
ok "analyze: report, findings (peak, knee, queueing, clock capping, DCGM absent), CSVs, charts, compare"

mkdir -p "$ROOT/tests/artifacts"
{ echo "> SYNTHETIC — produced by tests/run_e2e.sh from a fake vLLM and a fake Prometheus. It shows the report format only; none of these numbers are measurements."; echo; cat "$R"; } > "$ROOT/tests/artifacts/sample-report.md"
for f in "$T"/run/analysis/*.png; do [[ -f "$f" ]] && cp "$f" "$ROOT/tests/artifacts/synthetic-$(basename "$f")"; done
echo "all offline tests passed (sample report: tests/artifacts/sample-report.md)"
