#!/usr/bin/env bash
# 06-load.sh — generate heavy inference load against vLLM from the load node.
# usage: 06-load.sh list                          show load-generator profiles
#        06-load.sh run <profile> [time-scale]    staged load (tools/loadgen.py); 0.25 = quick rehearsal
#        06-load.sh bench [concurrencies]         vllm bench serve sweep, e.g. "1 4 16 64"
#        06-load.sh noisy <profile> <seconds>     same as run, with a PyTorch burn on the vLLM node
# Results land in results/<run-id>/ on Spark 1; analyze with 07-analyze.sh <run-id>.
source "$(dirname "$0")/lib.sh"
need kubectl jq python3
L="$LAB_ROOT/k8s/load"
ensure_ns "$NS_LAB"
tools_configmap "$NS_LAB"

follow_job() {
  local job="$1" timeout_min="$2" t=0
  wait_for "pod for $job" 900 bash -c \
    "kubectl -n ${NS_LAB} get pod -l job-name=$job -o jsonpath='{.items[0].status.phase}' | grep -Eq 'Running|Succeeded|Failed'" \
    || { fail "$job pod did not start"; return 1; }
  kubectl -n "$NS_LAB" logs -f "job/$job" || true
  until kubectl -n "$NS_LAB" get job "$job" -o jsonpath='{.status.conditions[*].type}' | grep -Eq 'Complete|Failed'; do
    sleep 10; t=$((t + 1)); (( t > timeout_min * 6 )) && break
  done
  kubectl -n "$NS_LAB" get job "$job" -o jsonpath='{.status.conditions[*].type}' | grep -q Complete
}

check_server() {
  kubectl -n "$NS_LLM" get deploy vllm >/dev/null 2>&1 || die "vLLM is not deployed (scripts/04-vllm.sh)"
  local ready; ready="$(kubectl -n "$NS_LLM" get deploy vllm -o jsonpath='{.status.readyReplicas}')"
  (( ${ready:-0} >= 1 )) || die "vLLM has no ready replica"
  info "vLLM replicas ready: $ready ($(kubectl -n "$NS_LLM" get pods -l app=vllm -o jsonpath='{range .items[*]}{.spec.nodeName}{" "}{end}'))"
}

run_profile() {
  local profile="$1" scale="${2:-1.0}" rid; rid="$(run_id)-${profile}"
  check_server
  # LOADGEN_NO_KEEPALIVE=1: new connection per request, so the Service balances per request (default 0)
  export LOADGEN_NO_KEEPALIVE="${LOADGEN_NO_KEEPALIVE:-0}"
  RUN_ID="$rid" PROFILE="$profile" TIME_SCALE="$scale" TARGET_NODE="$NODE2_NAME" \
    render "$L/loadgen-job.yaml" | kubectl apply -f - >/dev/null
  info "run ${rid}: profile ${profile}, time scale ${scale}, keep-alive $([[ $LOADGEN_NO_KEEPALIVE == 1 ]] && echo off || echo on) (Grafana: 'Spark inference lab' dashboard)"
  if follow_job "loadgen-$rid" 240; then pass "load run finished"; else fail "load job failed"; fi
  bash "$LAB_ROOT/scripts/pull-results.sh" "$rid" "$NODE2_NAME" || true
  check_errors "$RESULTS_DIR/$rid"
  echo "$rid" > "$RESULTS_DIR/LAST_RUN"
  echo "next: scripts/07-analyze.sh $rid"
}

# check_errors <run dir>: a load run with failed requests is not a clean result (a broken replica
# behind the Service fails fast, so it shows up as errors, not as latency). Warn, and show why.
check_errors() {
  local dir="$1"
  [[ -f "$dir/requests.csv" ]] || return 0
  local out; out="$(python3 - "$dir/requests.csv" <<'PY'
import csv, collections, sys
rows = list(csv.DictReader(open(sys.argv[1])))
bad = [r for r in rows if r["status"] != "ok"]
print(f"{len(bad)} {len(rows)}")
for (s, e), n in collections.Counter((r["status"], r["error"][:110]) for r in bad).most_common(3):
    print(f"    {n:5d}  {s}  {e}")
PY
)" || return 0
  local n total; read -r n total <<<"$(head -n1 <<<"$out")"
  (( ${n:-0} == 0 )) && return 0
  if (( n * 100 > total )); then
    warn "${n} of ${total} requests failed — results are not valid until this is fixed. Top errors:"
  else
    info "${n} of ${total} requests failed (under 1%). Top errors:"
  fi
  tail -n +2 <<<"$out"
}

bench() {
  local conc="${1:-1 2 4 8 16 32 64 128}" rid; rid="$(run_id)-bench"
  check_server
  RUN_ID="$rid" SWEEP_CONCURRENCY="$conc" TARGET_NODE="$NODE2_NAME" \
    render "$L/vllm-bench-sweep-job.yaml" | kubectl apply -f - >/dev/null
  info "run ${rid}: vllm bench serve sweep over concurrency: ${conc}"
  if follow_job "bench-$rid" 360; then pass "sweep finished"; else fail "sweep job failed"; fi
  bash "$LAB_ROOT/scripts/pull-results.sh" "$rid" "$NODE2_NAME" || true
  echo "$rid" > "$RESULTS_DIR/LAST_RUN"
  if compgen -G "$RESULTS_DIR/$rid/c*.json" >/dev/null; then
    python3 - "$RESULTS_DIR/$rid" <<'EOF'
import glob, json, os, sys
rows = []
for f in glob.glob(os.path.join(sys.argv[1], "c*.json")):
    d = json.load(open(f))
    rows.append((d.get("max_concurrency") or 0, d))
print(f"\n{'conc':>5} {'done':>5} {'out tok/s':>10} {'req/s':>7} {'TTFT p50 ms':>12} {'TTFT p99 ms':>12} {'TPOT p50 ms':>12} {'user tok/s':>11}")
for c, d in sorted(rows, key=lambda r: r[0]):
    tpot = d.get("median_tpot_ms") or 0
    print(f"{c:>5} {d.get('completed', 0):>5} {d.get('output_throughput', 0):>10.1f} {d.get('request_throughput', 0):>7.2f} "
          f"{d.get('median_ttft_ms', 0):>12.0f} {d.get('p99_ttft_ms', 0):>12.0f} {tpot:>12.1f} {1000 / tpot if tpot else 0:>11.1f}")
EOF
  fi
  echo "next: scripts/07-analyze.sh $rid"
}

case "${1:-}" in
  list)  python3 "$LAB_ROOT/tools/loadgen.py" --list-profiles ;;
  run)   run_profile "${2:?profile (see: 06-load.sh list)}" "${3:-1.0}" ;;
  bench) bench "${2:-}" ;;
  noisy)
    secs="${3:?seconds the burn should last (about the profile length)}"
    bash "$LAB_ROOT/scripts/05-pytorch.sh" burn-bg "$NODE1_NAME" "$secs" 1.0 0
    run_profile "${2:?profile}" 1.0 ;;
  *)     sed -n '2,7p' "$0"; exit 2 ;;
esac
summary "load ${1}"
