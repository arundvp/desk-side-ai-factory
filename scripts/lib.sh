#!/usr/bin/env bash
# lib.sh — shared helpers for the desk-side-ai-factory scripts. Sourced, not executed.
# shellcheck disable=SC2034  # variables here are used by the scripts that source this file

set -euo pipefail

LAB_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LAB_ENV="${LAB_ENV:-$LAB_ROOT/lab.env}"

if [[ ! -f "$LAB_ENV" ]]; then
  echo "missing $LAB_ENV — run: cp $LAB_ROOT/lab.env.example $LAB_ROOT/lab.env and edit it" >&2
  exit 2
fi
# export everything lab.env sets, so render() and child processes see it
set -a
# shellcheck source=/dev/null
source "$LAB_ENV"
set +a

export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"

NS_LLM="llm"
NS_LAB="lab"
NS_MON="monitoring"
NS_GPU="gpu-operator"
RESULTS_DIR="${RESULTS_DIR:-$LAB_ROOT/results}"

FAILS=0
WARNS=0
c_ok=$'\e[32m'; c_bad=$'\e[31m'; c_warn=$'\e[33m'; c_dim=$'\e[2m'; c_off=$'\e[0m'
if [[ ! -t 1 ]]; then c_ok=""; c_bad=""; c_warn=""; c_dim=""; c_off=""; fi

pass() { printf '%sPASS%s  %s\n' "$c_ok" "$c_off" "$*"; }
fail() { printf '%sFAIL%s  %s\n' "$c_bad" "$c_off" "$*"; FAILS=$((FAILS + 1)); }
warn() { printf '%sWARN%s  %s\n' "$c_warn" "$c_off" "$*"; WARNS=$((WARNS + 1)); }
info() { printf '%s....%s  %s\n' "$c_dim" "$c_off" "$*"; }
die()  { printf '%sFAIL%s  %s\n' "$c_bad" "$c_off" "$*" >&2; exit 1; }

# summary: print a one-line verdict and exit non-zero if any FAIL was recorded
summary() {
  local what="$1"
  if (( FAILS > 0 )); then
    printf '%s%s: %d FAIL, %d WARN%s\n' "$c_bad" "$what" "$FAILS" "$WARNS" "$c_off"
    exit 1
  fi
  printf '%s%s: OK (%d WARN)%s\n' "$c_ok" "$what" "$WARNS" "$c_off"
}

need() {
  local c
  for c in "$@"; do command -v "$c" >/dev/null 2>&1 || die "'$c' not found on PATH"; done
}

# on_node2 <command...>: run a command on Spark 2 over SSH (management LAN)
on_node2() {
  ssh -o BatchMode=yes -o ConnectTimeout=10 "${SSH_USER}@${NODE2_MGMT}" "$@"
}

# run a local script on Spark 2 with arguments: on_node2_script <script> [args...]
on_node2_script() {
  local script="$1"; shift
  local q=""
  local a
  for a in "$@"; do q+=" $(printf '%q' "$a")"; done
  ssh -o BatchMode=yes -o ConnectTimeout=10 "${SSH_USER}@${NODE2_MGMT}" "bash -s --${q}" < "$script"
}

ensure_ns() {
  local ns
  for ns in "$@"; do
    kubectl get ns "$ns" >/dev/null 2>&1 || kubectl create ns "$ns" >/dev/null
  done
}

# tools_configmap <namespace>: publish tools/*.py as ConfigMap "lab-tools"
tools_configmap() {
  local ns="$1"
  kubectl -n "$ns" create configmap lab-tools \
    --from-file="$LAB_ROOT/tools/" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
}

# render <file>: substitute ${VAR} placeholders in a manifest from the environment.
# Only the names in RENDER_VARS are replaced; anything else (shell variables inside
# container commands) is left alone. Set per-call values as prefixes: TARGET_NODE=x render f
RENDER_VARS="NODE1_NAME NODE2_NAME VLLM_IMAGE PYTORCH_IMAGE EXPORTER_IMAGE MODEL_ID SERVED_MODEL_NAME
LAB_DATA_DIR RAIL_A_NODE1_IP NCCL_IB_HCA NCCL_SOCKET_IFNAME NCCL_IB_GID_INDEX TARGET_NODE RUN_ID PROFILE
BURN_SECONDS BURN_ALLOC_GB BURN_DUTY TIME_SCALE SWEEP_CONCURRENCY VLLM_REPLICAS LOADGEN_NO_KEEPALIVE"
render() {
  RENDER_VARS="$RENDER_VARS" python3 - "$1" <<'PY'
import os, re, sys
names = set(os.environ["RENDER_VARS"].split())
text = open(sys.argv[1]).read()
missing = set()
def sub(m):
    name = m.group(1)
    if name not in names:
        return m.group(0)
    if name not in os.environ:
        missing.add(name)
        return m.group(0)
    return os.environ[name]
out = re.sub(r"\$\{([A-Z0-9_]+)\}", sub, text)
if missing:
    sys.exit(f"render {sys.argv[1]}: unset variables: {', '.join(sorted(missing))}")
sys.stdout.write(out)
PY
}

# wait_for <description> <timeout-seconds> <command...>: poll every 5 s until the command succeeds
wait_for() {
  local what="$1" timeout="$2"; shift 2
  local t=0
  printf '%s....%s  waiting for %s ' "$c_dim" "$c_off" "$what"
  until "$@" >/dev/null 2>&1; do
    if (( t >= timeout )); then printf '\n'; return 1; fi
    sleep 5; t=$((t + 5)); printf '.'
  done
  printf ' %ss\n' "$t"
}

run_id() { date -u +%Y%m%d-%H%M%S; }   # lowercase: used in Job names

# --- Prometheus through the API-server service proxy (no port-forward needed) --------
prom_get()   { kubectl get --raw "/api/v1/namespaces/${NS_MON}/services/http:kps-prometheus:9090/proxy$1"; }
prom_query() { local enc; enc="$(jq -rn --arg q "$1" '$q|@uri')"; prom_get "/api/v1/query?query=${enc}"; }
prom_value() { prom_query "$1" | jq -r '.data.result[0].value[1] // "0"'; }

# with_port_forward <ns> <svc> <local-port> <remote-port> <command...>
# starts kubectl port-forward, waits until the port answers, runs the command, stops the forward
with_port_forward() {
  local ns="$1" svc="$2" lport="$3" rport="$4"; shift 4
  kubectl -n "$ns" port-forward "svc/$svc" "${lport}:${rport}" >/dev/null 2>&1 &
  local pf=$! t=0 rc=0
  until curl -s -o /dev/null "http://127.0.0.1:${lport}/" 2>/dev/null; do
    sleep 1; t=$((t + 1))
    if (( t > 30 )); then kill "$pf" 2>/dev/null; return 1; fi
  done
  "$@" || rc=$?
  kill "$pf" 2>/dev/null; wait "$pf" 2>/dev/null || true
  return "$rc"
}
