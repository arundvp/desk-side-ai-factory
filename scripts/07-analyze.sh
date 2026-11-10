#!/usr/bin/env bash
# 07-analyze.sh — per-stage analysis of a load run against Prometheus; writes results/<run>/analysis/.
# usage: 07-analyze.sh [run-id] [--compare <other-run-id>]     (default run: the last one)
source "$(dirname "$0")/lib.sh"
need kubectl python3 curl

run="${1:-$(cat "$RESULTS_DIR/LAST_RUN" 2>/dev/null || true)}"
[[ -n "$run" && -d "$RESULTS_DIR/$run" ]] || die "no run directory '$run' under $RESULTS_DIR (scripts/pull-results.sh <run-id>)"
shift || true
extra=()
if [[ "${1:-}" == --compare ]]; then
  [[ -d "$RESULTS_DIR/${2:?run id to compare}" ]] || die "no run $2 under $RESULTS_DIR"
  extra=(--compare "$RESULTS_DIR/$2")
fi

python3 -c 'import matplotlib' 2>/dev/null || warn "matplotlib not installed — tables only (pip install --user matplotlib for charts)"
with_port_forward "$NS_MON" kps-prometheus 19090 9090 \
  python3 "$LAB_ROOT/tools/analyze.py" "$RESULTS_DIR/$run" --prom http://127.0.0.1:19090 "${extra[@]}" \
  && pass "report: $RESULTS_DIR/$run/analysis/report.md" \
  || fail "analysis failed"
summary "analyze $run"
