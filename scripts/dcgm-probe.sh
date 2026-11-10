#!/usr/bin/env bash
# dcgm-probe.sh — which DCGM fields does DCGM Exporter actually return on each GB10?
# Compares against the nvidia-smi exporter so you can see where the fallback is doing the work.
source "$(dirname "$0")/lib.sh"
need kubectl jq

prom() {
  local enc; enc="$(jq -rn --arg q "$1" '$q|@uri')"
  kubectl get --raw "/api/v1/namespaces/${NS_MON}/services/http:kps-prometheus:9090/proxy/api/v1/query?query=${enc}"
}

echo "== DCGM Exporter pods"
kubectl -n "$NS_GPU" get pods -l app=nvidia-dcgm-exporter -o wide 2>/dev/null || echo "(none)"
restarts="$(kubectl -n "$NS_GPU" get pods -l app=nvidia-dcgm-exporter -o jsonpath='{range .items[*]}{.status.containerStatuses[0].restartCount}{" "}{end}' 2>/dev/null)"

echo
echo "== DCGM fields per node (latest value)"
prom '{__name__=~"DCGM_FI_.*"}' | jq -r '
  .data.result
  | group_by(.metric.node // .metric.Hostname // "unknown")
  | .[]
  | "\(.[0].metric.node // .[0].metric.Hostname // "unknown"): \(length) series",
    (.[] | "   \(.metric.__name__) = \(.value[1])")' 2>/dev/null | sort -u | head -80
n_dcgm="$(prom 'count({__name__=~"DCGM_FI_.*"})' | jq -r '.data.result[0].value[1] // 0')"

echo
echo "== nvidia-smi exporter fields per node"
prom 'spark_nvsmi_field_supported' | jq -r '
  .data.result | group_by(.metric.node) | .[]
  | "\(.[0].metric.node): " + ([.[] | "\(.metric.field)=\(if .value[1]=="1" then "yes" else "N/A" end)"] | join("  "))' 2>/dev/null

echo
if [[ "$n_dcgm" == 0 ]]; then
  warn "DCGM Exporter returns no fields (restarts: ${restarts:-n/a}). Expected on GB10: dashboards use the nvidia-smi exporter."
else
  pass "DCGM Exporter returns ${n_dcgm} series; DCGM is preferred in spark:gpu_* recording rules where present"
fi
