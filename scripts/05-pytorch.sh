#!/usr/bin/env bash
# 05-pytorch.sh — PyTorch workloads on the cluster, observed through DCGM / nvidia-smi.
# usage: 05-pytorch.sh burn [node] [seconds] [duty] [alloc_gb]   GPU burn, then GPU telemetry summary
#        05-pytorch.sh burn-bg [node] [seconds] [duty] [alloc_gb] start a burn and return (noisy neighbour)
#        05-pytorch.sh allreduce                                  two-node all_reduce over RoCE
source "$(dirname "$0")/lib.sh"
need kubectl jq python3
P="$LAB_ROOT/k8s/pytorch"
ensure_ns "$NS_LAB"
tools_configmap "$NS_LAB"

gpu_summary() {
  # gpu_summary <node> <window-seconds>: what each telemetry source saw during the window
  local node="$1" w="$2"
  printf '\n%-34s %10s %10s %10s\n' "GPU telemetry on $node, last ${w}s" "avg" "min" "max"
  local sel="{node=\"$node\"}" row label expr avg min max
  for row in \
    "util % (recorded)|spark:gpu_utilization:percent${sel}" \
    "util % (DCGM_FI_DEV_GPU_UTIL)|DCGM_FI_DEV_GPU_UTIL${sel}" \
    "util % (nvidia-smi)|spark_nvsmi_gpu_utilization_percent${sel}" \
    "power W (recorded)|spark:gpu_power:watts${sel}" \
    "temperature C (recorded)|spark:gpu_temperature:celsius${sel}" \
    "SM clock MHz (recorded)|spark:gpu_sm_clock:mhz${sel}" \
    "unified mem available GiB|spark:node_memory_available:bytes${sel} / 2^30"; do
    label="${row%%|*}"; expr="max(${row#*|})"
    avg="$(prom_value "avg_over_time((${expr})[${w}s:5s])")"
    min="$(prom_value "min_over_time((${expr})[${w}s:5s])")"
    max="$(prom_value "max_over_time((${expr})[${w}s:5s])")"
    if [[ "$avg" == 0 && "$max" == 0 && "$label" == *DCGM* ]]; then
      printf '%-34s %10s %10s %10s\n' "$label" "-" "-" "-"
    else
      printf '%-34s %10.1f %10.1f %10.1f\n' "$label" "${avg:-0}" "${min:-0}" "${max:-0}"
    fi
  done
}

burn() {
  local node="${1:-$NODE1_NAME}" secs="${2:-180}" duty="${3:-1.0}" alloc="${4:-0}" wait="${5:-yes}"
  kubectl -n "$NS_LAB" delete job "torch-burn-$node" --ignore-not-found --wait >/dev/null
  TARGET_NODE="$node" BURN_SECONDS="$secs" BURN_DUTY="$duty" BURN_ALLOC_GB="$alloc" \
    render "$P/torch-burn-job.yaml" | kubectl apply -f - >/dev/null
  info "torch-burn on $node: ${secs}s at duty ${duty}, holding ${alloc} GiB extra"
  [[ "$wait" == yes ]] || { pass "burn started in the background (kubectl -n $NS_LAB logs -f job/torch-burn-$node)"; return 0; }
  wait_for "burn pod to start" 1200 bash -c \
    "kubectl -n ${NS_LAB} get pod -l job-name=torch-burn-$node -o jsonpath='{.items[0].status.phase}' | grep -Eq 'Running|Succeeded|Failed'" \
    || { fail "burn pod did not start"; return 1; }
  kubectl -n "$NS_LAB" logs -f "job/torch-burn-$node" || true
  kubectl -n "$NS_LAB" wait --for=condition=complete "job/torch-burn-$node" --timeout=120s >/dev/null 2>&1 || true
  local done_line; done_line="$(kubectl -n "$NS_LAB" logs "job/torch-burn-$node" | grep BURN_DONE || true)"
  [[ -n "$done_line" ]] && pass "$done_line" || fail "burn did not finish cleanly"
  sleep 10
  gpu_summary "$node" "$secs"
}

allreduce() {
  [[ -f "$HOME/.config/dgx-spark-roce/nccl.env" ]] || warn "nccl.env not found — values come from lab.env"
  kubectl -n "$NS_LAB" delete pod -l app=torch-allreduce --ignore-not-found --wait >/dev/null
  render "$P/torch-allreduce.yaml" | kubectl apply -f - >/dev/null
  local t0; t0="$(date +%s)"
  wait_for "all_reduce pods to finish" 1800 bash -c \
    "[[ \$(kubectl -n ${NS_LAB} get pods -l app=torch-allreduce -o jsonpath='{range .items[*]}{.status.phase}{\"\\n\"}{end}' | grep -Ec 'Succeeded|Failed') -ge 2 ]]" \
    || warn "pods still running"
  local log0 log1; log0="$(kubectl -n "$NS_LAB" logs torch-ar-0 2>&1)"; log1="$(kubectl -n "$NS_LAB" logs torch-ar-1 2>&1)"
  grep -E '^ *[0-9]+MB|correctness|memlock' <<<"$log0" || true
  grep -q 'ALLREDUCE_OK' <<<"$log0" && pass "$(grep ALLREDUCE_OK <<<"$log0")" || fail "all_reduce did not complete: $(tail -n 5 <<<"$log0" | tr '\n' ' ')"
  if grep -Eq 'NET/Socket|Using network Socket' <<<"$log0$log1"; then
    fail "NCCL fell back to TCP sockets — RDMA devices not visible or NCCL_IB_HCA wrong"
  else
    local ib; ib="$(grep -m1 -o 'NET/IB : Using .*' <<<"$log0" || true)"
    [[ -n "$ib" ]] && pass "NCCL transport: $ib" || warn "no 'NET/IB : Using' line found (NCCL_DEBUG=INFO set?)"
    local dev; for dev in ${NCCL_IB_HCA//,/ }; do
      grep -q "$dev" <<<"$ib" || warn "rail device $dev missing from NCCL's NET/IB line (half bandwidth)"
    done
  fi
  local w=$(( $(date +%s) - t0 + 30 ))
  printf '\nRDMA throughput seen by node-exporter (peak over %ss):\n' "$w"
  prom_query "max_over_time((spark:rdma_tx:bytes_per_second * 8 / 1e9)[${w}s:5s])" \
    | jq -r '.data.result[] | "  \(.metric.node) \(.metric.device): \(.value[1] | tonumber | . * 10 | floor / 10) Gb/s tx"'
  kubectl -n "$NS_LAB" delete pod -l app=torch-allreduce --ignore-not-found >/dev/null
}

case "${1:-}" in
  burn)      burn "${2:-$NODE1_NAME}" "${3:-180}" "${4:-1.0}" "${5:-0}" yes ;;
  burn-bg)   burn "${2:-$NODE1_NAME}" "${3:-600}" "${4:-1.0}" "${5:-0}" no ;;
  allreduce) allreduce ;;
  *)         sed -n '2,5p' "$0"; exit 2 ;;
esac
summary "pytorch ${1}"
