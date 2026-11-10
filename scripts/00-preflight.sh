#!/usr/bin/env bash
# 00-preflight.sh — read-only checks on both Sparks before building the cluster.
# Run on Spark 1. Exit 0 = no FAIL.
source "$(dirname "$0")/lib.sh"

declare -A F1 F2
# shellcheck disable=SC2034  # dst is a nameref into F1/F2
load_facts() {
  local -n dst="$1"
  while IFS='=' read -r k v; do [[ -n "$k" ]] && dst["$k"]="$v"; done <<<"$2"
}

info "collecting facts on Spark 1 ($(hostname))"
load_facts F1 "$(bash "$LAB_ROOT/scripts/node-facts.sh" "$LAB_DATA_DIR")"

info "collecting facts on Spark 2 (${SSH_USER}@${NODE2_MGMT})"
if out2="$(on_node2_script "$LAB_ROOT/scripts/node-facts.sh" "$LAB_DATA_DIR" 2>&1)"; then
  load_facts F2 "$out2"
  pass "passwordless SSH to Spark 2"
else
  fail "cannot SSH to ${SSH_USER}@${NODE2_MGMT} without a password (run ./run.sh ssh from dgx-spark-roce): $out2"
  summary "preflight"
fi

check_node() {
  local -n f="$1"; local label="$2" want_name="$3" min_free="$4"
  [[ "${f[hostname]}" == "$want_name" ]] && pass "$label hostname ${f[hostname]}" \
    || fail "$label hostname is '${f[hostname]}', lab.env says '$want_name'"
  [[ "${f[arch]}" == "aarch64" ]] && pass "$label arch aarch64" || fail "$label arch ${f[arch]}"
  [[ "${f[gpu_name]}" == *GB10* ]] && pass "$label GPU ${f[gpu_name]}, driver ${f[driver]}" \
    || fail "$label GPU not visible to nvidia-smi ('${f[gpu_name]}')"
  [[ -n "${f[nvidia_container_runtime]}" ]] && pass "$label nvidia-container-runtime ${f[nvidia_container_runtime]} (toolkit ${f[nvidia_ctk]})" \
    || fail "$label nvidia-container-runtime missing (DGX OS ships it; reinstall nvidia-container-toolkit)"
  [[ -n "${f[nvidia_cdi_hook]}" ]] && pass "$label nvidia-cdi-hook present (CDI mode)" \
    || warn "$label nvidia-cdi-hook missing: install the GPU Operator with --set cdi.enabled=false (see Troubleshooting)"
  (( ${f[data_dir_free_gib]:-0} >= min_free )) && pass "$label ${f[data_dir_free_gib]} GiB free for $LAB_DATA_DIR (need $min_free)" \
    || fail "$label only ${f[data_dir_free_gib]:-0} GiB free for $LAB_DATA_DIR (need $min_free)"
  if [[ -z "${f[k3s_installed]}" ]]; then
    [[ "${f[port_6443_busy]}" == no && "${f[port_10250_busy]}" == no ]] && pass "$label ports 6443/10250 free" \
      || fail "$label port 6443 or 10250 already in use (another Kubernetes?)"
  else
    info "$label k3s already installed (${f[k3s_installed]})"
  fi
  [[ -z "${f[docker_gpu_containers]// /}" ]] && pass "$label no inference containers running in Docker" \
    || fail "$label inference containers still running in Docker: ${f[docker_gpu_containers]}— stop them (~/vllm/launch-vllm.sh stop); they hold unified memory"
  awk -v a="${f[mem_avail_gib]}" 'BEGIN{exit !(a >= 100)}' && pass "$label ${f[mem_avail_gib]} GiB memory available of ${f[mem_total_gib]}" \
    || warn "$label only ${f[mem_avail_gib]} GiB available — flush the page cache: sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'"
}

check_node F1 "Spark 1" "$NODE1_NAME" 150
check_node F2 "Spark 2" "$NODE2_NAME" 100

[[ "${F1[driver]}" == "${F2[driver]}" ]] && pass "same driver on both (${F1[driver]})" \
  || fail "driver differs: ${F1[driver]} vs ${F2[driver]} — update both Sparks"
[[ "${F1[dgx_release]}" == "${F2[dgx_release]}" ]] && pass "same DGX OS release on both (${F1[dgx_release]})" \
  || fail "DGX OS release differs: ${F1[dgx_release]} vs ${F2[dgx_release]}"

for t in curl jq python3 openssl; do
  [[ "${F1[tool_$t]}" == yes ]] && pass "Spark 1 has $t" || fail "Spark 1 lacks $t — sudo apt-get install -y curl jq python3 openssl"
done
[[ "${F1[tool_helm]}" == yes ]] && pass "Spark 1 has helm" \
  || warn "Spark 1 lacks helm — 01-k3s-install.sh installs it"
[[ "${F1[nccl_env]}" == yes ]] && pass "dgx-spark-roce nccl.env found (needed for the all_reduce job)" \
  || warn "$HOME/.config/dgx-spark-roce/nccl.env missing — run dgx-spark-roce ./run.sh verify before Step 5c"

summary "preflight"
