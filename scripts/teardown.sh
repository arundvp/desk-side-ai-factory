#!/usr/bin/env bash
# teardown.sh — remove the lab in layers. Each layer can be removed on its own.
# usage: teardown.sh workloads   delete load, PyTorch and vLLM workloads (keeps model cache)
#        teardown.sh stack       + GPU Operator and monitoring (Helm releases, CRDs stay)
#        teardown.sh cluster     + uninstall k3s on both Sparks (asks first)
#        teardown.sh purge-data  delete ${LAB_DATA_DIR} (model cache, compile caches, results) on both
source "$(dirname "$0")/lib.sh"

workloads() {
  kubectl delete ns "$NS_LAB" --ignore-not-found --wait=false
  kubectl -n "$NS_LLM" delete deploy/vllm svc/vllm --ignore-not-found
  kubectl -n "$NS_LLM" delete job -l app=model-download --ignore-not-found
  pass "workloads removed (model cache kept in ${LAB_DATA_DIR} on each Spark)"
}
stack() {
  workloads
  kubectl -n "$NS_MON" delete ds/nvsmi-exporter svc/nvsmi-exporter --ignore-not-found
  helm -n "$NS_GPU" uninstall gpu-operator 2>/dev/null || true
  helm -n "$NS_MON" uninstall kps 2>/dev/null || true
  kubectl delete ns "$NS_GPU" "$NS_MON" "$NS_LLM" --ignore-not-found --wait=false
  pass "GPU Operator and monitoring removed (CRDs left in place; delete with kubectl delete crd ... if wanted)"
}
cluster() {
  read -r -p "Uninstall k3s on ${NODE1_NAME} and ${NODE2_NAME}? [y/N] " a
  [[ "$a" == [yY] ]] || { info "cancelled"; return 0; }
  ssh -t "${SSH_USER}@${NODE2_MGMT}" 'sudo /usr/local/bin/k3s-agent-uninstall.sh; sudo rm -f /etc/systemd/system/k3s-agent.service.d/10-spark-lab-memlock.conf' || true
  sudo /usr/local/bin/k3s-uninstall.sh || true
  sudo rm -f /etc/systemd/system/k3s.service.d/10-spark-lab-memlock.conf
  pass "k3s removed from both Sparks"
}
purge_data() {
  read -r -p "Delete ${LAB_DATA_DIR} (≈63 GB model cache per Spark) on both Sparks? [y/N] " a
  [[ "$a" == [yY] ]] || { info "cancelled"; return 0; }
  sudo rm -rf "${LAB_DATA_DIR}"
  ssh -t "${SSH_USER}@${NODE2_MGMT}" "sudo rm -rf '${LAB_DATA_DIR}'"
  pass "${LAB_DATA_DIR} removed on both Sparks"
}

case "${1:-}" in
  workloads)  workloads ;;
  stack)      stack ;;
  cluster)    stack; cluster ;;
  purge-data) purge_data ;;
  *)          sed -n '2,6p' "$0"; exit 2 ;;
esac
