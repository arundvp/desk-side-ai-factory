#!/usr/bin/env bash
# 01-k3s-install.sh — two-node k3s: server on Spark 1, agent on Spark 2, over the management LAN.
# Run on Spark 1. Asks for sudo on each Spark if passwordless sudo is not set up.
source "$(dirname "$0")/lib.sh"
need curl ssh scp

if command -v ufw >/dev/null && sudo ufw status 2>/dev/null | grep -q 'Status: active'; then
  warn "ufw is active on Spark 1 — allow 6443/tcp, 10250/tcp, 8472/udp between the Sparks before continuing"
fi

log1="$(mktemp)"; log2="$(mktemp)"
trap 'rm -f "$log1" "$log2"' EXIT

info "installing k3s ${K3S_VERSION} server on ${NODE1_NAME} (${NODE1_MGMT})"
bash "$LAB_ROOT/scripts/k3s-node-install.sh" server "$K3S_VERSION" "$NODE1_MGMT" "$MGMT_IF" "$NODE1_NAME" | tee "$log1"
grep -q 'nvidia_runtime=detected' "$log1" && pass "Spark 1 containerd has the nvidia runtime" \
  || fail "Spark 1: k3s did not detect nvidia-container-runtime (check: which nvidia-container-runtime)"
grep -q 'memlock=unlimited' "$log1" && pass "Spark 1 k3s memlock unlimited" \
  || warn "Spark 1 k3s memlock not unlimited ($(grep memlock= "$log1"))"

wait_for "API server" 180 kubectl get --raw /readyz || die "k3s API server did not come up — sudo journalctl -u k3s -n 100"
mkdir -p "$HOME/.kube"
cp /etc/rancher/k3s/k3s.yaml "$HOME/.kube/config" && chmod 600 "$HOME/.kube/config"

info "joining ${NODE2_NAME} (${NODE2_MGMT}) as agent"
tok_local="$(mktemp)"; chmod 600 "$tok_local"
# shellcheck disable=SC2024  # root reads the token, the redirect writes our own 600 temp file
sudo cat /var/lib/rancher/k3s/server/node-token > "$tok_local"
scp -q -o BatchMode=yes "$tok_local" "$LAB_ROOT/scripts/k3s-node-install.sh" "${SSH_USER}@${NODE2_MGMT}:/tmp/"
tok_remote="/tmp/$(basename "$tok_local")"
rm -f "$tok_local"
# -t gives the remote sudo a terminal for its password prompt; tee shows it and keeps a copy
ssh -t -o BatchMode=yes "${SSH_USER}@${NODE2_MGMT}" \
  "chmod 600 $tok_remote && bash /tmp/k3s-node-install.sh agent '$K3S_VERSION' '$NODE2_MGMT' '$MGMT_IF' '$NODE2_NAME' '$NODE1_MGMT' $tok_remote; rc=\$?; rm -f /tmp/k3s-node-install.sh $tok_remote; exit \$rc" \
  | tee "$log2"
grep -q 'nvidia_runtime=detected' "$log2" && pass "Spark 2 containerd has the nvidia runtime" \
  || fail "Spark 2: k3s-agent did not detect nvidia-container-runtime"
grep -q 'memlock=unlimited' "$log2" && pass "Spark 2 k3s-agent memlock unlimited" \
  || warn "Spark 2 k3s-agent memlock not unlimited"

wait_for "both nodes Ready" 300 bash -c \
  "[[ \$(kubectl get nodes --no-headers 2>/dev/null | awk '\$2==\"Ready\"' | wc -l) -ge 2 ]]" \
  || fail "nodes not Ready: $(kubectl get nodes --no-headers 2>&1 | tr '\n' ';')"

kubectl label node "$NODE1_NAME" spark-lab/role=inference --overwrite >/dev/null
kubectl label node "$NODE2_NAME" spark-lab/role=load --overwrite >/dev/null

if ! command -v helm >/dev/null; then
  info "installing helm"
  curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash >/dev/null
fi
command -v helm >/dev/null && pass "helm $(helm version --short 2>/dev/null)" || fail "helm install failed"

# --- checks ---------------------------------------------------------------------
kubectl get nodes -o wide
for n in "$NODE1_NAME" "$NODE2_NAME"; do
  arch="$(kubectl get node "$n" -o jsonpath='{.status.nodeInfo.architecture}' 2>/dev/null)"
  ver="$(kubectl get node "$n" -o jsonpath='{.status.nodeInfo.kubeletVersion}' 2>/dev/null)"
  ip="$(kubectl get node "$n" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null)"
  [[ "$arch" == arm64 ]] && pass "$n arm64, kubelet $ver, InternalIP $ip" || fail "$n not registered correctly (arch='$arch')"
done
wait_for "CoreDNS" 180 kubectl -n kube-system rollout status deploy/coredns --timeout=5s \
  && pass "CoreDNS ready" || fail "CoreDNS not ready"

# Cross-node pod network: a pod on Spark 2 resolves a service name and fetches CoreDNS
# metrics over plain HTTP from the CoreDNS pod (which runs on Spark 1). Spark 2 joined
# seconds ago, so allow up to 3 attempts while flannel and the image pull settle.
net_ok=0
for attempt in 1 2 3; do
  kubectl delete pod netcheck --ignore-not-found --wait=true >/dev/null 2>&1
  if kubectl run netcheck --rm -i --restart=Never --image=busybox:1.37 --pod-running-timeout=3m \
       --overrides="{\"spec\":{\"nodeSelector\":{\"spark-lab/node\":\"${NODE2_NAME}\"}}}" \
       --command -- sh -c 'wget -q -T 5 -O- http://kube-dns.kube-system.svc.cluster.local:9153/metrics | grep -m1 -c coredns_' \
       2>/dev/null | grep -q '^1'; then
    net_ok=1; break
  fi
  info "pod network check attempt ${attempt} failed; retrying in 20 s"
  sleep 20
done
if (( net_ok )); then
  pass "pod on ${NODE2_NAME} resolves DNS and reaches CoreDNS on ${NODE1_NAME} (flannel over ${MGMT_IF})"
else
  fail "pod network check from ${NODE2_NAME} failed — run ./scripts/netcheck.sh to see which layer breaks"
fi

summary "k3s cluster"
