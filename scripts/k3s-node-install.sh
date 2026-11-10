#!/usr/bin/env bash
# k3s-node-install.sh — install k3s on THIS Spark as server or agent.
# Called by 01-k3s-install.sh (locally on Spark 1, over SSH on Spark 2). Needs sudo.
# usage: k3s-node-install.sh server <k3s-version> <node-ip> <iface> <node-name>
#        k3s-node-install.sh agent  <k3s-version> <node-ip> <iface> <node-name> <server-ip> <token-file>
set -euo pipefail

role="$1" version="$2" node_ip="$3" iface="$4" node_name="$5"

if [[ "$(hostname)" != "$node_name" ]]; then
  echo "this host is '$(hostname)', expected '$node_name' — fix NODE*_NAME in lab.env" >&2
  exit 2
fi

unit="k3s"; [[ "$role" == agent ]] && unit="k3s-agent"

# RDMA (NCCL over RoCE) needs unlimited locked memory inside pods. Containers inherit the
# limit of containerd, which inherits it from the k3s systemd unit, so raise it there.
sudo mkdir -p "/etc/systemd/system/${unit}.service.d"
printf '[Service]\nLimitMEMLOCK=infinity\n' | sudo tee "/etc/systemd/system/${unit}.service.d/10-spark-lab-memlock.conf" >/dev/null

common=(--node-ip "$node_ip" --flannel-iface "$iface" --node-label "spark-lab/node=${node_name}")

if [[ "$role" == server ]]; then
  curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION="$version" sh -s - server \
    "${common[@]}" \
    --advertise-address "$node_ip" \
    --tls-san "$node_ip" \
    --write-kubeconfig-mode 644 \
    --disable traefik \
    --disable servicelb
else
  server_ip="$6" token_file="$7"
  K3S_TOKEN="$(cat "$token_file")"
  rm -f "$token_file"
  curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION="$version" \
    K3S_URL="https://${server_ip}:6443" K3S_TOKEN="$K3S_TOKEN" sh -s - agent "${common[@]}"
fi

sudo systemctl daemon-reload
sudo systemctl restart "$unit"

# k3s adds an "nvidia" runtime to its containerd config when nvidia-container-runtime is on PATH
for _ in $(seq 1 30); do
  if sudo grep -q 'nvidia' /var/lib/rancher/k3s/agent/etc/containerd/config.toml 2>/dev/null; then
    echo "nvidia_runtime=detected"
    break
  fi
  sleep 2
done
sudo grep -q 'nvidia' /var/lib/rancher/k3s/agent/etc/containerd/config.toml 2>/dev/null || echo "nvidia_runtime=missing"

pid="$(systemctl show -p MainPID --value "$unit")"
echo "memlock=$(awk '/Max locked memory/ {print $4}' "/proc/${pid}/limits" 2>/dev/null)"
