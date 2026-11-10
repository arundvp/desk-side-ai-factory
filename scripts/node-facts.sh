#!/usr/bin/env bash
# node-facts.sh — read-only facts about one Spark, printed as key=value lines.
# Runs locally on Spark 1 and over SSH on Spark 2 (00-preflight.sh). No root needed.
# usage: node-facts.sh <lab-data-dir>
set -uo pipefail
DATA_DIR="${1:-/srv/spark-lab}"

kv() { printf '%s=%s\n' "$1" "$2"; }

kv hostname "$(hostname)"
kv arch "$(uname -m)"
kv kernel "$(uname -r)"
kv dgx_release "$( (grep -h '^DGX_OTA_VERSION=' /etc/dgx-release 2>/dev/null || grep -h '^DGX_SWBUILD_VERSION=' /etc/dgx-release 2>/dev/null) | tail -n1 | cut -d= -f2 | tr -d '"')"

if command -v nvidia-smi >/dev/null 2>&1; then
  kv gpu_name "$(nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null | head -n1)"
  kv driver "$(nvidia-smi --query-gpu=driver_version --format=csv,noheader 2>/dev/null | head -n1)"
else
  kv gpu_name ""
  kv driver ""
fi

kv nvidia_container_runtime "$(command -v nvidia-container-runtime 2>/dev/null || echo "")"
kv nvidia_ctk "$(nvidia-ctk --version 2>/dev/null | head -n1 | awk '{print $NF}')"
kv nvidia_cdi_hook "$(command -v nvidia-cdi-hook 2>/dev/null || echo "")"

kv mem_total_gib "$(awk '/^MemTotal:/ {printf "%.1f", $2/1048576}' /proc/meminfo)"
kv mem_avail_gib "$(awk '/^MemAvailable:/ {printf "%.1f", $2/1048576}' /proc/meminfo)"
kv swap_total_gib "$(awk '/^SwapTotal:/ {printf "%.1f", $2/1048576}' /proc/meminfo)"

# free space where the lab data will live (walk up to the first existing parent)
d="$DATA_DIR"
while [[ ! -d "$d" && "$d" != "/" ]]; do d="$(dirname "$d")"; done
kv data_dir_free_gib "$(df -BG --output=avail "$d" 2>/dev/null | tail -n1 | tr -dc '0-9')"
kv data_dir_model_present "$( [[ -d "$DATA_DIR/hf/hub/models--openai--gpt-oss-120b" ]] && echo yes || echo no)"

kv k3s_installed "$( (command -v k3s >/dev/null && k3s --version 2>/dev/null | head -n1 | awk '{print $3}') || echo "")"
kv port_6443_busy "$(ss -ltnH 'sport = :6443' 2>/dev/null | grep -q . && echo yes || echo no)"
kv port_10250_busy "$(ss -ltnH 'sport = :10250' 2>/dev/null | grep -q . && echo yes || echo no)"

if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  kv docker_gpu_containers "$(docker ps --format '{{.Names}}:{{.Image}}' 2>/dev/null | grep -Ei 'vllm|sglang|trtllm|nim|ollama' | tr '\n' ' ')"
else
  kv docker_gpu_containers ""
fi

for t in curl jq python3 openssl helm kubectl; do
  kv "tool_$t" "$(command -v "$t" >/dev/null 2>&1 && echo yes || echo no)"
done
kv nccl_env "$( [[ -f "$HOME/.config/dgx-spark-roce/nccl.env" ]] && echo yes || echo no)"
kv infiniband_devices "$(ls /sys/class/infiniband 2>/dev/null | tr '\n' ' ')"
