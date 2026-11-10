#!/usr/bin/env bash
# status.sh — one-screen view of the lab: nodes, GPU slices, pods, scrape targets, live numbers.
source "$(dirname "$0")/lib.sh"
need kubectl jq

echo "== nodes"
kubectl get nodes -L spark-lab/role,spark-lab/model-cache,nvidia.com/gpu.product \
  -o custom-columns='NAME:.metadata.name,STATUS:.status.conditions[?(@.type=="Ready")].status,GPU-SLICES:.status.allocatable.nvidia\.com/gpu,ROLE:.metadata.labels.spark-lab/role,MODEL:.metadata.labels.spark-lab/model-cache'
echo
echo "== GPU slices in use"
kubectl get pods -A -o json | jq -r '
  [.items[] | select(.status.phase=="Running" or .status.phase=="Pending")
   | {ns: .metadata.namespace, pod: .metadata.name, node: (.spec.nodeName // "-"),
      gpu: ([.spec.containers[].resources.limits["nvidia.com/gpu"] // "0" | tonumber] | add)}
   | select(.gpu > 0)] | .[] | "\(.node)\t\(.ns)/\(.pod)\t\(.gpu)"' | column -t -s $'\t'
echo
echo "== lab pods"
kubectl get pods -n "$NS_LLM" -o wide 2>/dev/null
kubectl get pods -n "$NS_LAB" -o wide 2>/dev/null | head -20
echo
echo "== scrape targets (up)"
prom_query 'sum by (job) (up)' | jq -r '.data.result[] | "\(.metric.job)\t\(.value[1])"' | column -t -s $'\t'
echo
echo "== live"
printf 'vLLM running/waiting     %s / %s\n' "$(prom_value 'sum(vllm:num_requests_running)')" "$(prom_value 'sum(vllm:num_requests_waiting)')"
printf 'generated tok/s          %s\n' "$(prom_value 'round(sum(spark:vllm_generation_tokens:rate30s))')"
printf 'KV cache usage           %s\n' "$(prom_value 'round(100 * max(vllm:kv_cache_usage_perc))')%"
prom_query 'spark:gpu_utilization:percent' | jq -r '.data.result[] | "GPU util \(.metric.node)        \(.value[1])%"'
prom_query 'round(spark:node_memory_available:bytes / 2^30)' | jq -r '.data.result[] | "mem available \(.metric.node)   \(.value[1]) GiB"'
