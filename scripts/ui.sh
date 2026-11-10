#!/usr/bin/env bash
# ui.sh — port-forward Grafana (3000), Prometheus (9090) and the vLLM API (8000) on Spark 1.
# Ctrl+C stops all three. From a workstation, tunnel to Spark 1 first:
#   ssh -L 3000:127.0.0.1:3000 -L 9090:127.0.0.1:9090 -L 8000:127.0.0.1:8000 <user>@<spark-1>
source "$(dirname "$0")/lib.sh"
need kubectl
pids=()
trap 'kill "${pids[@]}" 2>/dev/null' EXIT INT TERM
kubectl -n "$NS_MON" port-forward svc/kps-grafana 3000:80 >/dev/null & pids+=($!)
kubectl -n "$NS_MON" port-forward svc/kps-prometheus 9090:9090 >/dev/null & pids+=($!)
kubectl -n "$NS_LLM" port-forward svc/vllm 8000:8000 >/dev/null 2>&1 & pids+=($!)
sleep 2
cat <<EOF
Grafana     http://localhost:3000   user admin, password: $(cat "$LAB_ROOT/.grafana-admin-password" 2>/dev/null || echo "kubectl -n monitoring get secret grafana-admin -o jsonpath='{.data.admin-password}' | base64 -d")
            dashboard: Spark inference lab — vLLM, GPU, unified memory
Prometheus  http://localhost:9090
vLLM API    http://localhost:8000/v1   (model ${SERVED_MODEL_NAME})
Ctrl+C to stop.
EOF
wait
