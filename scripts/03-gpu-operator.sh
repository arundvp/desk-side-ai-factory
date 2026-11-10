#!/usr/bin/env bash
# 03-gpu-operator.sh — NVIDIA GPU Operator (device plugin with 4-way time-slicing, GFD,
# DCGM Exporter), the nvidia-smi exporter, and a CUDA smoke test on each Spark.
source "$(dirname "$0")/lib.sh"
need helm kubectl jq python3
G="$LAB_ROOT/k8s/gpu"

ensure_ns "$NS_GPU" "$NS_LAB" "$NS_MON"
helm repo add nvidia https://helm.ngc.nvidia.com/nvidia >/dev/null 2>&1 || true
helm repo update nvidia >/dev/null

info "installing GPU Operator ${GPU_OPERATOR_VERSION} (driver and toolkit from DGX OS)"
helm upgrade --install gpu-operator nvidia/gpu-operator \
  --version "$GPU_OPERATOR_VERSION" -n "$NS_GPU" \
  -f "$G/gpu-operator-values.yaml" \
  --wait --timeout 20m

wait_for "ClusterPolicy ready" 900 bash -c \
  "[[ \$(kubectl get clusterpolicy cluster-policy -o jsonpath='{.status.state}') == ready ]]" \
  && pass "ClusterPolicy ready" \
  || warn "ClusterPolicy not ready — see 'kubectl -n ${NS_GPU} get pods'; a crash-looping nvidia-dcgm-exporter is the known GB10 case (Troubleshooting)"
kubectl -n "$NS_GPU" get pods -o wide

for n in "$NODE1_NAME" "$NODE2_NAME"; do
  cap="$(kubectl get node "$n" -o jsonpath='{.status.capacity.nvidia\.com/gpu}')"
  prod="$(kubectl get node "$n" -o jsonpath='{.metadata.labels.nvidia\.com/gpu\.product}')"
  rep="$(kubectl get node "$n" -o jsonpath='{.metadata.labels.nvidia\.com/gpu\.replicas}')"
  [[ "$cap" == 4 ]] && pass "$n advertises nvidia.com/gpu=4 ($prod, replicas=$rep)" \
    || fail "$n advertises nvidia.com/gpu='${cap}' (want 4) — check the device plugin log"
done

info "deploying the nvidia-smi exporter"
render "$G/nvsmi-exporter.yaml" | kubectl apply -f - >/dev/null
kubectl -n "$NS_MON" rollout status ds/nvsmi-exporter --timeout=300s >/dev/null \
  && pass "nvsmi-exporter running on $(kubectl -n "$NS_MON" get ds nvsmi-exporter -o jsonpath='{.status.numberReady}') nodes" \
  || fail "nvsmi-exporter not ready"

info "CUDA smoke test on both Sparks (pulls ${PYTORCH_IMAGE} the first time; ~10 min)"
for n in "$NODE1_NAME" "$NODE2_NAME"; do
  kubectl -n "$NS_LAB" delete pod "cuda-smoke-$n" --ignore-not-found >/dev/null
  TARGET_NODE="$n" render "$G/cuda-smoke.yaml" | kubectl apply -f - >/dev/null
done
for n in "$NODE1_NAME" "$NODE2_NAME"; do
  wait_for "cuda-smoke-$n to finish" 1800 bash -c \
    "kubectl -n ${NS_LAB} get pod cuda-smoke-$n -o jsonpath='{.status.phase}' | grep -Eq 'Succeeded|Failed'" || true
  log="$(kubectl -n "$NS_LAB" logs "cuda-smoke-$n" 2>&1 || true)"
  if grep -q SMOKE_OK <<<"$log"; then
    pass "$n: $(grep -m1 '^torch ' <<<"$log"); $(grep -m1 SMOKE_OK <<<"$log" | sed 's/SMOKE_OK //')"
  else
    fail "$n: CUDA smoke test failed: $(tail -n 5 <<<"$log" | tr '\n' ' ')"
  fi
done
kubectl -n "$NS_LAB" delete pod -l app=cuda-smoke --ignore-not-found >/dev/null

info "GPU telemetry in Prometheus (allow ~30 s for the first scrapes)"
sleep 30
bash "$LAB_ROOT/scripts/dcgm-probe.sh" || true
pq() { kubectl get --raw "/api/v1/namespaces/${NS_MON}/services/http:kps-prometheus:9090/proxy/api/v1/query?query=$1" | jq -r '.data.result[0].value[1] // 0'; }
[[ "$(pq 'count(spark_nvsmi_up%3D%3D1)')" -ge 2 ]] && pass "nvidia-smi exporter scraped on both nodes" \
  || fail "spark_nvsmi_up missing in Prometheus"
[[ "$(pq 'count(spark%3Agpu_utilization%3Apercent)')" -ge 2 ]] && pass "spark:gpu_utilization:percent recorded for both nodes" \
  || fail "GPU recording rules produce no series"

summary "GPU operator"
