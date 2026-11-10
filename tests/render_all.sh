#!/usr/bin/env bash
# render_all.sh — render every templated manifest with lab.env.example values into tests/out/
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/tests/out"; rm -rf "$OUT"; mkdir -p "$OUT"
cp "$ROOT/lab.env.example" "$OUT/lab.env"
export LAB_ENV="$OUT/lab.env"
# shellcheck source=../scripts/lib.sh
source "$ROOT/scripts/lib.sh"
TARGET_NODE="$NODE1_NAME" render "$ROOT/k8s/gpu/cuda-smoke.yaml" > "$OUT/cuda-smoke.yaml"
render "$ROOT/k8s/gpu/nvsmi-exporter.yaml" > "$OUT/nvsmi-exporter.yaml"
TARGET_NODE="$NODE1_NAME" render "$ROOT/k8s/vllm/model-download-job.yaml" > "$OUT/model-download-job.yaml"
VLLM_REPLICAS=1 render "$ROOT/k8s/vllm/vllm.yaml" > "$OUT/vllm.yaml"
TARGET_NODE="$NODE1_NAME" BURN_SECONDS=180 BURN_DUTY=1.0 BURN_ALLOC_GB=0 render "$ROOT/k8s/pytorch/torch-burn-job.yaml" > "$OUT/torch-burn-job.yaml"
render "$ROOT/k8s/pytorch/torch-allreduce.yaml" > "$OUT/torch-allreduce.yaml"
LOADGEN_NO_KEEPALIVE=0 RUN_ID=20261029t120000z-ramp PROFILE=ramp TIME_SCALE=1.0 TARGET_NODE="$NODE2_NAME" render "$ROOT/k8s/load/loadgen-job.yaml" > "$OUT/loadgen-job.yaml"
RUN_ID=20261029t120000z-bench SWEEP_CONCURRENCY="1 2 4 8 16 32 64 128" TARGET_NODE="$NODE2_NAME" render "$ROOT/k8s/load/vllm-bench-sweep-job.yaml" > "$OUT/vllm-bench-sweep-job.yaml"
cp "$ROOT/k8s/monitoring/servicemonitors.yaml" "$ROOT/k8s/monitoring/prometheusrules.yaml" "$ROOT/k8s/gpu/dcgm-metrics-basic.yaml" "$OUT/"
left=0
for v in $RENDER_VARS; do
  if grep -n "\${$v}" "$OUT"/*.yaml; then left=1; fi
done
(( left == 0 )) || { echo "unrendered lab placeholders above" >&2; exit 1; }
echo "rendered: $(ls "$OUT"/*.yaml | wc -l) manifests"
