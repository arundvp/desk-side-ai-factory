#!/usr/bin/env bash
# pull-results.sh — copy a run's result files from the load node to ./results/<run-id> on Spark 1.
# usage: pull-results.sh <run-id> [node]
source "$(dirname "$0")/lib.sh"
need kubectl
run="${1:?run id}"; node="${2:-$NODE2_NAME}"
pod="results-reader-$(date +%s)"
kubectl -n "$NS_LAB" run "$pod" --image=busybox:1.37 --restart=Never \
  --overrides="{\"spec\":{\"nodeSelector\":{\"spark-lab/node\":\"${node}\"},\"volumes\":[{\"name\":\"r\",\"hostPath\":{\"path\":\"${LAB_DATA_DIR}/results\"}}],\"containers\":[{\"name\":\"${pod}\",\"image\":\"busybox:1.37\",\"command\":[\"sleep\",\"600\"],\"volumeMounts\":[{\"name\":\"r\",\"mountPath\":\"/results\"}]}]}}" >/dev/null
kubectl -n "$NS_LAB" wait --for=condition=Ready "pod/$pod" --timeout=120s >/dev/null
mkdir -p "$RESULTS_DIR"
kubectl -n "$NS_LAB" cp "$pod:/results/$run" "$RESULTS_DIR/$run" >/dev/null
kubectl -n "$NS_LAB" delete pod "$pod" --wait=false >/dev/null
if [[ -f "$RESULTS_DIR/$run/stages.json" ]]; then
  pass "results in $RESULTS_DIR/$run: $(cd "$RESULTS_DIR/$run" && ls | tr '\n' ' ')"
else
  fail "no stages.json under $RESULTS_DIR/$run"
fi
