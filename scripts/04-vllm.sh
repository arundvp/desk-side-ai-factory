#!/usr/bin/env bash
# 04-vllm.sh — download gpt-oss-120b, run vLLM on Kubernetes, prove it works.
# usage: 04-vllm.sh [all]            download on Spark 1, deploy, verify (default)
#        04-vllm.sh download <node>  download the model on a node and label it eligible
#        04-vllm.sh deploy           apply the Deployment and Service, wait until Ready
#        04-vllm.sh verify           health, models, metrics scrape, smoke test
#        04-vllm.sh scale <n>        1 or 2 replicas (2 needs the model on both Sparks)
source "$(dirname "$0")/lib.sh"
need kubectl jq python3 curl
V="$LAB_ROOT/k8s/vllm"

download() {
  local node="$1"
  if [[ "$(kubectl get node "$node" -o jsonpath='{.metadata.labels.spark-lab/model-cache}')" == gpt-oss-120b ]]; then
    pass "$node already has ${MODEL_ID} (label spark-lab/model-cache)"; return 0
  fi
  if [[ -n "${HF_TOKEN:-}" ]]; then
    kubectl -n "$NS_LLM" create secret generic hf-token --from-literal=HF_TOKEN="$HF_TOKEN" \
      --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  fi
  info "downloading ${MODEL_ID} on ${node} into ${LAB_DATA_DIR}/hf (~63 GB; 20–90 min depending on your link)"
  kubectl -n "$NS_LLM" delete job "model-download-$node" --ignore-not-found >/dev/null
  TARGET_NODE="$node" render "$V/model-download-job.yaml" | kubectl apply -f - >/dev/null
  local t=0
  until kubectl -n "$NS_LLM" wait --for=condition=complete "job/model-download-$node" --timeout=60s >/dev/null 2>&1; do
    t=$((t + 1))
    if kubectl -n "$NS_LLM" get job "model-download-$node" -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' | grep -q True; then
      kubectl -n "$NS_LLM" logs "job/model-download-$node" --tail=20 || true
      fail "download on $node failed"; return 1
    fi
    (( t % 5 == 0 )) && info "still downloading on $node ($t min): $(kubectl -n "$NS_LLM" logs "job/model-download-$node" --tail=1 2>/dev/null | tr '\r' '\n' | tail -n1 | cut -c1-110)"
    (( t > 240 )) && { fail "download on $node did not finish in 4 h"; return 1; }
  done
  local line shards size
  line="$(kubectl -n "$NS_LLM" logs "job/model-download-$node" | grep DOWNLOAD_OK | tail -n1)"
  shards="$(sed -n 's/.*shards=\([0-9]*\).*/\1/p' <<<"$line")"
  size="$(sed -n 's/.*size_gb=\([0-9.]*\).*/\1/p' <<<"$line")"
  if (( ${shards:-0} >= 10 )) && awk -v s="${size:-0}" 'BEGIN{exit !(s > 55 && s < 90)}'; then
    pass "$node: ${shards} weight shards, ${size} GB"
  else
    fail "$node: unexpected download result: ${line:-no DOWNLOAD_OK line} (expected >=10 shards, 55-90 GB)"; return 1
  fi
  kubectl label node "$node" spark-lab/model-cache=gpt-oss-120b --overwrite >/dev/null
}

# vLLM sizes its memory budget from the memory free when it starts, and on GB10 the page cache
# (e.g. the 65 GB the download just wrote) counts against that. Drop it on every model node first.
drop_caches() {
  local n
  for n in $(kubectl get nodes -l spark-lab/model-cache=gpt-oss-120b -o jsonpath='{.items[*].metadata.name}'); do
    if [[ "$n" == "$NODE1_NAME" ]]; then
      sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches' && info "dropped page cache on $n ($(free -g | awk '/^Mem:/{print $7}') GiB available)"
    else
      ssh -t "${SSH_USER}@${NODE2_MGMT}" "sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'" && info "dropped page cache on $n"
    fi
  done
}

# gpt-oss needs the Harmony tokenizer vocab (o200k_base). vLLM downloads it on the first request
# into a hostPath cache; a node whose pods cannot fetch it fails every request with HTTP 500
# "invalid tiktoken vocab file". Copy spark-1's cached vocab to every other model node.
seed_tiktoken() {
  local d="${LAB_DATA_DIR}/cache/tiktoken" n
  if ! ls -A "$d" 2>/dev/null | grep -q .; then
    info "no Harmony vocab cached on $NODE1_NAME yet (vLLM fetches it on the first request; re-run deploy after verify to copy it)"
    return 0
  fi
  for n in $(kubectl get nodes -l spark-lab/model-cache=gpt-oss-120b -o jsonpath='{.items[*].metadata.name}'); do
    [[ "$n" == "$NODE1_NAME" ]] && continue
    if ssh "${SSH_USER}@${NODE2_MGMT}" "ls -A '$d' 2>/dev/null | grep -q ." 2>/dev/null; then continue; fi
    tar -C "$d" -cf /tmp/spark-lab-tiktoken.tar . && scp -q /tmp/spark-lab-tiktoken.tar "${SSH_USER}@${NODE2_MGMT}:/tmp/" \
      && ssh -t "${SSH_USER}@${NODE2_MGMT}" "sudo mkdir -p '$d' && sudo tar -C '$d' -xf /tmp/spark-lab-tiktoken.tar" \
      && pass "copied Harmony vocab cache to $n" || warn "could not copy the Harmony vocab cache to $n ($d)"
  done
}

deploy() {
  # keep the current replica count (a re-deploy must not undo `scale 2`); scale() sets it explicitly
  local cur; cur="$(kubectl -n "$NS_LLM" get deploy vllm -o jsonpath='{.spec.replicas}' 2>/dev/null)"
  export VLLM_REPLICAS="${VLLM_REPLICAS:-${cur:-1}}"
  drop_caches
  seed_tiktoken
  render "$V/vllm.yaml" | kubectl apply -f - >/dev/null
  info "waiting for vLLM to load weights, compile and capture CUDA graphs (first start 10–30 min)"
  local t=0 seen_rc=0
  until kubectl -n "$NS_LLM" rollout status deploy/vllm --timeout=60s >/dev/null 2>&1; do
    t=$((t + 1))
    # follow a pod that is not Ready yet (with 2 replicas, the new one), else the first
    local pod; pod="$(kubectl -n "$NS_LLM" get pod -l app=vllm -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' 2>/dev/null | awk '$2!="True"{print $1; exit}')"
    [[ -z "$pod" ]] && pod="$(kubectl -n "$NS_LLM" get pod -l app=vllm -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)"
    if [[ -n "$pod" ]]; then
      local why; why="$(kubectl -n "$NS_LLM" get pod "$pod" -o jsonpath='{.status.containerStatuses[0].state.waiting.reason}{.status.conditions[?(@.type=="PodScheduled")].reason}' 2>/dev/null)"
      [[ "$why" == *CrashLoopBackOff* ]] && { kubectl -n "$NS_LLM" logs "$pod" --tail=40 || true; fail "vLLM crash-looping"; return 1; }
      [[ "$why" == *Unschedulable* ]] && { kubectl -n "$NS_LLM" describe pod "$pod" | sed -n '/Events:/,$p' | tail -5; fail "vLLM pod unschedulable"; return 1; }
      local rc last
      rc="$(kubectl -n "$NS_LLM" get pod "$pod" -o jsonpath='{.status.containerStatuses[0].restartCount}' 2>/dev/null)"
      if (( ${rc:-0} > ${seen_rc:-0} )); then
        last="$(kubectl -n "$NS_LLM" get pod "$pod" -o jsonpath='{.status.containerStatuses[0].lastState.terminated.reason} exit={.status.containerStatuses[0].lastState.terminated.exitCode}' 2>/dev/null)"
        warn "vLLM container restarted (restart ${rc}, last: ${last}); previous log tail:"
        kubectl -n "$NS_LLM" logs "$pod" --previous --tail=200 2>/dev/null | tr '\r' '\n' | grep -v 'Loading safetensors' | tail -n 25 | sed 's/^/    /'
        seen_rc="$rc"
        (( rc >= 3 )) && { fail "vLLM restarted ${rc} times — see the log tail above"; return 1; }
      fi
      (( t % 2 == 0 )) && info "($t min) $(kubectl -n "$NS_LLM" logs "$pod" --tail=1 2>/dev/null | tr '\r' '\n' | grep -v '^$' | tail -n1 | cut -c1-120)"
    fi
    (( t > 50 )) && { fail "vLLM not Ready after 50 min"; return 1; }
  done
  pass "vLLM Deployment ready: $(kubectl -n "$NS_LLM" get pods -l app=vllm -o custom-columns=POD:.metadata.name,NODE:.spec.nodeName --no-headers | tr '\n' ' ')"
}

verify() {
  local pod; pod="$(kubectl -n "$NS_LLM" get pod -l app=vllm -o jsonpath='{.items[0].metadata.name}')"
  local logs; logs="$(kubectl -n "$NS_LLM" logs "$pod" 2>/dev/null)"
  local kv; kv="$(grep -Eio 'GPU KV cache size: [0-9,]+ tokens' <<<"$logs" | tail -n1)"
  local mc; mc="$(grep -Eio 'Maximum concurrency for [0-9,]+ tokens per request: [0-9.]+x' <<<"$logs" | tail -n1)"
  local wt; wt="$(grep -Eio 'Model loading took [0-9.]+ GiB' <<<"$logs" | tail -n1)"
  [[ -n "$kv" ]] && pass "${kv}; ${mc:-max concurrency line not found}; ${wt:-}" \
    || warn "KV cache size line not found in the log (log format may differ in this image)"

  local svc="/api/v1/namespaces/${NS_LLM}/services/http:vllm:8000/proxy"
  kubectl get --raw "$svc/health" >/dev/null 2>&1 && pass "/health 200 through the Service" || fail "/health failed through the Service"
  local mid; mid="$(kubectl get --raw "$svc/v1/models" 2>/dev/null | jq -r '.data[0].id // empty')"
  [[ "$mid" == "$SERVED_MODEL_NAME" ]] && pass "/v1/models lists $mid" \
    || warn "/v1/models returned '${mid}' (an API key set? expected ${SERVED_MODEL_NAME})"

  wait_for "Prometheus scraping vLLM" 120 bash -c \
    "[[ \$(kubectl get --raw '/api/v1/namespaces/${NS_MON}/services/http:kps-prometheus:9090/proxy/api/v1/query?query=count(vllm%3Akv_cache_usage_perc)' | jq -r '.data.result[0].value[1] // 0') -ge 1 ]]" \
    && pass "vllm:* metrics in Prometheus (node=$(prom_query 'vllm:kv_cache_usage_perc' | jq -r '[.data.result[].metric.node] | join(",")'))" \
    || fail "Prometheus is not scraping vLLM (check: kubectl -n ${NS_LLM} get servicemonitor vllm)"

  info "warm-up request (JIT on first call can take ~30 s), then the smoke test"
  local key; key="$(kubectl -n "$NS_LLM" get secret vllm-api-key -o jsonpath='{.data.VLLM_API_KEY}' 2>/dev/null | base64 -d 2>/dev/null || true)"
  export VLLM_API_KEY="${key:-}"
  if with_port_forward "$NS_LLM" vllm 18000 8000 bash -c \
       "curl -s -m 300 http://127.0.0.1:18000/v1/chat/completions -H 'Content-Type: application/json' \
          \${VLLM_API_KEY:+-H \"Authorization: Bearer \$VLLM_API_KEY\"} \
          -d '{\"model\":\"${SERVED_MODEL_NAME}\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"max_tokens\":64}' >/dev/null; \
        MAX_TOKENS=4096 '$LAB_ROOT/scripts/vllm-smoke.sh' http://127.0.0.1:18000"; then
    pass "smoke test: health, models, chat, tool call"
  else
    fail "smoke test failed (see lines above; a null first answer points at the MXFP4 backend — Troubleshooting)"
  fi
}

scale() {
  local n="$1"
  if [[ "$n" == 2 ]]; then
    download "$NODE2_NAME" || return 1
  fi
  VLLM_REPLICAS="$n" deploy || return 1
  local ready; ready="$(kubectl -n "$NS_LLM" get deploy vllm -o jsonpath='{.status.readyReplicas}')"
  [[ "${ready:-0}" == "$n" ]] && pass "$n of $n vLLM replicas ready" || fail "only ${ready:-0} of $n vLLM replicas ready"
  kubectl -n "$NS_LLM" get pods -l app=vllm -o wide
}

ensure_ns "$NS_LLM"
case "${1:-all}" in
  all)      download "$NODE1_NAME" && deploy && verify ;;
  download) download "${2:?node name}" ;;
  deploy)   deploy ;;
  verify)   verify ;;
  scale)    scale "${2:?1 or 2}" ;;
  *)        sed -n '2,8p' "$0"; exit 2 ;;
esac
summary "vLLM ${1:-all}"
