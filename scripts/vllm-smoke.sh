#!/usr/bin/env bash
# vllm-smoke.sh — pass/fail functional checks for a vLLM OpenAI-compatible endpoint
# (same script as the single-Spark vLLM runbook, Step 4d).
# usage: ./vllm-smoke.sh [BASE_URL]        default: http://127.0.0.1:${PORT:-8000}
# Honors VLLM_API_KEY (sent as a bearer token) and MAX_TOKENS (default 4096).
# Exit code 0 = all checks passed.
set -uo pipefail

BASE="${1:-http://127.0.0.1:${PORT:-8000}}"
MAX_TOKENS="${MAX_TOKENS:-4096}"
AUTH=()
if [[ -n "${VLLM_API_KEY:-}" ]]; then
  AUTH=(-H "Authorization: Bearer ${VLLM_API_KEY}")
fi
command -v jq >/dev/null || { echo "jq is required: sudo apt-get install -y jq"; exit 2; }

fail=0
pass() { printf 'PASS  %s\n' "$*"; }
bad()  { printf 'FAIL  %s\n' "$*"; fail=1; }

# 1. health
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/health")
if [[ "$code" == "200" ]]; then pass "health: HTTP 200"; else bad "health: HTTP $code (server down or still loading?)"; exit 1; fi

# 2. model list
MODEL_ID=$(curl -sS "${AUTH[@]}" "$BASE/v1/models" | jq -r '.data[0].id // empty')
if [[ -n "$MODEL_ID" ]]; then pass "models: $MODEL_ID"; else bad "models: empty /v1/models (wrong API key?)"; exit 1; fi

chat() {
  curl -sS --max-time 600 "${AUTH[@]}" "$BASE/v1/chat/completions" \
    -H 'Content-Type: application/json' -d "$1" -w '\n%{time_total}'
}

# 3. chat completion
body=$(jq -n --arg m "$MODEL_ID" --argjson n "$MAX_TOKENS" \
  '{model:$m, messages:[{role:"user",content:"Reply with a short greeting."}], max_tokens:$n}')
out=$(chat "$body"); secs=${out##*$'\n'}; r=${out%$'\n'*}
fin=$(jq -r '.choices[0].finish_reason // "none"' <<<"$r" 2>/dev/null)
txt=$(jq -r '.choices[0].message.content // ""' <<<"$r" 2>/dev/null)
rlen=$(jq -r '(.choices[0].message.reasoning // .choices[0].message.reasoning_content // "") | length' <<<"$r" 2>/dev/null)
if [[ "$fin" == "stop" && -n "$txt" ]]; then
  pass "chat (${secs}s, reasoning ${rlen:-0} chars): ${txt:0:70}"
elif [[ "$fin" == "length" ]]; then
  bad "chat: finish_reason=length — raise MAX_TOKENS (reasoning used the budget)"
else
  bad "chat: finish_reason=$fin content='${txt:0:70}' raw='${r:0:200}'"
fi

# 4. tool call
body=$(jq -n --arg m "$MODEL_ID" --argjson n "$MAX_TOKENS" '{
  model:$m,
  messages:[{role:"user",content:"Use get_weather to look up the weather in Boston."}],
  tools:[{type:"function",function:{name:"get_weather",description:"Get current weather for a location.",
          parameters:{type:"object",properties:{location:{type:"string"}},required:["location"]}}}],
  tool_choice:"auto", max_tokens:$n}')
out=$(chat "$body"); secs=${out##*$'\n'}; r=${out%$'\n'*}
fin=$(jq -r '.choices[0].finish_reason // "none"' <<<"$r" 2>/dev/null)
fn=$(jq -r '.choices[0].message.tool_calls[0].function.name // ""' <<<"$r" 2>/dev/null)
args=$(jq -r '.choices[0].message.tool_calls[0].function.arguments // ""' <<<"$r" 2>/dev/null)
if [[ "$fin" == "tool_calls" && "$fn" == "get_weather" && "$args" == *Boston* ]]; then
  pass "tool call (${secs}s): $fn $args"
else
  bad "tool call: finish_reason=$fin name='$fn' args='$args' (check --tool-call-parser / --enable-auto-tool-choice)"
fi

exit "$fail"
