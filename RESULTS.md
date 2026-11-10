# Results: Desk-Side AI Factory benchmarks

Measured on two NVIDIA DGX Spark systems (GB10, 128 GB unified memory each, 200 Gb/s ConnectX-7 RoCEv2 link),
November 2026. vLLM container `nvcr.io/nvidia/vllm:26.09-py3`, model `openai/gpt-oss-120b`
(MXFP4). One replica unless stated.

vLLM settings: `--gpu-memory-utilization 0.75`, `--max-model-len 65536`, `--max-num-seqs 64`,
`--max-num-batched-tokens 8192`, prefix caching on, `--moe-backend marlin`,
`--attention-backend TRITON_ATTN`.

Throughput and queue numbers come from vLLM's Prometheus counters; latency percentiles come
from the load generator unless marked "server". Server latency histograms jump from 160 s to
640 s buckets, so server tails above ~160 s are not reliable.

## Baselines

| Item | Result |
|---|---|
| vLLM cold start (weights, MoE repack, KV sizing, compile cache, CUDA graphs) | ~8 min (weights ~6.5 min for 15 shards) |
| KV cache | ~505k tokens (~48 KiB per token); ~7.7 concurrent 64k-token requests |
| PyTorch burn (matmul) | 95.6 TFLOP/s at ~92 W, 78 °C |
| NCCL all_reduce between nodes | 23.2 GB/s bus bandwidth (~186 Gb/s, ~93% of the link) at 2 GB messages; ~0.5-0.8 ms per 1-8 MB message |

## One replica: where the limits are

| Question | Answer |
|---|---|
| Generation ceiling (short chats) | ~360-374 tok/s, set by `max-num-seqs 64` |
| Request capacity (open loop) | ~0.94 req/s |
| Interactive limit (TTFT p95 under 2 s) | ~16 concurrent users, ~0.5 req/s |
| Long prompts (16k tokens) that fit in KV at once | ~30 |
| Prefill speed | ~4,400 prompt tok/s at up to ~96 W |
| Decode power at full batch | ~40-75 W (memory-bandwidth bound) |

## Prefix caching (2,048-token shared system prompt + 256 unique tokens vs 2,304 unique tokens, 16 users)

| Stage | Prefix cache hits | TTFT p50 | TTFT p95 | Requests in 180 s |
|---|---|---|---|---|
| unique prompts | 3% | 5.53 s | 6.68 s | 112 |
| shared prefix | 88% | 1.19 s | 1.30 s | 128 |

Cached prefixes cut time to first token by ~4.6x. Decode speed is unchanged; the cache only
skips prompt work.

## Traffic spike: 8 users, then 200 for 90 s, then 8 again

| | 1 replica | 2 replicas |
|---|---|---|
| Server generation rate during the spike | 374 tok/s | 764 tok/s |
| Requests waiting (average) during the spike | 134.5 | 76.5 |
| TTFT p50 / p95 during the spike | 136 s / 189 s | 8.8 s / 76 s |
| TTFT p50 for users arriving after the spike | 175 s | 0.96 s |
| Requests still in flight when the run ended | 80 | 8 |
| Errors | 0 | 0 |

vLLM never rejected a request; overload showed up only as queueing. With one replica the
backlog outlasted the spike by minutes. A second replica doubled throughput, and because the
queue was both shorter and drained twice as fast, waiting time fell far more than 2x.

## Two replicas behind a Kubernetes Service (prefix profile, 16 users)

| Shared-prefix stage | Split across replicas (running) | TTFT p50 | Requests |
|---|---|---|---|
| 1 replica | - | 1.19 s | 128 |
| 2 replicas, keep-alive connections | 96% on one replica | 1.05 s | 134 |
| 2 replicas, new connection per request | 66% on one replica | 0.62 s | 165 |

A Kubernetes Service picks a backend per TCP connection. With keep-alive clients, one replica
took almost all the load and the second GPU sat idle. A new connection per request fixed most
of it; random choice still leaves short-term imbalance, which least-request or queue-aware
routing would address. With a single shared prefix the cache hit ratio stayed at 88% either way:
each replica misses once, then hits.

## Problems found and fixed along the way

| Symptom | Cause | Fix |
|---|---|---|
| node-exporter target down, scrapes time out | `cpufreq` collector stalls on GB10 | `--no-collector.cpufreq` |
| vLLM restarts at startup with exit 1 | page cache counts against free unified memory | drop page cache before deploy |
| Second replica fails every request with HTTP 500 "invalid tiktoken vocab file" | tokenizer vocab missing on the new node | copy the cached vocab to every model node |
| `scale 2` silently reverted to one replica | manifest re-apply reset `replicas` | template the replica count; verify n of n ready |
| Load run reported OK with 384 failed requests | no error check | warn when more than 1% of requests fail |
| 68 timeouts in the KV-pressure test | default client timeout too short for 16k prompts | per-profile timeout (1,800 s) |
