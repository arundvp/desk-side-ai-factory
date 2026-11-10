# Desk-Side AI Factory: Serving gpt-oss-120b with vLLM on Kubernetes

*What two NVIDIA DGX Spark systems taught me about throughput ceilings, traffic spikes, prefix caching and load balancing for LLM inference.*

## Why build an AI factory on a desk

Two NVIDIA DGX Spark desktops can serve a 120-billion-parameter open-weight model to real traffic, and they teach the same lessons as a datacenter: where throughput tops out, why latency collapses under a spike, and how a second replica changes everything.

I built a small, fully instrumented inference stack to see those lessons with real numbers instead of vendor slides. It runs OpenAI's open-weight `gpt-oss-120b` with vLLM on a two-node Kubernetes cluster, watched by Prometheus and Grafana, and pushed with staged load tests: steady chat, long contexts, shared system prompts and a sudden 25x traffic spike.

This post covers what I built, what the measurements showed, and the surprises that cost the most time. The code, scripts and full results are open source in the [desk-side-ai-factory](https://github.com/arundvp/desk-side-ai-factory) repository.

## The setup

The whole factory is two NVIDIA DGX Spark systems, a 200 Gb/s cable and about a dozen scripts. Each DGX Spark has a GB10 Grace Blackwell Superchip with 128 GB of memory shared by CPU and GPU, so one box holds the full 120B model with room left for KV cache.

| Layer | What runs | Why it matters |
| --- | --- | --- |
| Cluster | k3s: server on spark-1, agent on spark-2, pod network over the ConnectX-7 RoCEv2 link | Real Kubernetes, small footprint |
| GPU | NVIDIA GPU Operator with time-slicing, DCGM exporter plus an nvidia-smi fallback | GB10 exposes only a few DCGM fields |
| Serving | vLLM with `gpt-oss-120b` (MXFP4), one replica per node | Each node holds the whole model, so scaling out needs no sharding |
| Telemetry | Prometheus, Grafana, recording rules for GPU, node and vLLM | One dashboard for tokens, queues, power and clocks |
| Load | A Python load generator run as a Kubernetes Job | Staged profiles: ramp, spike, long context, shared prefix, open loop |
| Analysis | A script that joins client records with Prometheus | Per-stage tables and plain-language findings |

Every step is one idempotent script that prints PASS, WARN or FAIL, so a broken step is obvious and a rerun is safe. All settings live in one env file, and the manifests are templates rendered from it.

## The limits of one replica

One DGX Spark running vLLM tops out at about 370 generated tokens per second and just under one request per second. Past that, extra users only wait longer.

| Question | Measured answer |
| --- | --- |
| Generation ceiling (short chats) | 360-374 tok/s, capped by 64 concurrent sequences |
| Request capacity (open loop) | ~0.94 requests/s |
| Users served with first token under 2 s (p95) | ~16 concurrent |
| 16k-token prompts that fit in KV cache at once | ~30 |
| Prefill speed | ~4,400 prompt tok/s |
| Cold start to ready | ~8 min |

Two measurement lessons came out of this stage. First, the GPU "busy" counter reads about 96% even with a single user, because it only reports whether any kernel is running. Power is the honest signal: decode drew 40-75 W while prefill reached ~96 W, against ~92 W in a pure matmul burn test.

Second, at high concurrency the client and server disagree about throughput. The client books a request's tokens to the stage that sent it, even if the tokens arrive minutes later. The server counts what it generated in each window, so for anything above the ceiling I trust the server.

## Prefix caching: 4.6x faster first token

When every request starts with the same 2,048-token system prompt, vLLM's prefix cache cut time to first token from 5.5 s to 1.2 s at 16 concurrent users.

The test compares two stages of identical prompt size: 2,304 unique tokens per request, against a shared 2,048-token prefix plus 256 unique tokens. Only the sharing changes.

| Stage | Cache hit ratio | TTFT p50 | TTFT p95 | Requests in 180 s |
| --- | --- | --- | --- | --- |
| Unique prompts | 3% | 5.53 s | 6.68 s | 112 |
| Shared prefix | 88% | 1.19 s | 1.30 s | 128 |

Decode speed did not change, because the cache only skips prompt work. The lesson for agents and RAG: a long, stable system prompt or tool block is nearly free after the first request, as long as it stays byte-identical at the start of the prompt.

My first version of this test was unfair: the shared stage sent longer prompts than the unique one. Equal-size prompts are what make the comparison mean something.

## A 25x traffic spike, with one replica and with two

Under a spike, vLLM never returned an error; it queued. With one replica, users arriving after the spike still waited about three minutes for a first token. With two replicas, they waited under a second.

The profile runs 8 users for 2 minutes, jumps to 200 users for 90 seconds, then drops back to 8.

| Metric | 1 replica | 2 replicas |
| --- | --- | --- |
| Server generation rate in the spike | 374 tok/s | 764 tok/s |
| Requests waiting, average | 134.5 | 76.5 |
| TTFT p50 / p95 in the spike | 136 s / 189 s | 8.8 s / 76 s |
| TTFT p50 after the spike | 175 s | 0.96 s |
| Errors | 0 | 0 |

Throughput scaled 2.04x, because each DGX Spark holds the full model and the two share nothing. Waiting time fell about 15x at the median, far more than 2x. Doubling capacity both shortens the queue (200 users against 128 slots instead of 64) and drains it twice as fast, and the two effects multiply.

The operational point: an overloaded LLM server looks healthy. No errors, GPUs busy, and users waiting minutes. Alert on queue depth (`num_requests_waiting`) and time to first token, not on error rate or GPU utilisation, and shed load at a gateway before the queue grows.

## The surprise: your Service balances connections, not requests

With two replicas behind a plain Kubernetes Service, one replica carried 96% of a test's load while the other GPU sat nearly idle.

A Service (kube-proxy) picks a backend pod when a TCP connection opens. Every request on that connection then goes to the same pod. My load generator, like most SDKs, gateways and agent frameworks, keeps HTTP connections alive, so whichever pod each connection landed on kept it.

| Shared-prefix stage, 16 users | Load on the busiest replica | TTFT p50 | Requests |
| --- | --- | --- | --- |
| 1 replica | - | 1.19 s | 128 |
| 2 replicas, keep-alive connections | 96% | 1.05 s | 134 |
| 2 replicas, new connection per request | 66% | 0.62 s | 165 |

Opening a new connection per request fixed most of it. Random per-request choice still leaves short-term imbalance, so the real fix for LLM serving is a request-level balancer that routes by in-flight requests or queue depth: Envoy's least-request policy, vLLM's router, or the Kubernetes Gateway API Inference Extension. Those can also route by prefix-cache contents, which matters once there are many distinct system prompts.

With a single shared prefix, random routing cost almost nothing in cache hits: each replica missed once, then hit 88% of the time.

## Gotchas that cost the most time

Each of these looked like something else at first.

| Symptom | Real cause | Fix |
| --- | --- | --- |
| vLLM restarts at startup with exit 1, no OOM kill | On DGX Spark's unified memory the page cache counts as used, and vLLM checks free memory against `--gpu-memory-utilization` at startup | Drop the page cache before every deploy |
| Second replica passes health checks but fails every request with HTTP 500 "invalid tiktoken vocab file" | gpt-oss downloads its tokenizer vocab on first request; the new node had none | Copy the cached vocab to every model node |
| node-exporter target down, scrapes time out | The `cpufreq` collector stalls on GB10 | `--no-collector.cpufreq` |
| Load test reported OK with 384 of 696 requests failed | The broken replica failed fast, so errors looked like throughput | Warn when more than 1% of requests fail |
| Scaling to 2 replicas silently went back to 1 | Re-applying the manifest reset `replicas` | Template the count; check n of n ready |

The pattern behind most of them: a check that passes is not the same as a system that works. Health endpoints, rollout status and "0 warnings" all lied at least once. The checks that never lied were an end-to-end request and the server's own counters.

## Takeaways for production inference

1. Know your ceiling before users find it. One replica of a 120B model on a DGX Spark serves about 16 interactive users; beyond that, latency, not errors, tells you.
2. Scale out before you scale up. If the model fits on one node, a replica per node doubled throughput and turned a three-minute recovery into one second.
3. Balance requests, not connections. A plain Kubernetes Service can hide your second GPU behind keep-alive connections.
4. Keep system prompts stable. Prefix caching made the first token 4.6x faster for free.
5. Trust server counters and power draw, not GPU "busy" or client-side throughput.

Everything here, from the k3s install to the load profiles and the analyzer, is in the [desk-side-ai-factory](https://github.com/arundvp/desk-side-ai-factory) repository, with the full measurements in `RESULTS.md`. Next on my list: cache-aware routing with many distinct prefixes, and a 30-minute soak test.

---

*Setup: 2x NVIDIA DGX Spark (GB10, 128 GB unified memory), ConnectX-7 200 Gb/s RoCEv2, k3s v1.36, NVIDIA GPU Operator v26.7, vLLM container `nvcr.io/nvidia/vllm:26.09-py3`, `openai/gpt-oss-120b` (MXFP4), November 2026.*
