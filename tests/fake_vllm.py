#!/usr/bin/env python3
"""fake_vllm.py — a tiny stand-in for the vLLM OpenAI server, for offline tests of loadgen.py.

Streams chat completions in vLLM's SSE format (reasoning deltas, then content, then a
finish chunk and a usage chunk), queues requests above MAX_SEQS like the vLLM scheduler,
slows decode as concurrency rises, and serves /health and /metrics.
usage: fake_vllm.py <port>
"""
import asyncio
import json
import sys
import time

from aiohttp import web

MAX_SEQS = 64
state = {"running": 0, "waiting": 0, "gen_tokens": 0, "prompt_tokens": 0, "ok": 0}
slots = asyncio.Semaphore(MAX_SEQS)


async def chat(request):
    body = await request.json()
    if body.get("model") != "gpt-oss-120b":
        return web.json_response({"error": {"message": "model not found"}}, status=404)
    max_tokens = int(body.get("max_tokens", 64))
    prompt = sum(len(m["content"].split()) for m in body["messages"])
    stream = body.get("stream", False)
    state["waiting"] += 1
    async with slots:
        state["waiting"] -= 1
        state["running"] += 1
        try:
            resp = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
            await resp.prepare(request)
            await asyncio.sleep(0.02 + prompt / 200000)          # prefill
            n_out = min(max_tokens, 40)
            itl = 0.002 + 0.0004 * state["running"]              # decode slows with batch size
            for i in range(n_out):
                key = "reasoning_content" if i < 5 else "content"
                chunk = {"id": "x", "object": "chat.completion.chunk", "model": "gpt-oss-120b",
                         "choices": [{"index": 0, "delta": {key: f"t{i} "}, "finish_reason": None}]}
                await resp.write(f"data: {json.dumps(chunk)}\n\n".encode())
                state["gen_tokens"] += 1
                await asyncio.sleep(itl)
            fin = {"choices": [{"index": 0, "delta": {}, "finish_reason": "length" if n_out == max_tokens else "stop"}]}
            await resp.write(f"data: {json.dumps(fin)}\n\n".encode())
            usage = {"choices": [], "usage": {"prompt_tokens": prompt, "completion_tokens": n_out,
                                              "total_tokens": prompt + n_out}}
            await resp.write(f"data: {json.dumps(usage)}\n\n".encode())
            await resp.write(b"data: [DONE]\n\n")
            state["prompt_tokens"] += prompt
            state["ok"] += 1
            return resp
        finally:
            state["running"] -= 1


async def health(_request):
    return web.Response(text="")


async def models(_request):
    return web.json_response({"object": "list", "data": [{"id": "gpt-oss-120b", "object": "model"}]})


async def metrics(_request):
    lbl = 'model_name="gpt-oss-120b",engine="0"'
    text = "\n".join([
        f"vllm:num_requests_running{{{lbl}}} {state['running']}",
        f"vllm:num_requests_waiting{{{lbl}}} {state['waiting']}",
        f"vllm:generation_tokens_total{{{lbl}}} {state['gen_tokens']}",
        f"vllm:prompt_tokens_total{{{lbl}}} {state['prompt_tokens']}",
        f"vllm:kv_cache_usage_perc{{{lbl}}} {min(1.0, state['running'] / 80):.3f}",
    ]) + "\n"
    return web.Response(text=text)


def main():
    app = web.Application()
    app.router.add_post("/v1/chat/completions", chat)
    app.router.add_get("/health", health)
    app.router.add_get("/v1/models", models)
    app.router.add_get("/metrics", metrics)
    web.run_app(app, host="127.0.0.1", port=int(sys.argv[1]), print=None)


if __name__ == "__main__":
    main()
