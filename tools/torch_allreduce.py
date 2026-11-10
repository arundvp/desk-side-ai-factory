#!/usr/bin/env python3
"""torch_allreduce.py — PyTorch distributed all_reduce across the two Sparks (NCCL over RoCE).

Launched by torchrun on each Spark (one process per node). Verifies the result, then sweeps
message sizes and prints algorithm and bus bandwidth. Rank 0 ends with an ALLREDUCE_OK line.

Environment: AR_MIN_MB (default 1), AR_MAX_MB (default 2048), AR_ITERS (default 20)
"""
import os
import sys
import time

import torch
import torch.distributed as dist


def main():
    dist.init_process_group("nccl")
    rank, world = dist.get_rank(), dist.get_world_size()
    local = int(os.environ.get("LOCAL_RANK", "0"))
    torch.cuda.set_device(local)
    dev = torch.device("cuda", local)

    # correctness: every rank contributes rank+1, the sum is world*(world+1)/2
    x = torch.full((1024,), float(rank + 1), device=dev)
    dist.all_reduce(x)
    want = world * (world + 1) / 2
    ok = bool(torch.allclose(x, torch.full_like(x, want)))
    if rank == 0:
        print(f"world={world} correctness={'ok' if ok else 'WRONG'} (got {x[0].item()}, want {want})", flush=True)
    if not ok:
        dist.destroy_process_group()
        return 1

    mb = int(os.environ.get("AR_MIN_MB", "1"))
    max_mb = int(os.environ.get("AR_MAX_MB", "2048"))
    iters = int(os.environ.get("AR_ITERS", "20"))
    best = 0.0
    if rank == 0:
        print(f"{'size':>10} {'time_ms':>10} {'algbw_GB/s':>11} {'busbw_GB/s':>11}", flush=True)
    while mb <= max_mb:
        n = mb * 2**20 // 4
        t = torch.ones(n, dtype=torch.float32, device=dev)
        for _ in range(5):
            dist.all_reduce(t)
        torch.cuda.synchronize()
        dist.barrier()
        t0 = time.perf_counter()
        for _ in range(iters):
            dist.all_reduce(t)
        torch.cuda.synchronize()
        dt = (time.perf_counter() - t0) / iters
        algbw = (n * 4) / dt / 1e9
        busbw = algbw * 2 * (world - 1) / world
        best = max(best, busbw)
        if rank == 0:
            print(f"{mb:>8}MB {dt*1e3:>10.2f} {algbw:>11.2f} {busbw:>11.2f}", flush=True)
        del t
        mb *= 2
    if rank == 0:
        print(f"ALLREDUCE_OK peak_busbw_GBps={best:.2f}", flush=True)
    dist.destroy_process_group()
    return 0


if __name__ == "__main__":
    sys.exit(main())
