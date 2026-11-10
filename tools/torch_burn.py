#!/usr/bin/env python3
"""torch_burn.py — controllable GPU load with PyTorch, to watch DCGM / nvidia-smi respond.

Runs bf16 (or fp16/fp32) matrix multiplies on the GPU for a fixed time at a chosen duty
cycle, optionally holding extra memory to create unified-memory pressure on GB10.
Prints achieved TFLOP/s every REPORT_EVERY seconds and a BURN_DONE summary line.

Environment:
  BURN_SECONDS   total run time (default 300)
  BURN_N         square matrix size (default 8192)
  BURN_DTYPE     bf16 | fp16 | fp32 (default bf16)
  BURN_DUTY      fraction of each second spent computing, 0.05..1.0 (default 1.0)
  BURN_ALLOC_GB  extra GiB to allocate and hold for the whole run (default 0)
  REPORT_EVERY   seconds between progress lines (default 10)
"""
import os
import sys
import time

import torch

SECONDS = float(os.environ.get("BURN_SECONDS", "300"))
N = int(os.environ.get("BURN_N", "8192"))
DTYPE = {"bf16": torch.bfloat16, "fp16": torch.float16, "fp32": torch.float32}[os.environ.get("BURN_DTYPE", "bf16")]
DUTY = min(1.0, max(0.05, float(os.environ.get("BURN_DUTY", "1.0"))))
ALLOC_GB = float(os.environ.get("BURN_ALLOC_GB", "0"))
REPORT = float(os.environ.get("REPORT_EVERY", "10"))


def main():
    if not torch.cuda.is_available():
        print("BURN_FAIL CUDA not available", flush=True)
        return 1
    dev = torch.device("cuda")
    props = torch.cuda.get_device_properties(dev)
    print(f"device {props.name} sm_{props.major}{props.minor} n={N} dtype={DTYPE} duty={DUTY} "
          f"alloc={ALLOC_GB}GiB seconds={SECONDS:g}", flush=True)

    hold = None
    if ALLOC_GB > 0:
        hold = torch.empty(int(ALLOC_GB * 2**30), dtype=torch.uint8, device=dev)
        hold.fill_(1)  # touch every page so the memory is really committed
        torch.cuda.synchronize()
        free, total = torch.cuda.mem_get_info()
        print(f"holding {ALLOC_GB:g} GiB; cudaMemGetInfo free={free/2**30:.1f} GiB of {total/2**30:.1f} GiB", flush=True)

    a = torch.randn(N, N, device=dev, dtype=DTYPE)
    b = torch.randn(N, N, device=dev, dtype=DTYPE)
    flop_per_mm = 2.0 * N ** 3

    # calibrate: how many matmuls fit in ~50 ms
    torch.cuda.synchronize()
    t = time.perf_counter()
    for _ in range(5):
        a @ b
    torch.cuda.synchronize()
    per_mm = (time.perf_counter() - t) / 5
    batch = max(1, int(0.05 / per_mm))

    start = time.perf_counter()
    last = start
    mm_total = 0
    mm_window = 0
    busy_window = 0.0
    while True:
        now = time.perf_counter()
        if now - start >= SECONDS:
            break
        sec_start = now
        busy = 0.0
        # compute for DUTY of this one-second slot, then idle for the rest
        while busy < DUTY and time.perf_counter() - start < SECONDS:
            t0 = time.perf_counter()
            for _ in range(batch):
                a @ b
            torch.cuda.synchronize()
            busy += time.perf_counter() - t0
            mm_total += batch
            mm_window += batch
        busy_window += busy
        rest = 1.0 - (time.perf_counter() - sec_start)
        if DUTY < 1.0 and rest > 0:
            time.sleep(rest)
        now = time.perf_counter()
        if now - last >= REPORT:
            tflops = mm_window * flop_per_mm / (now - last) / 1e12
            print(f"t={now - start:6.0f}s  {tflops:7.1f} TFLOP/s  busy={busy_window / (now - last):.0%}", flush=True)
            last, mm_window, busy_window = now, 0, 0.0

    elapsed = time.perf_counter() - start
    avg = mm_total * flop_per_mm / elapsed / 1e12
    print(f"BURN_DONE seconds={elapsed:.0f} avg_tflops={avg:.1f} matmuls={mm_total}", flush=True)
    del hold
    return 0


if __name__ == "__main__":
    sys.exit(main())
