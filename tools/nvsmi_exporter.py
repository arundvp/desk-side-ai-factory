#!/usr/bin/env python3
"""nvsmi_exporter.py — Prometheus exporter for GPU telemetry read through nvidia-smi.

Why it exists: NVIDIA does not support DCGM on GB10 (DGX Spark), so DCGM Exporter may
return some fields, none, or fail to start. nvidia-smi is NVIDIA's suggested tool there.
This exporter turns its query output into Prometheus metrics so dashboards always have
GPU utilization, power, temperature and clocks, whatever DCGM does.

Standard library only. Configure with environment variables:
  NVSMI_PORT      listen port (default 9401)
  NVSMI_INTERVAL  seconds between nvidia-smi calls (default 5)
  NVSMI_BIN       nvidia-smi path (default: found on PATH)
"""
import http.server
import os
import re
import shutil
import socketserver
import subprocess
import threading
import time

PORT = int(os.environ.get("NVSMI_PORT", "9401"))
INTERVAL = float(os.environ.get("NVSMI_INTERVAL", "5"))
NVSMI = os.environ.get("NVSMI_BIN") or shutil.which("nvidia-smi") or "nvidia-smi"

# nvidia-smi field -> (metric name, help, parser)
def _num(v):
    return float(v)

def _hex(v):
    return float(int(v, 16))

def _pstate(v):
    m = re.fullmatch(r"P(\d+)", v.strip())
    if not m:
        raise ValueError(v)
    return float(m.group(1))

CANDIDATES = [
    ("utilization.gpu", "spark_nvsmi_gpu_utilization_percent", "GPU utilization (percent of time a kernel ran)", _num),
    ("utilization.memory", "spark_nvsmi_memory_utilization_percent", "Memory controller utilization (percent)", _num),
    ("power.draw", "spark_nvsmi_power_draw_watts", "GPU power draw (W)", _num),
    ("power.limit", "spark_nvsmi_power_limit_watts", "GPU power limit (W)", _num),
    ("temperature.gpu", "spark_nvsmi_temperature_gpu_celsius", "GPU temperature (C)", _num),
    ("clocks.sm", "spark_nvsmi_clocks_sm_mhz", "SM clock (MHz)", _num),
    ("clocks.max.sm", "spark_nvsmi_clocks_max_sm_mhz", "Maximum SM clock (MHz)", _num),
    ("clocks.mem", "spark_nvsmi_clocks_mem_mhz", "Memory clock (MHz)", _num),
    ("clocks_event_reasons.active", "spark_nvsmi_clocks_event_reasons_active",
     "Bitmask of active clock event (throttle) reasons; 0 = none", _hex),
    ("pstate", "spark_nvsmi_pstate", "Performance state (0 = P0, fastest)", _pstate),
    ("memory.used", "spark_nvsmi_memory_used_mib", "Framebuffer memory used (MiB); N/A on unified memory", _num),
]
NA = re.compile(r"^\[?(N/A|Not Supported|Unknown Error|GPU is lost)\]?$", re.I)

_lock = threading.Lock()
_page = "# exporter starting\n"


def _run(args, timeout=10):
    return subprocess.run([NVSMI, *args], capture_output=True, text=True, timeout=timeout)


def probe_fields():
    """Keep only the fields this driver's nvidia-smi accepts (an unknown field fails the whole query)."""
    ok = []
    for field, metric, helptext, parse in CANDIDATES:
        try:
            r = _run([f"--query-gpu={field}", "--format=csv,noheader,nounits"])
        except (OSError, subprocess.TimeoutExpired):
            return []
        if r.returncode == 0:
            ok.append((field, metric, helptext, parse))
    return ok


def esc(v):
    return str(v).replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


def collect(fields):
    lines = []
    t0 = time.monotonic()
    up = 0
    values = {}
    supported = {}
    gpus = []
    try:
        query = ",".join(["index", "uuid", "name"] + [f[0] for f in fields])
        r = _run([f"--query-gpu={query}", "--format=csv,noheader,nounits"])
        if r.returncode == 0:
            up = 1
            for row in r.stdout.strip().splitlines():
                cols = [c.strip() for c in row.split(",")]
                if len(cols) < 3 + len(fields):
                    continue
                idx, uuid, name = cols[:3]
                gpus.append((idx, uuid, name))
                for (field, metric, _h, parse), raw in zip(fields, cols[3:]):
                    if NA.match(raw) or raw == "":
                        supported[(field, idx)] = 0
                        continue
                    try:
                        values.setdefault(metric, []).append(((idx, uuid, name), parse(raw)))
                        supported[(field, idx)] = 1
                    except ValueError:
                        supported[(field, idx)] = 0
    except (OSError, subprocess.TimeoutExpired):
        up = 0

    procs = {}
    if up:
        try:
            r = _run(["--query-compute-apps=pid,process_name,used_memory", "--format=csv,noheader,nounits"])
            if r.returncode == 0:
                for row in r.stdout.strip().splitlines():
                    cols = [c.strip() for c in row.split(",")]
                    if len(cols) != 3 or NA.match(cols[2]):
                        continue
                    pname = os.path.basename(cols[1]) or "unknown"
                    try:
                        procs[pname] = procs.get(pname, 0.0) + float(cols[2])
                    except ValueError:
                        pass
        except (OSError, subprocess.TimeoutExpired):
            pass

    lines.append("# HELP spark_nvsmi_up 1 if the last nvidia-smi query succeeded")
    lines.append("# TYPE spark_nvsmi_up gauge")
    lines.append(f"spark_nvsmi_up {up}")
    for field, metric, helptext, _p in fields:
        if metric not in values:
            continue
        lines.append(f"# HELP {metric} {helptext}")
        lines.append(f"# TYPE {metric} gauge")
        for (idx, uuid, name), v in values[metric]:
            lines.append(f'{metric}{{gpu="{esc(idx)}",uuid="{esc(uuid)}",model="{esc(name)}"}} {v:g}')
    lines.append("# HELP spark_nvsmi_field_supported 1 if nvidia-smi returned a value for the field")
    lines.append("# TYPE spark_nvsmi_field_supported gauge")
    for (field, idx), s in sorted(supported.items()):
        lines.append(f'spark_nvsmi_field_supported{{field="{esc(field)}",gpu="{esc(idx)}"}} {s}')
    if procs:
        lines.append("# HELP spark_nvsmi_process_used_memory_mib GPU memory used by compute processes, summed by process name")
        lines.append("# TYPE spark_nvsmi_process_used_memory_mib gauge")
        for pname, mib in sorted(procs.items()):
            lines.append(f'spark_nvsmi_process_used_memory_mib{{process="{esc(pname)}"}} {mib:g}')
    lines.append("# HELP spark_nvsmi_scrape_duration_seconds Time the nvidia-smi calls took")
    lines.append("# TYPE spark_nvsmi_scrape_duration_seconds gauge")
    lines.append(f"spark_nvsmi_scrape_duration_seconds {time.monotonic() - t0:.3f}")
    return "\n".join(lines) + "\n"


def poller():
    global _page
    fields = probe_fields()
    reprobe_at = time.monotonic() + 300
    while True:
        if not fields or time.monotonic() > reprobe_at:
            fields = probe_fields() or fields
            reprobe_at = time.monotonic() + 300
        page = collect(fields)
        with _lock:
            _page = page
        time.sleep(INTERVAL)


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.split("?")[0] not in ("/metrics", "/"):
            self.send_error(404)
            return
        with _lock:
            body = _page.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


class Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    threading.Thread(target=poller, daemon=True).start()
    print(f"nvsmi_exporter: {NVSMI}, every {INTERVAL:g}s, listening on :{PORT}", flush=True)
    Server(("", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
