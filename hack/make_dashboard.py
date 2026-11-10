#!/usr/bin/env python3
"""Generate k8s/monitoring/dashboards/spark-inference-lab.json (Grafana dashboard).

Kept as code so panel layout and PromQL stay consistent; tests/ checks every expression.
usage: python3 hack/make_dashboard.py > k8s/monitoring/dashboards/spark-inference-lab.json
"""
import json

DS = {"type": "prometheus", "uid": "prometheus"}
_id = [0]
_y = [0]


def nid():
    _id[0] += 1
    return _id[0]


def target(expr, legend, ref):
    return {"datasource": DS, "expr": expr, "legendFormat": legend, "refId": ref, "range": True}


def ts(title, exprs, unit="short", w=8, h=8, x=0, desc="", stack=False, minv=None, maxv=None, thresholds=None):
    fc = {
        "defaults": {
            "unit": unit,
            "custom": {
                "drawStyle": "line",
                "lineWidth": 2,
                "fillOpacity": 10 if not stack else 40,
                "showPoints": "never",
                "spanNulls": True,
                "stacking": {"mode": "normal" if stack else "none", "group": "A"},
            },
            "color": {"mode": "palette-classic"},
        },
        "overrides": [],
    }
    if minv is not None:
        fc["defaults"]["min"] = minv
    if maxv is not None:
        fc["defaults"]["max"] = maxv
    if thresholds:
        fc["defaults"]["thresholds"] = {"mode": "absolute", "steps": thresholds}
        fc["defaults"]["custom"]["thresholdsStyle"] = {"mode": "dashed"}
    return {
        "id": nid(),
        "type": "timeseries",
        "title": title,
        "description": desc,
        "datasource": DS,
        "gridPos": {"x": x, "y": _y[0], "w": w, "h": h},
        "fieldConfig": fc,
        "options": {
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
            "tooltip": {"mode": "multi", "sort": "desc"},
        },
        "targets": [target(e, l, chr(65 + i)) for i, (e, l) in enumerate(exprs)],
    }


def stat(title, expr, legend, unit="short", w=8, h=8, x=0, desc=""):
    return {
        "id": nid(),
        "type": "stat",
        "title": title,
        "description": desc,
        "datasource": DS,
        "gridPos": {"x": x, "y": _y[0], "w": w, "h": h},
        "fieldConfig": {"defaults": {"unit": unit, "color": {"mode": "thresholds"},
                                     "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}]}},
                        "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "orientation": "auto", "textMode": "value_and_name", "colorMode": "value", "graphMode": "none"},
        "targets": [target(expr, legend, "A")],
    }


def row(title):
    p = {"id": nid(), "type": "row", "title": title, "collapsed": False,
         "gridPos": {"x": 0, "y": _y[0], "w": 24, "h": 1}, "panels": []}
    _y[0] += 1
    return p


def line(*panels, h=8):
    for p in panels:
        p["gridPos"]["y"] = _y[0]
    _y[0] += h
    return list(panels)


q = lambda quant, metric: (  # noqa: E731
    f'histogram_quantile({quant}, sum by (node, le) (rate({metric}_bucket[$__rate_interval])))'
)

panels = []
panels.append(row("Load profile"))
panels += line(
    ts("Load generator: target concurrency vs requests in flight",
       [("max(loadgen_stage_concurrency)", "target concurrency (stage)"),
        ("sum(loadgen_inflight)", "client in flight"),
        ("sum by (node) (vllm:num_requests_running)", "vLLM running {{node}}"),
        ("sum by (node) (vllm:num_requests_waiting)", "vLLM waiting {{node}}")],
       w=24, h=8,
       desc="Stage target from the load generator next to what vLLM is actually running. "
            "Waiting > 0 means the scheduler is full (max-num-seqs or KV cache)."),
    h=8)

panels.append(row("Inference (vLLM)"))
panels += line(
    ts("Token throughput", [
        ("spark:vllm_generation_tokens:rate30s", "generated tok/s {{node}}"),
        ("spark:vllm_prompt_tokens:rate30s", "prompt tok/s {{node}}")], unit="short", w=8, x=0,
       desc="Output and prefill tokens per second, 30 s rate."),
    ts("Time to first token", [
        (q(0.5, "vllm:time_to_first_token_seconds"), "p50 {{node}}"),
        (q(0.95, "vllm:time_to_first_token_seconds"), "p95 {{node}}"),
        (q(0.99, "vllm:time_to_first_token_seconds"), "p99 {{node}}")], unit="s", w=8, x=8,
       desc="Queue wait + prefill. Grows with prompt length and with waiting requests."),
    ts("Inter-token latency", [
        (q(0.5, "vllm:inter_token_latency_seconds"), "p50 {{node}}"),
        (q(0.95, "vllm:inter_token_latency_seconds"), "p95 {{node}}"),
        (q(0.99, "vllm:inter_token_latency_seconds"), "p99 {{node}}")], unit="s", w=8, x=16,
       desc="Decode step time per user. 1/ITL = tokens/s each user sees."))
panels += line(
    ts("End-to-end and queue time", [
        (q(0.5, "vllm:e2e_request_latency_seconds"), "e2e p50 {{node}}"),
        (q(0.95, "vllm:e2e_request_latency_seconds"), "e2e p95 {{node}}"),
        (q(0.95, "vllm:request_queue_time_seconds"), "queue p95 {{node}}")], unit="s", w=8, x=0),
    ts("KV cache usage", [
        ("max by (node) (vllm:kv_cache_usage_perc)", "{{node}}")], unit="percentunit", w=8, x=8,
       minv=0, maxv=1, thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 0.9}],
       desc="Fraction of the KV block pool in use. Near 1.0 → preemptions."),
    ts("Requests finished and preemptions", [
        ("sum by (node, finished_reason) (rate(vllm:request_success_total[$__rate_interval]))", "{{finished_reason}} {{node}}"),
        ("sum by (node) (rate(vllm:num_preemptions_total[$__rate_interval]))", "preemptions/s {{node}}")],
       unit="reqps", w=8, x=16))
panels += line(
    ts("Prefix cache hit ratio", [
        ("sum by (node) (rate(vllm:prefix_cache_hits_total[$__rate_interval])) / "
         "clamp_min(sum by (node) (rate(vllm:prefix_cache_queries_total[$__rate_interval])), 1)", "{{node}}")],
       unit="percentunit", w=8, x=0, minv=0, maxv=1),
    ts("Client-side errors (load generator)", [
        ('sum by (stage) (rate(loadgen_requests_total{status!="ok"}[$__rate_interval]))', "{{stage}}")],
       unit="reqps", w=8, x=8),
    ts("Client-side output tokens/s (load generator)", [
        ("sum(rate(loadgen_output_tokens_total[$__rate_interval]))", "client tok/s")], w=8, x=16,
       desc="Should match vLLM generated tok/s; a gap means client or network overhead."))

panels.append(row("GPU — DCGM with nvidia-smi fallback (GB10)"))
panels += line(
    ts("GPU utilization", [("spark:gpu_utilization:percent", "{{node}}")], unit="percent", w=8, x=0,
       minv=0, maxv=100),
    ts("GPU power", [("spark:gpu_power:watts", "{{node}}")], unit="watt", w=8, x=8),
    ts("GPU temperature", [("spark:gpu_temperature:celsius", "{{node}}")], unit="celsius", w=8, x=16,
       thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 85}]))
panels += line(
    ts("SM clock vs max", [
        ("spark:gpu_sm_clock:mhz", "SM clock {{node}}"),
        ("spark:gpu_sm_clock_max:mhz", "max {{node}}")], unit="rotmhz", w=8, x=0,
       desc="A clock well under max while busy = power or thermal capping."),
    ts("Utilization: DCGM vs nvidia-smi", [
        ("max by (node) (DCGM_FI_DEV_GPU_UTIL)", "DCGM {{node}}"),
        ("max by (node) (spark_nvsmi_gpu_utilization_percent)", "nvidia-smi {{node}}")],
       unit="percent", w=8, x=8,
       desc="Both sources side by side. A missing or flat DCGM line is the GB10 support gap."),
    ts("Clock event reasons (bitmask)", [
        ("spark:gpu_clock_event_reasons:bitmask", "{{node}}")], w=8, x=16,
       desc="0 = no throttling. Nonzero bits: see nvidia-smi -q -d PERFORMANCE."))
panels += line(
    ts("GPU memory by process (nvidia-smi)", [
        ("sum by (node, process) (spark_nvsmi_process_used_memory_mib) * 1024 * 1024", "{{process}} {{node}}")],
       unit="bytes", w=8, x=0, stack=True,
       desc="Unified memory held by CUDA processes, if the driver reports it."),
    stat("DCGM fields exporting", 'count by (node) ({__name__=~"DCGM_FI_.*"})', "{{node}}", w=8, x=8,
         desc="How many DCGM series each node returns. 0 or missing = DCGM unsupported on this GB10."),
    stat("nvidia-smi fields supported", "sum by (node) (spark_nvsmi_field_supported)", "{{node}}", w=8, x=16))

panels.append(row("Node — unified memory, CPU, fabric"))
panels += line(
    ts("Unified memory available", [
        ("spark:node_memory_available:bytes", "available {{node}}"),
        ("spark:node_memory_total:bytes", "total {{node}}")], unit="bytes", w=8, x=0,
       thresholds=[{"color": "red", "value": None}, {"color": "green", "value": 8589934592}],
       desc="CPU + GPU share one pool on GB10; this is the real memory headroom."),
    ts("CPU busy", [("spark:node_cpu_busy:ratio", "{{node}}")], unit="percentunit", w=8, x=8, minv=0, maxv=1),
    ts("RDMA throughput (ConnectX-7)", [
        ("spark:rdma_tx:bytes_per_second * 8", "tx {{device}} {{node}}"),
        ("spark:rdma_rx:bytes_per_second * 8", "rx {{device}} {{node}}")], unit="bps", w=8, x=16,
       desc="RoCE traffic per rail from node-exporter's infiniband collector (PyTorch all_reduce, 2-node vLLM)."))

dashboard = {
    "uid": "spark-inference-lab",
    "title": "Spark inference lab — vLLM, GPU, unified memory",
    "tags": ["spark-lab", "vllm", "gpu"],
    "timezone": "browser",
    "editable": True,
    "graphTooltip": 1,
    "refresh": "5s",
    "schemaVersion": 39,
    "version": 1,
    "time": {"from": "now-30m", "to": "now"},
    "annotations": {"list": [
        {"builtIn": 1, "datasource": {"type": "grafana", "uid": "-- Grafana --"}, "enable": True,
         "hide": True, "iconColor": "rgba(0, 211, 255, 1)", "name": "Annotations & Alerts", "type": "dashboard"},
        {"datasource": DS, "enable": True, "iconColor": "orange", "name": "Load stage change",
         "expr": "changes(max(loadgen_stage_index)[15s:5s]) > 0", "step": "5s",
         "titleFormat": "stage change", "textFormat": "load generator moved to the next stage"},
    ]},
    "templating": {"list": []},
    "panels": panels,
}

if __name__ == "__main__":
    print(json.dumps(dashboard, indent=2))
