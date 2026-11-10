#!/usr/bin/env python3
"""Parse every PromQL expression in the rules, the Grafana dashboard and the scripts."""
import json, re, sys, glob
import yaml
import promql_parser

ROOT = sys.argv[1] if len(sys.argv) > 1 else "."
exprs = []
rules = yaml.safe_load(open(f"{ROOT}/k8s/monitoring/prometheusrules.yaml"))
for g in rules["spec"]["groups"]:
    for r in g["rules"]:
        exprs.append((f"rule {r.get('record') or r.get('alert')}", r["expr"]))
dash = json.load(open(f"{ROOT}/k8s/monitoring/dashboards/spark-inference-lab.json"))
for p in dash["panels"]:
    for t in p.get("targets", []):
        exprs.append((f"panel '{p['title']}' {t['refId']}", t["expr"]))
for a in dash["annotations"]["list"]:
    if a.get("expr"):
        exprs.append((f"annotation {a['name']}", a["expr"]))
# literal queries in the shell scripts: prom_value/prom_query "<expr>" or '<expr>'
for f in glob.glob(f"{ROOT}/scripts/*.sh"):
    src = open(f).read()
    for m in re.finditer(r"prom_(?:value|query) (['\"])(.+?)\1", src):
        e = m.group(2)
        if e.startswith("$"):
            continue  # helper definitions in lib.sh
        e = e.replace('${expr}', 'spark:gpu_power:watts').replace('${w}', '180').replace('\\"', '"')
        e = e.replace('${node}', 'spark-1').replace('$node', 'spark-1')
        exprs.append((f"{f.split('/')[-1]}", e))

bad = 0
for where, e in exprs:
    e2 = e.replace("$__rate_interval", "1m")
    try:
        promql_parser.parse(e2)
    except Exception as ex:  # noqa: BLE001
        bad += 1
        print(f"FAIL {where}: {e2}\n     {ex}")
print(f"{len(exprs)} expressions parsed, {bad} failed")
sys.exit(1 if bad else 0)
