#!/usr/bin/env python3
"""Check that every key in our Helm values exists in the chart's default values.
usage: check_values.py <ours.yaml> <chart-values.yaml> [<subchart-prefix>=<subchart-values.yaml> ...]
Free-form maps (empty {} / lists in the chart defaults, or *.data / *.ini / relabelings) are not descended."""
import sys
import yaml

FREEFORM = {"data", "grafana.ini", "relabelings", "nodeSelector", "resources", "storageSpec", "env",
            "annotations", "labels", "podLabels"}

def walk(ours, ref, path, problems):
    if not isinstance(ours, dict):
        return
    for k, v in ours.items():
        p = f"{path}.{k}" if path else k
        if not isinstance(ref, dict) or k not in ref:
            problems.append(p)
            continue
        if k in FREEFORM or ref[k] in ({}, [], None) or not isinstance(ref[k], dict):
            continue
        walk(v, ref[k], p, problems)

ours = yaml.safe_load(open(sys.argv[1]))
ref = yaml.safe_load(open(sys.argv[2]))
subs = dict(a.split("=", 1) for a in sys.argv[3:])
def merge(a, b):
    out = dict(a or {})
    for k, v in (b or {}).items():
        out[k] = merge(out.get(k), v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out

problems = []
for k, v in ours.items():
    if k in subs:
        # subchart keys may be defined by the subchart itself or by the parent chart's overrides
        walk(v, merge(yaml.safe_load(open(subs[k])), ref.get(k)), k, problems)
    else:
        walk({k: v}, ref, "", problems)
for p in problems:
    print(f"UNKNOWN KEY  {p}")
print(f"{sys.argv[1]}: {'OK' if not problems else str(len(problems)) + ' unknown keys'}")
sys.exit(1 if problems else 0)
