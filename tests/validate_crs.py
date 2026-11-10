#!/usr/bin/env python3
"""Validate ServiceMonitor / PodMonitor / PrometheusRule objects against the CRD schemas
shipped with kube-prometheus-stack (path given as argv[1])."""
import glob, os, sys
import jsonschema, yaml

# CRD enums contain a bare "=" which PyYAML reads as the !!value tag
yaml.SafeLoader.add_constructor("tag:yaml.org,2002:value", lambda loader, node: loader.construct_scalar(node))

crd_dir = sys.argv[1]
schemas = {}
for f in glob.glob(os.path.join(crd_dir, "crd-*.yaml")):
    for doc in yaml.safe_load_all(open(f)):
        if not doc or doc.get("kind") != "CustomResourceDefinition":
            continue
        kind = doc["spec"]["names"]["kind"]
        for v in doc["spec"]["versions"]:
            schemas[(kind, f'{doc["spec"]["group"]}/{v["name"]}')] = v["schema"]["openAPIV3Schema"]

def strict(s):
    # treat unknown fields as errors, like kubectl's server-side field validation
    if isinstance(s, dict):
        if s.get("type") == "object" and "properties" in s and "additionalProperties" not in s \
                and not s.get("x-kubernetes-preserve-unknown-fields"):
            s = {**s, "additionalProperties": False}
        return {k: strict(v) for k, v in s.items()}
    if isinstance(s, list):
        return [strict(x) for x in s]
    return s

bad = 0
for path in sys.argv[2:]:
    for doc in yaml.safe_load_all(open(path)):
        if not doc:
            continue
        key = (doc["kind"], doc["apiVersion"])
        if key not in schemas:
            continue
        sch = strict(schemas[key])
        sch["properties"]["metadata"] = {"type": "object"}
        errs = sorted(jsonschema.Draft7Validator(sch).iter_errors(doc), key=lambda e: e.path)
        for e in errs:
            bad += 1
            print(f"FAIL {path} {doc['kind']}/{doc['metadata']['name']}: {list(e.path)}: {e.message}")
        if not errs:
            print(f"ok   {path} {doc['kind']}/{doc['metadata']['name']}")
sys.exit(1 if bad else 0)
