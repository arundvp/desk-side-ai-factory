# Offline checks for the desk-side-ai-factory bundle (no cluster or GPU needed).
# Needs: python3 with aiohttp, promql-parser, pyyaml, jsonschema, kubernetes-validate,
#        prometheus_client, matplotlib (optional); shellcheck; yamllint.
.PHONY: test lint render validate e2e dashboard
test: lint validate e2e

lint:
	shellcheck -x scripts/*.sh tests/*.sh
	python3 -m py_compile tools/*.py tests/*.py hack/*.py

dashboard:
	python3 hack/make_dashboard.py > k8s/monitoring/dashboards/spark-inference-lab.json

render:
	bash tests/render_all.sh

validate: render
	for f in tests/out/*.yaml; do kubernetes-validate -k 1.36.0 --strict $$f || exit 1; done 2>&1 | grep -v 'kind ServiceMonitor\|kind PodMonitor\|kind PrometheusRule' ; true
	python3 tests/validate_crs.py $${KPS_CRDS:?set KPS_CRDS to kube-prometheus-stack charts/crds/crds} tests/out/servicemonitors.yaml tests/out/prometheusrules.yaml
	python3 tests/check_promql.py .

e2e:
	bash tests/run_e2e.sh
