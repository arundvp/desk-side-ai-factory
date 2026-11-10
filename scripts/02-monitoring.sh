#!/usr/bin/env bash
# 02-monitoring.sh — Prometheus, Grafana, Alertmanager, node-exporter (kube-prometheus-stack),
# plus the lab's scrape configs, recording/alert rules and Grafana dashboard.
# Installed before the GPU Operator so its DCGM ServiceMonitor has a Prometheus to land in.
source "$(dirname "$0")/lib.sh"
need helm kubectl jq openssl
M="$LAB_ROOT/k8s/monitoring"

ensure_ns "$NS_MON" "$NS_LLM" "$NS_LAB"

# Grafana admin credentials live in a Secret, not in Helm values
pw_file="$LAB_ROOT/.grafana-admin-password"
if ! kubectl -n "$NS_MON" get secret grafana-admin >/dev/null 2>&1; then
  pw="${GRAFANA_ADMIN_PASSWORD:-$(openssl rand -base64 18 | tr -d '/+=')}"
  kubectl -n "$NS_MON" create secret generic grafana-admin \
    --from-literal=admin-user=admin --from-literal=admin-password="$pw" >/dev/null
  umask 077; printf '%s\n' "$pw" > "$pw_file"
  info "Grafana admin password saved to $pw_file"
fi

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null 2>&1 || true
helm repo update prometheus-community >/dev/null

info "installing kube-prometheus-stack ${KPS_VERSION} (takes 2–5 min)"
helm upgrade --install kps prometheus-community/kube-prometheus-stack \
  --version "$KPS_VERSION" -n "$NS_MON" \
  -f "$M/kube-prometheus-stack-values.yaml" \
  --wait --timeout 15m

kubectl apply -f "$M/servicemonitors.yaml" >/dev/null
kubectl apply -f "$M/prometheusrules.yaml" >/dev/null
kubectl -n "$NS_MON" create configmap spark-lab-dashboards \
  --from-file="$M/dashboards/" --dry-run=client -o yaml \
  | kubectl label --local -f - grafana_dashboard=1 -o yaml \
  | kubectl apply -f - >/dev/null
tools_configmap "$NS_MON"

# --- checks (through the API-server service proxy; no port-forward needed) ------------
prom() { kubectl get --raw "/api/v1/namespaces/${NS_MON}/services/http:kps-prometheus:9090/proxy$1"; }
graf() { kubectl get --raw "/api/v1/namespaces/${NS_MON}/services/http:kps-grafana:80/proxy$1"; }

kubectl -n "$NS_MON" get pods -o wide
wait_for "Prometheus ready" 300 prom /-/ready && pass "Prometheus ready" || fail "Prometheus not ready"
wait_for "node-exporter targets on both nodes" 180 bash -c \
  "[[ \$(kubectl get --raw '/api/v1/namespaces/${NS_MON}/services/http:kps-prometheus:9090/proxy/api/v1/query?query=count(up%7Bjob%3D%22node-exporter%22%7D%3D%3D1)' | jq -r '.data.result[0].value[1] // 0') -ge 2 ]]" \
  && pass "node-exporter up on 2 nodes" || fail "node-exporter not up on both nodes"
nodes_labeled="$(prom '/api/v1/query?query=count(count%20by%20(node)%20(node_memory_MemAvailable_bytes))' | jq -r '.data.result[0].value[1] // 0')"
[[ "$nodes_labeled" -ge 2 ]] && pass "node label present on node-exporter series ($nodes_labeled nodes)" \
  || fail "node-exporter series lack the node label (check prometheus-node-exporter relabelings)"
wait_for "spark-lab rules loaded" 120 bash -c \
  "kubectl get --raw '/api/v1/namespaces/${NS_MON}/services/http:kps-prometheus:9090/proxy/api/v1/rules' | jq -e '[.data.groups[].name] | index(\"spark-lab.gpu\")' " \
  && pass "spark-lab recording and alert rules loaded" || fail "spark-lab rules not loaded"
graf /api/health | jq -e '.database=="ok"' >/dev/null && pass "Grafana healthy" || fail "Grafana not healthy"

cat <<EOF

Grafana:    kubectl -n ${NS_MON} port-forward svc/kps-grafana 3000:80        → http://localhost:3000  (admin / see $pw_file)
Prometheus: kubectl -n ${NS_MON} port-forward svc/kps-prometheus 9090:9090   → http://localhost:9090
From your workstation, tunnel first: ssh -L 3000:127.0.0.1:3000 -L 9090:127.0.0.1:9090 ${SSH_USER}@${NODE1_MGMT}
Or run scripts/ui.sh on Spark 1 to start all port-forwards at once.
EOF
summary "monitoring"
