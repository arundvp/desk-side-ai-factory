#!/usr/bin/env bash
# netcheck.sh — diagnose the cross-node pod network (flannel VXLAN over the k3s node network).
# Run on Spark 1 after 01-k3s-install.sh. Read-only apart from a short-lived busybox pod.
# Each step isolates one layer, so the first FAIL says where the problem is:
#   host/VXLAN  ->  pod-to-pod (no DNS, no kube-proxy)  ->  service ClusterIP  ->  DNS name
source "$(dirname "$0")/lib.sh"
need kubectl ssh

host_facts='
  echo "host=$(hostname)"
  ip -d -br link show flannel.1 2>/dev/null | head -1 | sed "s/^/flannel1=/" || true
  ip -d link show flannel.1 2>/dev/null | grep -o "vxlan id [0-9]* local [0-9.]* dev [^ ]*" | sed "s/^/vxlan=/" || true
  echo "cni0=$(ip -br addr show cni0 2>/dev/null | awk "{print \$3}")"
  echo "routes=$(ip route | grep -c "flannel.1" )"
  if command -v ufw >/dev/null; then echo "ufw=$(sudo -n ufw status 2>/dev/null | head -1 || echo unknown)"; fi
  echo "fwd_policy=$(sudo -n iptables -S FORWARD 2>/dev/null | head -1 || echo unknown)"
  echo "ip_forward=$(cat /proc/sys/net/ipv4/ip_forward)"
'

info "host side (flannel.1 should exist on both, local = the node IP, dev = ${MGMT_IF})"
echo "--- ${NODE1_NAME}"; bash -c "$host_facts"
echo "--- ${NODE2_NAME}"; on_node2 "$host_facts" || fail "cannot SSH to ${NODE2_MGMT}"

for n in "$NODE1_NAME" "$NODE2_NAME"; do
  cidr="$(kubectl get node "$n" -o jsonpath='{.spec.podCIDR}')"
  pip="$(kubectl get node "$n" -o jsonpath='{.metadata.annotations.flannel\.alpha\.coreos\.com/public-ip}')"
  [[ -n "$cidr" && "$pip" == 10.* ]] && pass "$n podCIDR $cidr, flannel public-ip $pip" \
    || fail "$n flannel not initialised (podCIDR='$cidr' public-ip='$pip')"
done

dns_ip="$(kubectl -n kube-system get pod -l k8s-app=kube-dns -o jsonpath='{.items[0].status.podIP}')"
dns_node="$(kubectl -n kube-system get pod -l k8s-app=kube-dns -o jsonpath='{.items[0].spec.nodeName}')"
svc_ip="$(kubectl -n kube-system get svc kube-dns -o jsonpath='{.spec.clusterIP}')"
info "CoreDNS pod ${dns_ip} on ${dns_node}; kube-dns service ${svc_ip}"

kubectl delete pod netcheck --ignore-not-found --wait=true >/dev/null 2>&1
out="$(kubectl run netcheck --rm -i --restart=Never --image=busybox:1.37 --pod-running-timeout=3m \
  --overrides="{\"spec\":{\"nodeSelector\":{\"spark-lab/node\":\"${NODE2_NAME}\"}}}" \
  --command -- sh -c "
    echo podip=\$(hostname -i)
    wget -q -T 5 -O- http://${dns_ip}:9153/metrics >/dev/null 2>&1 && echo step_pod=ok || echo step_pod=fail
    wget -q -T 5 -O- http://${svc_ip}:9153/metrics >/dev/null 2>&1 && echo step_svc=ok || echo step_svc=fail
    nslookup kube-dns.kube-system.svc.cluster.local ${svc_ip} >/dev/null 2>&1 && echo step_dns=ok || echo step_dns=fail
    cat /etc/resolv.conf | sed 's/^/resolv: /'
  " 2>&1)" || true
echo "$out" | sed 's/^/    /'

grep -q 'step_pod=ok' <<<"$out" && pass "pod on ${NODE2_NAME} -> CoreDNS pod ${dns_ip} (flannel VXLAN)" \
  || fail "pod-to-pod across nodes failed: VXLAN 8472/udp on ${MGMT_IF} is blocked or flannel.1 is wrong"
grep -q 'step_svc=ok' <<<"$out" && pass "pod -> service ClusterIP ${svc_ip} (kube-proxy)" \
  || fail "service ClusterIP unreachable from ${NODE2_NAME}"
grep -q 'step_dns=ok' <<<"$out" && pass "pod resolves kube-dns.kube-system.svc.cluster.local" \
  || fail "DNS lookup failed from ${NODE2_NAME}"
grep -q 'podip=' <<<"$out" || warn "netcheck pod never ran — see the kubectl output above (image pull? scheduling?)"

summary "pod network"
