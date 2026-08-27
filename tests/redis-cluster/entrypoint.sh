#!/bin/sh
set -eu

TLS_DIR=/tls
if [ -f "${TLS_DIR}/server.crt" ]; then
  TLS_ENABLED=1
else
  TLS_ENABLED=0
fi

for port in 7000 7001 7002; do
  mkdir -p "/data/${port}"
  if [ "${TLS_ENABLED}" = 1 ]; then
    # Keep an unpublished plaintext port so CLUSTER MEET and redis-cli can
    # join without node-to-node TLS. Client traffic uses tls-port.
    redis-server \
      --port "$((port + 100))" \
      --tls-port "${port}" \
      --tls-cert-file "${TLS_DIR}/server.crt" \
      --tls-key-file "${TLS_DIR}/server.key" \
      --tls-ca-cert-file "${TLS_DIR}/ca.crt" \
      --tls-auth-clients no \
      --tls-cluster no \
      --tls-replication no \
      --cluster-announce-ip 127.0.0.1 \
      --cluster-announce-port "${port}" \
      --cluster-announce-tls-port "${port}" \
      --dir "/data/${port}" \
      --cluster-enabled yes \
      --cluster-config-file "/data/${port}/nodes.conf" \
      --cluster-node-timeout 5000 \
      --appendonly yes \
      --daemonize yes \
      --protected-mode no \
      --bind 0.0.0.0
  else
    redis-server \
      --port "${port}" \
      --dir "/data/${port}" \
      --cluster-enabled yes \
      --cluster-config-file "/data/${port}/nodes.conf" \
      --cluster-node-timeout 5000 \
      --appendonly yes \
      --daemonize yes \
      --protected-mode no \
      --bind 0.0.0.0
  fi
done

for port in 7000 7001 7002; do
  if [ "${TLS_ENABLED}" = 1 ]; then
    ping_port="$((port + 100))"
  else
    ping_port="${port}"
  fi
  until redis-cli -p "${ping_port}" ping 2>/dev/null | grep -q PONG; do
    sleep 0.1
  done
done

if [ "${TLS_ENABLED}" = 1 ]; then
  redis-cli --cluster create \
    127.0.0.1:7100 127.0.0.1:7101 127.0.0.1:7102 \
    --cluster-replicas 0 \
    --cluster-yes
else
  redis-cli --cluster create \
    127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 \
    --cluster-replicas 0 \
    --cluster-yes
fi

echo "cluster-ready"

while true; do
  sleep 3600
done
