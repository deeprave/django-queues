#!/bin/sh
set -eu

for port in 7000 7001 7002; do
  mkdir -p "/data/${port}"
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
done

for port in 7000 7001 7002; do
  until redis-cli -p "${port}" ping 2>/dev/null | grep -q PONG; do
    sleep 0.1
  done
done

redis-cli --cluster create \
  127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 \
  --cluster-replicas 0 \
  --cluster-yes

echo "cluster-ready"

while true; do
  sleep 3600
done
