FROM redis:8

COPY entrypoint.sh /usr/local/bin/cluster-entrypoint.sh
RUN chmod +x /usr/local/bin/cluster-entrypoint.sh

EXPOSE 7000 7001 7002

HEALTHCHECK --interval=1s --timeout=3s --start-period=2s --retries=30 \
    CMD redis-cli -p 7000 cluster info | grep -q 'cluster_state:ok'

ENTRYPOINT ["/usr/local/bin/cluster-entrypoint.sh"]
