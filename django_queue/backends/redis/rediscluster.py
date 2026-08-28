"""Explicit Redis Cluster queue backends."""

from __future__ import annotations

from .provider import QueueProviderRedisCluster
from .rediseventqueue import RedisEventQueue
from .redisnotificationqueue import RedisNotificationQueue
from .redispqueue import RedisAsyncPriorityQueue
from .redispqueuejson import RedisAsyncPriorityQueueJson
from .redisqueue import RedisAsyncQueue, RedisAsyncStack
from .redisqueuejson import RedisAsyncQueueJson, RedisAsyncStackJson

_CLUSTER_PROVIDER = QueueProviderRedisCluster


class RedisClusterAsyncQueue(RedisAsyncQueue):
    redis_topology = "cluster"
    provider_class = _CLUSTER_PROVIDER


class RedisClusterAsyncStack(RedisAsyncStack):
    redis_topology = "cluster"
    provider_class = _CLUSTER_PROVIDER


class RedisClusterAsyncQueueJson(RedisAsyncQueueJson):
    redis_topology = "cluster"
    provider_class = _CLUSTER_PROVIDER


class RedisClusterAsyncStackJson(RedisAsyncStackJson):
    redis_topology = "cluster"
    provider_class = _CLUSTER_PROVIDER


class RedisClusterAsyncPriorityQueue(RedisAsyncPriorityQueue):
    redis_topology = "cluster"
    provider_class = _CLUSTER_PROVIDER


class RedisClusterAsyncPriorityQueueJson(RedisAsyncPriorityQueueJson):
    redis_topology = "cluster"
    provider_class = _CLUSTER_PROVIDER


class RedisClusterEventQueue(RedisEventQueue):
    redis_topology = "cluster"
    provider_class = _CLUSTER_PROVIDER


class RedisClusterNotificationQueue(RedisNotificationQueue):
    redis_topology = "cluster"
    provider_class = _CLUSTER_PROVIDER
