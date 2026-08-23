import asyncio

import pytest

from django_queue.backends import (
    QueueEmptyException,
    QueueFullException,
)
from django_queue.backends.redis import RedisAsyncPriorityQueue


@pytest.fixture
def redis_priority_queue(redis_url):
    """
    Fixture to set up and clean up a RedisAsyncPriorityQueue instance.
    """
    queue = RedisAsyncPriorityQueue(
        redis_url, queue_name="test_priority_queue", maxsize=5
    )
    queue.clear()
    return queue


def test_init(redis_url):
    """
    Test initialisation of RedisAsyncPriorityQueue.
    """
    queue = RedisAsyncPriorityQueue(redis_url, queue_name="test_priority_queue")
    assert queue.queue_name == "test_priority_queue"
    assert queue.capacity == 0  # Unlimited size by default


@pytest.mark.parametrize(
    "arguments",
    [
        {"options": {"stack": True}},
        {"stack": False},
    ],
)
def test_init_rejects_stack_option(arguments):
    with pytest.raises(ValueError):
        RedisAsyncPriorityQueue("redis://localhost:6379/0", **arguments)


def test_capacity(redis_priority_queue):
    """
    Test the capacity of the priority queue.
    """
    assert redis_priority_queue.capacity == 5


def test_add_overflow(redis_priority_queue):
    """
    Test adding more items than the maximum size should raise QueueFullException.
    """
    redis_priority_queue.add(
        (10, "item1"), (20, "item2"), (30, "item3"), (40, "item4"), (50, "item5")
    )
    with pytest.raises(QueueFullException):
        redis_priority_queue.add((60, "item6"))


def test_get_maintains_priority_order(redis_priority_queue):
    """
    Test `get()` retrieves items in order of priority (highest first).
    """
    redis_priority_queue.add(
        (-100, "low_priority"), (0, "medium_priority"), (100, "high_priority")
    )
    assert redis_priority_queue.get() == "high_priority"
    assert redis_priority_queue.get() == "medium_priority"
    assert redis_priority_queue.get() == "low_priority"


def test_get_empty(redis_priority_queue):
    """
    Test `get()` on an empty queue raises QueueEmptyException.
    """
    with pytest.raises(QueueEmptyException):
        redis_priority_queue.get()


def test_poll_wakes_when_a_priority_item_is_added(redis_priority_queue):
    async def exercise():
        poll_task = asyncio.create_task(redis_priority_queue.apoll(timeout=1))
        await asyncio.sleep(0.01)
        await redis_priority_queue.aadd((10, "arrived"))

        assert await asyncio.wait_for(poll_task, timeout=0.5) == "arrived"
        await redis_priority_queue.aclose()

    asyncio.run(exercise())


def test_poll_timeout(redis_priority_queue):
    """
    Test `poll()` times out gracefully if the queue remains empty.
    """
    with pytest.raises(QueueEmptyException):
        redis_priority_queue.poll(timeout=0)


def test_peek(redis_priority_queue):
    """
    Test `peek()` retrieves the highest-priority item without removing it.
    """
    redis_priority_queue.add((10, "item1"), (50, "item2"), (30, "item3"))
    assert redis_priority_queue.peek() == "item2"  # Highest priority (50)
    assert redis_priority_queue.size() == 3  # Size doesn't decrement


def test_peek_empty(redis_priority_queue):
    """
    Test `peek()` on an empty queue raises QueueEmptyException.
    """
    with pytest.raises(QueueEmptyException):
        redis_priority_queue.peek()


def test_size(redis_priority_queue):
    """
    Test the size of the queue.
    """
    redis_priority_queue.add((10, "item1"), (20, "item2"))
    assert redis_priority_queue.size() == 2
    redis_priority_queue.get()
    assert redis_priority_queue.size() == 1


def test_clear(redis_priority_queue):
    """
    Test clearing the queue.
    """
    redis_priority_queue.add((10, "item1"), (20, "item2"))
    redis_priority_queue.clear()
    assert redis_priority_queue.size() == 0


def test_priority_handling(redis_priority_queue):
    """
    Test that priorities are handled correctly, even with duplicates.
    """
    redis_priority_queue.add((5, "item0"), (10, "item1"), (10, "item2"), (20, "item3"))
    assert redis_priority_queue.get() == "item3"  # Highest priority (20)
    assert redis_priority_queue.get() == "item2"  # Last entered with priority 10
    assert redis_priority_queue.get() == "item1"  # Prev entered with priority 10
    assert redis_priority_queue.get() == "item0"  # Prev entered with priority 5


def test_get_removes_the_stored_member(redis_priority_queue):
    """`get()` returns the highest-priority item and removes exactly that member."""
    redis_priority_queue.add((10, "item1"), (20, "item2"))

    assert redis_priority_queue.get() == "item2"
    assert redis_priority_queue.size() == 1
    assert redis_priority_queue.get() == "item1"
    assert redis_priority_queue.size() == 0


def test_get_removes_the_stored_member_from_a_decoding_url(redis_url):
    """`get()` removes the member when a decoding URL yields text."""
    queue = RedisAsyncPriorityQueue(
        f"{redis_url}?decode_responses=true", queue_name="test-priority-decoding"
    )
    queue.clear()

    queue.add((10, "item1"), (20, "item2"))

    assert queue.get() == "item2"
    assert queue.size() == 1
    assert queue.get() == "item1"
    assert queue.size() == 0


def test_negative_priority(redis_priority_queue):
    """
    Test handling of negative priority values.
    """
    redis_priority_queue.add(
        (-50, "negative_priority"), (100, "high_priority"), (0, "neutral_priority")
    )
    assert redis_priority_queue.get() == "high_priority"  # Highest priority first
    assert redis_priority_queue.get() == "neutral_priority"  # Neutral priority
    assert redis_priority_queue.get() == "negative_priority"  # Lowest priority
