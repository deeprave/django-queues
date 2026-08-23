import pytest

from django_queue.backends import (
    MemoryAsyncPriorityQueue,
    QueueEmptyException,
    QueueFullException,
)


@pytest.fixture
def priority_queue():
    return MemoryAsyncPriorityQueue(options={"maxsize": 5})


@pytest.mark.parametrize(
    "arguments",
    [
        {"options": {"stack": True}},
        {"stack": False},
    ],
)
def test_init_rejects_stack_option(arguments):
    with pytest.raises(ValueError):
        MemoryAsyncPriorityQueue(**arguments)


def test_add_to_full_queue_raises_exception(priority_queue):
    priority_queue.add(
        (1, "item1"), (2, "item2"), (3, "item3"), (4, "item4"), (5, "item5")
    )
    with pytest.raises(QueueFullException):
        priority_queue.add((6, "item6"))


def test_get_from_empty_queue_raises_exception(priority_queue):
    with pytest.raises(QueueEmptyException):
        priority_queue.get()


def test_poll_item(priority_queue):
    priority_queue.add((3, "item3"), (1, "item1"), (2, "item2"))
    assert priority_queue.poll() == "item3"
    assert priority_queue.size() == 2


def test_peek_item(priority_queue):
    priority_queue.add((2, "item2"), (1, "item1"))
    assert priority_queue.size() == 2
    assert priority_queue.peek() == "item2"


def test_peek_empty_queue_raises_exception(priority_queue):
    with pytest.raises(QueueEmptyException):
        priority_queue.peek()


def test_clear_queue(priority_queue):
    priority_queue.add((1, "item1"), (2, "item2"))
    priority_queue.clear()
    assert priority_queue.size() == 0


def test_queue_capacity(priority_queue):
    assert priority_queue.capacity == 5


def test_priority_ordering(priority_queue):
    priority_queue.add(
        (100, "high_priority"), (-100, "low_priority"), (0, "medium_priority")
    )
    assert priority_queue.get() == "high_priority"
    assert priority_queue.get() == "medium_priority"
    assert priority_queue.get() == "low_priority"
