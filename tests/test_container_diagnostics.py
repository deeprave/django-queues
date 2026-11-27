import pytest

from tests import conftest


def test_start_container_captures_logs_before_cleanup():
    events = []

    class FailedContainer:
        def start(self):
            events.append("start")
            raise RuntimeError("exited before ready")

        def get_logs(self):
            events.append("get_logs")
            return b"cluster startup failure", b""

        def stop(self):
            events.append("stop")

    with pytest.raises(
        RuntimeError, match="stdout='cluster startup failure' stderr=''"
    ):
        conftest._start_container(FailedContainer(), "Redis Cluster")

    assert events == ["start", "get_logs", "stop"]
