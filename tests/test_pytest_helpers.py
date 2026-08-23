import asyncio

import pytest


def test_eventually_retries_a_failed_assertion(eventually):
    attempts = 0

    def assertion():
        nonlocal attempts
        attempts += 1
        assert attempts == 2

    asyncio.run(eventually(0.1, assertion))

    assert attempts == 2


def test_eventually_makes_a_final_uncaught_assertion_after_timeout(eventually):
    attempts = 0

    def assertion():
        nonlocal attempts
        attempts += 1
        assert False, "condition was not satisfied"

    with pytest.raises(AssertionError, match="condition was not satisfied"):
        asyncio.run(eventually(0, assertion))

    assert attempts == 2
