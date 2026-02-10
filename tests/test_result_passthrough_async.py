from __future__ import annotations
import pytest
from peercolab_engine import ResultPassthroughAsync, Result


class TestResultPassthroughAsync:
    @pytest.mark.asyncio
    async def test_returns_initial_result_on_success_with_no_chained_actions(self):
        r = await ResultPassthroughAsync.start_with(
            lambda: _async(Result.ok(42))
        ).run()
        assert r.is_success() is True
        assert r.value == 42

    @pytest.mark.asyncio
    async def test_returns_initial_result_when_all_chained_actions_succeed(self):
        r = await (
            ResultPassthroughAsync.start_with(lambda: _async(Result.ok("hello")))
            .then(lambda: _async(Result.ok()))
            .then(lambda: _async(Result.ok()))
            .run()
        )
        assert r.value == "hello"

    @pytest.mark.asyncio
    async def test_stops_on_first_chained_failure(self):
        third_called = [False]
        async def third():
            third_called[0] = True
            return Result.ok()
        r = await (
            ResultPassthroughAsync.start_with(lambda: _async(Result.ok(1)))
            .then(lambda: _async(Result.failed(400, "BAD")))
            .then(third)
            .run()
        )
        assert r.is_success() is False
        assert r.status_code == 400
        assert third_called[0] is False

    @pytest.mark.asyncio
    async def test_returns_failure_when_initial_action_fails(self):
        r = await (
            ResultPassthroughAsync.start_with(lambda: _async(Result.failed(500, "INIT_FAIL")))
            .then(lambda: _async(Result.ok()))
            .run()
        )
        assert r.is_success() is False
        assert r.error.code == "INIT_FAIL"

    @pytest.mark.asyncio
    async def test_handles_exception_in_initial_action(self):
        async def boom():
            raise RuntimeError("boom")
        r = await ResultPassthroughAsync.start_with(boom).run()
        assert r.is_success() is False
        assert r.status_code == 500

    @pytest.mark.asyncio
    async def test_handles_exception_in_chained_action(self):
        async def chain_boom():
            raise RuntimeError("chain boom")
        r = await (
            ResultPassthroughAsync.start_with(lambda: _async(Result.ok(1)))
            .then(chain_boom)
            .run()
        )
        assert r.is_success() is False
        assert r.status_code == 500


async def _async(val):
    return val
