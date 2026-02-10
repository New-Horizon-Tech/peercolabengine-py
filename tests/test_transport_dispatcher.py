from __future__ import annotations
import pytest
from peercolab_engine import (
    TransportDispatcher, InMemoryContextCache, TransportContext, CallInformation,
    OperationInformation, Result, DefaultTransportSerializer,
)

json_serializer = DefaultTransportSerializer()


def make_ctx(op_id, op_type="request"):
    return TransportContext(
        OperationInformation(op_id, "GET", op_type, "client", "usage"),
        CallInformation.new("en-GB", "", "tx-1"),
        json_serializer,
    )


class TestHandlerRegistration:
    def test_add_request_and_message_handler(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), False)
        d.add_request_handler("op.req", lambda input, ctx: _async(Result.ok()))
        d.add_message_handler("op.msg", lambda input, ctx: _async(Result.ok()))

    def test_duplicate_handler_throws(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), False)
        d.add_request_handler("op.req", lambda input, ctx: _async(Result.ok()))
        with pytest.raises(RuntimeError, match="already has a handler"):
            d.add_request_handler("op.req", lambda input, ctx: _async(Result.ok()))

    def test_duplicate_across_handler_types_throws(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), False)
        d.add_request_handler("op.x", lambda input, ctx: _async(Result.ok()))
        with pytest.raises(RuntimeError, match="already has a handler"):
            d.add_message_handler("op.x", lambda input, ctx: _async(Result.ok()))

    def test_duplicate_message_handler_throws(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), False)
        d.add_message_handler("op.msg", lambda input, ctx: _async(Result.ok()))
        with pytest.raises(RuntimeError, match="already has a handler"):
            d.add_message_handler("op.msg", lambda input, ctx: _async(Result.ok()))

    def test_duplicate_pattern_handler_throws(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), False)
        d.add_pattern_handler("items.", lambda input, ctx: _async(Result.ok()))
        with pytest.raises(RuntimeError, match="already has a handler"):
            d.add_pattern_handler("items.", lambda input, ctx: _async(Result.ok()))

    def test_add_pattern_handler(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), False)
        d.add_pattern_handler("items.", lambda input, ctx: _async(Result.ok()))


class TestHandleAsRequest:
    @pytest.mark.asyncio
    async def test_routes_to_correct_handler(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), False)
        d.add_request_handler("op.get", lambda input, ctx: _async(Result.ok({"echo": input["q"]})))
        ctx = make_ctx("op.get")
        result = await d.handle_as_request({"q": "hi"}, ctx)
        assert result.is_success() is True
        assert result.value["echo"] == "hi"

    @pytest.mark.asyncio
    async def test_returns_400_when_no_handler_found(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), False)
        ctx = make_ctx("missing.op")
        result = await d.handle_as_request({}, ctx)
        assert result.status_code == 400
        assert "HandlerNotFound" in result.error.code

    @pytest.mark.asyncio
    async def test_enriches_error(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), False)
        d.add_request_handler("op.fail", lambda input, ctx: _async(Result.failed(500, "ERR")))
        ctx = make_ctx("op.fail")
        result = await d.handle_as_request({}, ctx)
        assert result.error.details.called_operation == "op.fail"
        assert result.error.details.calling_client == "client"
        assert result.error.details.calling_usage == "usage"


class TestHandleAsMessage:
    @pytest.mark.asyncio
    async def test_routes_to_correct_handler(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), False)
        d.add_message_handler("op.msg", lambda input, ctx: _async(Result.ok()))
        ctx = make_ctx("op.msg", "message")
        result = await d.handle_as_message({}, ctx)
        assert result.is_success() is True

    @pytest.mark.asyncio
    async def test_returns_400_when_no_handler_found(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), False)
        ctx = make_ctx("missing.msg", "message")
        result = await d.handle_as_message({}, ctx)
        assert result.status_code == 400


class TestHandlerThrows:
    @pytest.mark.asyncio
    async def test_request_handler_throws_returns_error(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), False)

        async def boom(input, ctx):
            raise RuntimeError("handler boom")

        d.add_request_handler("op.throw", boom)
        ctx = make_ctx("op.throw")
        result = await d.handle_as_request({}, ctx)
        assert result.is_success() is False
        assert result.status_code == 500

    @pytest.mark.asyncio
    async def test_message_handler_throws_returns_error(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), False)

        async def boom(input, ctx):
            raise RuntimeError("msg boom")

        d.add_message_handler("op.throw", boom)
        ctx = make_ctx("op.throw", "message")
        result = await d.handle_as_message({}, ctx)
        assert result.is_success() is False
        assert result.status_code == 500


class TestRouteFromGatewayRequest:
    @pytest.mark.asyncio
    async def test_routes_request_type(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), False)
        d.add_request_handler("op.gw", lambda input, ctx: _async(Result.ok({"routed": True})))
        ctx = make_ctx("op.gw", "request")
        result = await d.route_from_gateway_request({}, ctx)
        assert result.is_success() is True
        assert result.value["routed"] is True

    @pytest.mark.asyncio
    async def test_routes_message_type(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), False)
        d.add_message_handler("op.gw", lambda input, ctx: _async(Result.ok()))
        ctx = make_ctx("op.gw", "message")
        result = await d.route_from_gateway_request({}, ctx)
        assert result.is_success() is True


class TestPatternMatching:
    @pytest.mark.asyncio
    async def test_matches_pattern_prefix(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), False)
        d.add_pattern_handler("items.", lambda input, ctx: _async(Result.ok({"matched": True})))
        ctx = make_ctx("items.getAll")
        result = await d.handle_as_request({}, ctx)
        assert result.is_success() is True
        assert result.value["matched"] is True

    @pytest.mark.asyncio
    async def test_longest_prefix_match_wins(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), False)
        d.add_pattern_handler("items.", lambda input, ctx: _async(Result.ok({"handler": "short"})))
        d.add_pattern_handler("items.admin.", lambda input, ctx: _async(Result.ok({"handler": "long"})))
        ctx = make_ctx("items.admin.delete")
        result = await d.handle_as_request({}, ctx)
        assert result.value["handler"] == "long"


class TestRequestInspector:
    @pytest.mark.asyncio
    async def test_can_short_circuit(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), False)
        d.add_request_handler("op.req", lambda input, ctx: _async(Result.ok({"normal": True})))
        d.requests_inspector = lambda input, ctx: _async(Result.failed(403, "DENIED"))
        ctx = make_ctx("op.req")
        result = await d.handle_as_request({}, ctx)
        assert result.status_code == 403

    @pytest.mark.asyncio
    async def test_returns_none_continues_normally(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), False)
        d.add_request_handler("op.req", lambda input, ctx: _async(Result.ok({"data": 1})))

        async def noop(input, ctx):
            pass

        d.requests_inspector = noop
        ctx = make_ctx("op.req")
        result = await d.handle_as_request({}, ctx)
        assert result.is_success() is True
        assert result.value["data"] == 1

    @pytest.mark.asyncio
    async def test_throws_continues_normally(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), False)
        d.add_request_handler("op.req", lambda input, ctx: _async(Result.ok({"data": 1})))

        async def boom(input, ctx):
            raise RuntimeError("inspector boom")

        d.requests_inspector = boom
        ctx = make_ctx("op.req")
        result = await d.handle_as_request({}, ctx)
        assert result.is_success() is True


class TestResponseInspector:
    @pytest.mark.asyncio
    async def test_receives_the_result(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), False)
        d.add_request_handler("op.req", lambda input, ctx: _async(Result.ok({"data": 1})))
        inspected = [None]

        async def inspector(r, input, ctx):
            inspected[0] = r
            return r

        d.responses_inspector = inspector
        ctx = make_ctx("op.req")
        await d.handle_as_request({}, ctx)
        assert inspected[0] is not None
        assert inspected[0].value["data"] == 1

    @pytest.mark.asyncio
    async def test_without_inspector_returns_original(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), False)
        d.add_request_handler("op.req", lambda input, ctx: _async(Result.ok({"data": 99})))
        ctx = make_ctx("op.req")
        result = await d.handle_as_request({}, ctx)
        assert result.value["data"] == 99

    @pytest.mark.asyncio
    async def test_inspect_message_response_throws_returns_original(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), False)

        async def boom(r, input, ctx):
            raise RuntimeError("msg inspector boom")

        d.responses_inspector = boom
        ctx = make_ctx("op.msg")
        original = Result.ok({"data": 42})
        result = await d.inspect_message_response(original, {}, ctx)
        assert result is original

    @pytest.mark.asyncio
    async def test_inspect_message_response_works(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), False)
        inspected = [False]

        async def inspector(r, input, ctx):
            inspected[0] = True
            return r

        d.responses_inspector = inspector
        ctx = make_ctx("op.msg")
        result = Result.ok({"msg": "hi"})
        await d.inspect_message_response(result, {}, ctx)
        assert inspected[0] is True

    @pytest.mark.asyncio
    async def test_inspect_message_response_without_inspector_returns_original(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), False)
        ctx = make_ctx("op.msg")
        original = Result.ok({"msg": "hi"})
        result = await d.inspect_message_response(original, {}, ctx)
        assert result is original


class TestContextCache:
    @pytest.mark.asyncio
    async def test_put_get_flow_cache_reads_false(self):
        cache = InMemoryContextCache()
        d = TransportDispatcher("s1", cache, False)
        d.add_request_handler("op.req", lambda input, ctx: _async(Result.ok()))
        ctx = make_ctx("op.req")
        await d.handle_as_request({}, ctx)
        cached = await cache.get("tx-1")
        assert cached is not None
        assert cached.locale == "en-GB"

    @pytest.mark.asyncio
    async def test_cache_reads_true_skips_put(self):
        cache = InMemoryContextCache()
        d = TransportDispatcher("s1", cache, True)
        d.add_request_handler("op.req", lambda input, ctx: _async(Result.ok()))
        ctx = make_ctx("op.req")
        await d.handle_as_request({}, ctx)
        cached = await cache.get("tx-1")
        assert cached is None


class TestGetCallInfoFromCache:
    @pytest.mark.asyncio
    async def test_returns_call_info_when_cache_reads_false(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), False)
        info = CallInformation.new("en-US")
        result = await d.get_call_info_from_cache("tx-1", info, True)
        assert result is info

    @pytest.mark.asyncio
    async def test_returns_call_info_when_match_sessions_false(self):
        d = TransportDispatcher("s1", InMemoryContextCache(), True)
        info = CallInformation.new("en-US")
        result = await d.get_call_info_from_cache("tx-1", info, False)
        assert result is info

    @pytest.mark.asyncio
    async def test_cache_miss_returns_fallback(self):
        cache = InMemoryContextCache()
        d = TransportDispatcher("s1", cache, True)
        fallback = CallInformation.new("en-US")
        result = await d.get_call_info_from_cache("nonexistent", fallback, True)
        assert result is fallback

    @pytest.mark.asyncio
    async def test_returns_cached_value(self):
        cache = InMemoryContextCache()
        stored = CallInformation.new("nb-NO", "cached-tenant", "tx-stored")
        await cache.put("tx-stored", stored)
        d = TransportDispatcher("s1", cache, True)
        fallback = CallInformation.new("en-US")
        result = await d.get_call_info_from_cache("tx-stored", fallback, True)
        assert result.locale == "nb-NO"


async def _async(val):
    return val
