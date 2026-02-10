from __future__ import annotations
import pytest
from peercolab_engine import (
    OutboundSessionBuilder, OutboundClientFactory, InMemoryContextCache,
    TransportClient, Result, CallInformation,
    RequestOperation, MessageOperation, RequestOperationRequest, MessageOperationRequest,
    DefaultTransportSerializer, TransportOperation,
)

json_serializer = DefaultTransportSerializer()


class SampleReqOp(RequestOperation):
    def __init__(self):
        super().__init__("test.req", "GET")


class SampleMsgOp(MessageOperation):
    def __init__(self):
        super().__init__("test.msg", "PROCESS")


class TestOutboundSessionBuilder:
    def test_fluent_api_returns_builder(self):
        builder = OutboundSessionBuilder("svc", InMemoryContextCache(), json_serializer)

        async def noop_req(input, ctx):
            pass

        async def noop_resp(r, input, ctx):
            return r

        assert builder.inspect_request(noop_req) is builder
        assert builder.inspect_response(noop_resp) is builder
        assert builder.intercept_pattern("test.", lambda input, ctx: _async(Result.ok())) is builder

    def test_intercept_adds_request_handler(self):
        op = SampleReqOp()
        builder = OutboundSessionBuilder("svc", InMemoryContextCache(), json_serializer)
        assert builder.intercept(op.handle(lambda input, ctx: _async(Result.ok({"a": "ok"})))) is builder

    def test_intercept_adds_message_handler(self):
        op = SampleMsgOp()
        builder = OutboundSessionBuilder("svc", InMemoryContextCache(), json_serializer)
        assert builder.intercept(op.handle(lambda input, ctx: _async(Result.ok()))) is builder

    def test_build_returns_outbound_client_factory(self):
        builder = OutboundSessionBuilder("svc", InMemoryContextCache(), json_serializer)
        factory = builder.build()
        assert isinstance(factory, OutboundClientFactory)


class TestOutboundClientFactory:
    @pytest.mark.asyncio
    async def test_for_incoming_request_creates_client(self):
        cache = InMemoryContextCache()
        call_info = CallInformation.new("en-US", "tenant", "tx-original")
        await cache.put("tx-original", call_info)
        factory = OutboundSessionBuilder("svc", cache, json_serializer).build()
        client = await factory.for_incoming_request("tx-original")
        assert isinstance(client, TransportClient)

    def test_as_independent_requests_creates_client(self):
        factory = OutboundSessionBuilder("svc", InMemoryContextCache(), json_serializer).build()
        client = factory.as_independent_requests()
        assert isinstance(client, TransportClient)

    @pytest.mark.asyncio
    async def test_as_independent_requests_can_execute_request(self):
        op = SampleReqOp()
        factory = (
            OutboundSessionBuilder("svc", InMemoryContextCache(), json_serializer)
            .intercept(op.handle(lambda input, ctx: _async(Result.ok({"a": input["q"] + "!"}))))
            .build()
        )
        client = factory.as_independent_requests()
        result = await client.request(RequestOperationRequest("u1", op, {"q": "hi"}))
        assert result.is_success() is True
        assert result.value == {"a": "hi!"}

    @pytest.mark.asyncio
    async def test_message_handler_works_through_outbound(self):
        op = SampleMsgOp()
        factory = (
            OutboundSessionBuilder("svc", InMemoryContextCache(), json_serializer)
            .intercept(op.handle(lambda input, ctx: _async(Result.ok())))
            .build()
        )
        client = factory.as_independent_requests()
        result = await client.request(MessageOperationRequest("u1", op, {"text": "msg"}))
        assert result.is_success() is True

    @pytest.mark.asyncio
    async def test_pattern_handler_works_through_outbound(self):
        factory = (
            OutboundSessionBuilder("svc", InMemoryContextCache(), json_serializer)
            .intercept_pattern("items.", lambda input, ctx: _async(Result.ok({"matched": True})))
            .build()
        )
        client = factory.as_independent_requests()
        op = TransportOperation("request", "items.list", "GET")
        result = await client.request(RequestOperationRequest("u1", op, {}))
        assert result.is_success() is True
        assert result.value["matched"] is True


async def _async(val):
    return val
