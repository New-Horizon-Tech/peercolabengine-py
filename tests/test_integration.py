from __future__ import annotations
import pytest
from peercolab_engine import (
    Transport, Result, TransportContext, TransportRequest,
    RequestOperation, MessageOperation, RequestOperationRequest, MessageOperationRequest,
    Metavalues, Metavalue, Identifier, ResultPassthroughAsync,
    TransportError, DefaultTransportSerializer, Characters, TransportOperation,
)

json_serializer = DefaultTransportSerializer()


class GetItemsOp(RequestOperation):
    def __init__(self):
        super().__init__("items.get", "GET")


class CreateItemOp(RequestOperation):
    def __init__(self):
        super().__init__("items.create", "CREATE")


class NotifyOp(MessageOperation):
    def __init__(self):
        super().__init__("notify.send", "PROCESS")


class TestIntegration:
    @pytest.mark.asyncio
    async def test_end_to_end_inbound_request_handling(self):
        op = GetItemsOp()
        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .intercept(op.handle(lambda input, ctx: _async(Result.ok({"items": ["a", "b"]}))))
            .build()
        )
        request = TransportRequest(
            op.id, op.verb, op.type, "client-1", "usage-1",
            "tx-1", "tenant", "en-GB", Characters(), [], [],
            {"query": "test"}, None,
        ).assign_serializer(json_serializer)
        result = await session.accept_incoming_request(request.serialize())
        assert result.is_success() is True
        assert result.value["items"] == ["a", "b"]

    @pytest.mark.asyncio
    async def test_end_to_end_client_to_session(self):
        get_op = GetItemsOp()
        create_op = CreateItemOp()
        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .intercept(get_op.handle(lambda input, ctx: _async(Result.ok({"items": [input["query"]]}))))
            .intercept(create_op.handle(lambda input, ctx: _async(Result.ok({"id": "new-" + input["name"]}))))
            .build()
        )
        client = session.create_client("c1")
        r1 = await client.request(RequestOperationRequest("u1", get_op, {"query": "foo"}))
        assert r1.is_success() is True
        assert r1.value == {"items": ["foo"]}
        r2 = await client.request(RequestOperationRequest("u2", create_op, {"name": "bar"}))
        assert r2.is_success() is True
        assert r2.value == {"id": "new-bar"}

    @pytest.mark.asyncio
    async def test_end_to_end_outbound_session(self):
        get_op = GetItemsOp()
        builder = (
            Transport.session("inbound")
            .assign_serializer(json_serializer)
            .intercept(get_op.handle(lambda input, ctx: _async(Result.ok({"items": ["x"]}))))
        )
        outbound = (
            builder.outbound_session_builder("outbound-svc")
            .intercept(get_op.handle(lambda input, ctx: _async(Result.ok({"items": ["outbound-item"]}))))
            .build()
        )
        client = outbound.as_independent_requests()
        result = await client.request(RequestOperationRequest("u1", get_op, {"query": "q"}))
        assert result.is_success() is True
        assert result.value == {"items": ["outbound-item"]}

    @pytest.mark.asyncio
    async def test_end_to_end_request_and_response_inspection(self):
        op = GetItemsOp()
        inspected_requests = []
        inspected_responses = []

        async def req_inspector(input, ctx):
            inspected_requests.append(input)

        async def resp_inspector(result, input, ctx):
            inspected_responses.append(result)
            return result

        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .intercept(op.handle(lambda input, ctx: _async(Result.ok({"items": ["a"]}))))
            .inspect_request(req_inspector)
            .inspect_response(resp_inspector)
            .build()
        )
        client = session.create_client("c1")
        await client.request(RequestOperationRequest("u1", op, {"query": "test"}))
        assert len(inspected_requests) == 1
        assert len(inspected_responses) == 1
        assert inspected_responses[0].is_success() is True

    def test_end_to_end_result_chaining_with_maybe(self):
        r = (
            Result.ok(10)
            .maybe(lambda v, m: Result.ok(v * 2))
            .maybe(lambda v, m: Result.ok(v + 5))
        )
        assert r.value == 25

    def test_end_to_end_result_chaining_maybe_stops_on_error(self):
        third_called = [False]
        r = (
            Result.ok(10)
            .maybe(lambda v, m: Result.ok(v * 2))
            .maybe(lambda v, m: Result.failed(400, "STOP"))
            .maybe(lambda v, m: (third_called.__setitem__(0, True), Result.ok(999))[1])
        )
        assert r.is_success() is False
        assert third_called[0] is False

    @pytest.mark.asyncio
    async def test_end_to_end_async_pipeline(self):
        results = []

        async def step1():
            results.append(1)
            return Result.ok("initial")

        async def step2():
            results.append(2)
            return Result.ok()

        async def step3():
            results.append(3)
            return Result.ok()

        r = await (
            ResultPassthroughAsync.start_with(step1)
            .then(step2)
            .then(step3)
            .run()
        )
        assert r.is_success() is True
        assert r.value == "initial"
        assert results == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_end_to_end_metadata_flow(self):
        op = GetItemsOp()

        async def handler(input, ctx):
            meta = Metavalues()
            meta.set_has_more_values(True)
            meta.set_total_value_count(100)
            mv = Metavalue.with_values("v1", "tenant", Identifier("p1", "user"))
            meta.add(mv)
            return Result.ok({"items": ["a"]}, meta)

        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .intercept(op.handle(handler))
            .build()
        )
        client = session.create_client("c1")
        result = await client.request(RequestOperationRequest("u1", op, {"query": "q"}))
        assert result.is_success() is True
        assert result.meta.has_more_values is True
        assert result.meta.total_value_count == 100
        assert len(result.meta.values) == 1

    @pytest.mark.asyncio
    async def test_end_to_end_error_propagation(self):
        op = GetItemsOp()

        async def handler(input, ctx):
            return Result.failed(422, TransportError.basic("VALIDATION", "Field required", "Please fill in all fields"))

        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .intercept(op.handle(handler))
            .build()
        )
        client = session.create_client("c1")
        result = await client.request(RequestOperationRequest("u1", op, {"query": ""}))
        assert result.is_success() is False
        assert result.status_code == 422
        assert result.error.code == "VALIDATION"
        assert result.error.details.technical_error == "Field required"
        assert result.error.details.user_error == "Please fill in all fields"

    @pytest.mark.asyncio
    async def test_end_to_end_multiple_pattern_handlers(self):
        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .intercept_pattern("items.", lambda input, ctx: _async(Result.ok({"handler": "items"})))
            .intercept_pattern("items.admin.", lambda input, ctx: _async(Result.ok({"handler": "items.admin"})))
            .build()
        )
        client = session.create_client("c1")
        op1 = TransportOperation("request", "items.admin.delete", "DELETE")
        r1 = await client.request(RequestOperationRequest("u1", op1, {}))
        assert r1.value["handler"] == "items.admin"

        op2 = TransportOperation("request", "items.list", "GET")
        r2 = await client.request(RequestOperationRequest("u2", op2, {}))
        assert r2.value["handler"] == "items"

    @pytest.mark.asyncio
    async def test_end_to_end_context_propagation(self):
        captured_ctx = [None]
        op = GetItemsOp()

        async def handler(input, ctx):
            captured_ctx[0] = ctx
            return Result.ok({"items": []})

        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .intercept(op.handle(handler))
            .build()
        )
        client = (
            session.create_client("c1", "my-tenant")
            .with_locale("nb-NO")
            .add_attribute("userId", "u123")
            .add_path_param("itemId", "456")
        )
        await client.request(RequestOperationRequest("u1", op, {"query": "test"}))
        assert captured_ctx[0] is not None
        assert captured_ctx[0].call.locale == "nb-NO"
        assert captured_ctx[0].call.data_tenant == "my-tenant"
        assert captured_ctx[0].has_attribute("userId") is True
        assert captured_ctx[0].get_attribute("userId") == "u123"
        assert captured_ctx[0].has_path_parameter("itemId") is True
        assert captured_ctx[0].get_path_parameter("itemId") == "456"


async def _async(val):
    return val
