from __future__ import annotations
import pytest
from peercolab_engine import (
    Transport, TransportClient, Result, Attribute,
    RequestOperation, MessageOperation, RequestOperationRequest, MessageOperationRequest,
    TransportContext, DefaultTransportSerializer, OutOfContextOperation,
    OutOfContextOperationPathParameter,
)

json_serializer = DefaultTransportSerializer()


class GetItemOp(RequestOperation):
    def __init__(self):
        super().__init__("items.get", "GET")


class NotifyOp(MessageOperation):
    def __init__(self):
        super().__init__("notify.send", "PROCESS")


def build_session():
    get_op = GetItemOp()
    msg_op = NotifyOp()
    return (
        Transport.session("test")
        .assign_serializer(json_serializer)
        .intercept(get_op.handle(lambda input, ctx: _async(Result.ok({"name": "item-" + input["id"]}))))
        .intercept(msg_op.handle(lambda input, ctx: _async(Result.ok())))
        .build()
    )


async def _async(val):
    return val


class TestTransportClientImmutability:
    def test_with_locale_returns_new_client(self):
        session = build_session()
        c1 = session.create_client("c1")
        c2 = c1.with_locale("nb-NO")
        assert c2 is not c1

    def test_with_data_tenant_returns_new_client(self):
        session = build_session()
        c1 = session.create_client("c1")
        c2 = c1.with_data_tenant("tenant-x")
        assert c2 is not c1

    def test_with_characters_returns_new_client(self):
        session = build_session()
        c1 = session.create_client("c1")
        c2 = c1.with_characters({"performer": None, "responsible": None, "subject": None})
        assert c2 is not c1

    def test_add_attribute_returns_new_client(self):
        session = build_session()
        c1 = session.create_client("c1")
        c2 = c1.add_attribute("key", "value")
        assert c2 is not c1

    def test_remove_attribute_returns_new_client(self):
        session = build_session()
        c1 = session.create_client("c1").add_attribute("key", "val")
        c2 = c1.remove_attribute("key")
        assert c2 is not c1

    def test_add_path_param_remove_path_param_return_new_clients(self):
        session = build_session()
        c1 = session.create_client("c1")
        c2 = c1.add_path_param("p1", "v1")
        assert c2 is not c1
        c3 = c2.remove_path_param("p1")
        assert c3 is not c2

    @pytest.mark.asyncio
    async def test_with_transaction_id_returns_new_client(self):
        session = build_session()
        c1 = session.create_client("c1")
        c2 = await c1.with_transaction_id("tx-custom")
        assert c2 is not c1


class TestTransportClientRequest:
    @pytest.mark.asyncio
    async def test_sends_request_operation_request(self):
        session = build_session()
        client = session.create_client("c1")
        op = GetItemOp()
        result = await client.request(RequestOperationRequest("u1", op, {"id": "42"}))
        assert result.is_success() is True
        assert result.value == {"name": "item-42"}

    @pytest.mark.asyncio
    async def test_sends_message_operation_request(self):
        session = build_session()
        client = session.create_client("c1")
        op = NotifyOp()
        result = await client.request(MessageOperationRequest("u1", op, {"text": "hi"}))
        assert result.is_success() is True


class TestTransportClientRequestContext:
    @pytest.mark.asyncio
    async def test_request_carries_locale_and_tenant(self):
        captured_ctx = [None]
        get_op = GetItemOp()

        async def handler(input, ctx):
            captured_ctx[0] = ctx
            return Result.ok({"name": "ok"})

        session = (
            Transport.session("test")
            .assign_serializer(json_serializer)
            .intercept(get_op.handle(handler))
            .build()
        )
        client = session.create_client("c1").with_locale("nb-NO").with_data_tenant("my-tenant")
        await client.request(RequestOperationRequest("u1", get_op, {"id": "1"}))
        assert captured_ctx[0].call.locale == "nb-NO"
        assert captured_ctx[0].call.data_tenant == "my-tenant"

    @pytest.mark.asyncio
    async def test_request_carries_attributes(self):
        captured_ctx = [None]
        get_op = GetItemOp()

        async def handler(input, ctx):
            captured_ctx[0] = ctx
            return Result.ok({"name": "ok"})

        session = (
            Transport.session("test")
            .assign_serializer(json_serializer)
            .intercept(get_op.handle(handler))
            .build()
        )
        client = session.create_client("c1").add_attribute("key", "val")
        await client.request(RequestOperationRequest("u1", get_op, {"id": "1"}))
        assert captured_ctx[0].has_attribute("key") is True
        assert captured_ctx[0].get_attribute("key") == "val"

    @pytest.mark.asyncio
    async def test_request_carries_path_params(self):
        captured_ctx = [None]
        get_op = GetItemOp()

        async def handler(input, ctx):
            captured_ctx[0] = ctx
            return Result.ok({"name": "ok"})

        session = (
            Transport.session("test")
            .assign_serializer(json_serializer)
            .intercept(get_op.handle(handler))
            .build()
        )
        client = session.create_client("c1").add_path_param("itemId", "42")
        await client.request(RequestOperationRequest("u1", get_op, {"id": "1"}))
        assert captured_ctx[0].has_path_parameter("itemId") is True
        assert captured_ctx[0].get_path_parameter("itemId") == "42"

    @pytest.mark.asyncio
    async def test_request_carries_characters(self):
        captured_ctx = [None]
        get_op = GetItemOp()

        async def handler(input, ctx):
            captured_ctx[0] = ctx
            return Result.ok({"name": "ok"})

        session = (
            Transport.session("test")
            .assign_serializer(json_serializer)
            .intercept(get_op.handle(handler))
            .build()
        )
        chars = {"performer": {"id": "p1", "type": "user"}}
        client = session.create_client("c1").with_characters(chars)
        await client.request(RequestOperationRequest("u1", get_op, {"id": "1"}))
        assert captured_ctx[0].call.characters["performer"]["id"] == "p1"

    @pytest.mark.asyncio
    async def test_add_attribute_updates_existing(self):
        captured_ctx = [None]
        get_op = GetItemOp()

        async def handler(input, ctx):
            captured_ctx[0] = ctx
            return Result.ok({"name": "ok"})

        session = (
            Transport.session("test")
            .assign_serializer(json_serializer)
            .intercept(get_op.handle(handler))
            .build()
        )
        client = session.create_client("c1").add_attribute("key", "first").add_attribute("key", "second")
        await client.request(RequestOperationRequest("u1", get_op, {"id": "1"}))
        assert captured_ctx[0].get_attribute("key") == "second"

    @pytest.mark.asyncio
    async def test_add_path_param_updates_existing(self):
        captured_ctx = [None]
        get_op = GetItemOp()

        async def handler(input, ctx):
            captured_ctx[0] = ctx
            return Result.ok({"name": "ok"})

        session = (
            Transport.session("test")
            .assign_serializer(json_serializer)
            .intercept(get_op.handle(handler))
            .build()
        )
        client = session.create_client("c1").add_path_param("id", "first").add_path_param("id", "second")
        await client.request(RequestOperationRequest("u1", get_op, {"id": "1"}))
        assert captured_ctx[0].get_path_parameter("id") == "second"

    @pytest.mark.asyncio
    async def test_immutability_preserves_base_client(self):
        captured_ctx = [None]
        get_op = GetItemOp()

        async def handler(input, ctx):
            captured_ctx[0] = ctx
            return Result.ok({"name": "ok"})

        session = (
            Transport.session("test")
            .assign_serializer(json_serializer)
            .intercept(get_op.handle(handler))
            .build()
        )
        base = session.create_client("c1").with_locale("en-US")
        derived = base.with_locale("nb-NO")

        await derived.request(RequestOperationRequest("u1", get_op, {"id": "1"}))
        assert captured_ctx[0].call.locale == "nb-NO"

        await base.request(RequestOperationRequest("u2", get_op, {"id": "2"}))
        assert captured_ctx[0].call.locale == "en-US"


class TestTransportClientAcceptOperation:
    @pytest.mark.asyncio
    async def test_handles_request_type(self):
        session = build_session()
        client = session.create_client("c1")
        result = await client.accept_operation(OutOfContextOperation(
            "u1", "items.get", "GET", "request", {"id": "99"},
        ))
        assert result.is_success() is True

    @pytest.mark.asyncio
    async def test_handles_message_type(self):
        session = build_session()
        client = session.create_client("c1")
        result = await client.accept_operation(OutOfContextOperation(
            "u1", "notify.send", "PROCESS", "message", {"text": "msg"},
        ))
        assert result.is_success() is True

    @pytest.mark.asyncio
    async def test_passes_path_params_and_custom_attributes(self):
        captured_ctx = [None]
        get_op = GetItemOp()

        async def handler(input, ctx):
            captured_ctx[0] = ctx
            return Result.ok({"name": "ok"})

        session = (
            Transport.session("test")
            .assign_serializer(json_serializer)
            .intercept(get_op.handle(handler))
            .build()
        )
        client = session.create_client("c1")
        await client.accept_operation(
            OutOfContextOperation(
                "u1", "items.get", "GET", "request", {"id": "1"},
                [OutOfContextOperationPathParameter("itemId", "123")],
            ),
            [Attribute("custom", "attr-val")],
        )
        assert captured_ctx[0].has_path_parameter("itemId") is True
        assert captured_ctx[0].get_path_parameter("itemId") == "123"
        assert captured_ctx[0].has_attribute("custom") is True
