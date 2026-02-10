from __future__ import annotations
import pytest
from peercolab_engine import (
    Transport, TransportSession, TransportSessionBuilder, Result,
    TransportContext, TransportRequest, Attribute, InMemoryContextCache,
    RequestOperation, MessageOperation, DefaultTransportSerializer,
)

json_serializer = DefaultTransportSerializer()


class SampleGetOp(RequestOperation):
    def __init__(self):
        super().__init__("test.get", "GET")


class SampleMsgOp(MessageOperation):
    def __init__(self):
        super().__init__("test.msg", "PROCESS")


class TestTransportSession:
    def test_returns_a_transport_session_builder(self):
        builder = Transport.session("test-session")
        assert isinstance(builder, TransportSessionBuilder)

    def test_fluent_api_returns_builder(self):
        builder = Transport.session("s1")
        assert builder.assign_serializer(json_serializer) is builder

        async def noop_inspector(input, ctx):
            pass

        async def noop_response_inspector(r, input, ctx):
            return r

        assert builder.inspect_request(noop_inspector) is builder
        assert builder.inspect_response(noop_response_inspector) is builder
        class NoopLogger:
            log_level = 3
            def write(self, msg): pass
        assert builder.on_log_message(NoopLogger()) is builder
        assert builder.setup_outbound_context_cache(InMemoryContextCache()) is builder

    def test_intercept_accepts_request_handlers(self):
        op = SampleGetOp()
        handler = op.handle(lambda input, ctx: _async(Result.ok({"a": "ok"})))
        builder = Transport.session("s1")
        assert builder.intercept(handler) is builder

    def test_intercept_accepts_message_handlers(self):
        op = SampleMsgOp()
        handler = op.handle(lambda input, ctx: _async(Result.ok()))
        builder = Transport.session("s1")
        assert builder.intercept(handler) is builder

    def test_intercept_pattern_returns_builder(self):
        builder = Transport.session("s1")
        assert builder.intercept_pattern("test.", lambda input, ctx: _async(Result.ok())) is builder

    def test_build_returns_transport_session(self):
        session = Transport.session("s1").build()
        assert isinstance(session, TransportSession)

    def test_outbound_session_builder_returns_builder(self):
        builder = Transport.session("s1")
        outbound = builder.outbound_session_builder("outbound-client")
        assert outbound is not None
        assert callable(outbound.build)

    def test_with_locale_sets_locale(self):
        session = Transport.session("s1").build()
        assert session.with_locale("nb-NO") is session

    def test_create_client_returns_client(self):
        session = Transport.session("s1").build()
        client = session.create_client("c1")
        assert client is not None

    def test_get_serializer_returns_serializer(self):
        session = Transport.session("s1").assign_serializer(json_serializer).build()
        assert session.get_serializer() is json_serializer

    def test_create_client_with_tenant(self):
        session = Transport.session("s1").build()
        client = session.create_client("c1", "my-tenant")
        assert client is not None

    @pytest.mark.asyncio
    async def test_accept_incoming_request_no_handler_returns_bad_request(self):
        session = Transport.session("s1").build()
        from peercolab_engine import Characters
        request = TransportRequest(
            "nonexistent.op", "GET", "request", "c1", "u1",
            "tx-1", "", "en-GB", Characters(), [], [], {}, None,
        ).assign_serializer(json_serializer)
        result = await session.accept_incoming_request(request.serialize())
        assert result.status_code == 400

    @pytest.mark.asyncio
    async def test_pattern_handler_matches_prefix(self):
        session = (
            Transport.session("s1")
            .intercept_pattern("items.", lambda input, ctx: _async(Result.ok({"matched": True})))
            .build()
        )
        from peercolab_engine import Characters
        request = TransportRequest(
            "items.getAll", "GET", "request", "c1", "u1",
            "tx-1", "", "en-GB", Characters(), [], [], {}, None,
        ).assign_serializer(json_serializer)
        result = await session.accept_incoming_request(request.serialize())
        assert result.is_success() is True
        assert result.value["matched"] is True

    @pytest.mark.asyncio
    async def test_request_inspector_can_short_circuit(self):
        op = SampleGetOp()
        session = (
            Transport.session("s1")
            .intercept(op.handle(lambda input, ctx: _async(Result.ok({"a": "normal"}))))
            .inspect_request(lambda input, ctx: _async(Result.failed(403, "BLOCKED")))
            .build()
        )
        from peercolab_engine import Characters
        request = TransportRequest(
            op.id, op.verb, op.type, "c1", "u1",
            "tx-1", "", "en-GB", Characters(), [], [], {"q": "test"}, None,
        ).assign_serializer(json_serializer)
        result = await session.accept_incoming_request(request.serialize())
        assert result.status_code == 403

    @pytest.mark.asyncio
    async def test_accept_incoming_request_routes_request_type(self):
        op = SampleGetOp()
        session = (
            Transport.session("s1")
            .intercept(op.handle(lambda input, ctx: _async(Result.ok({"a": input["q"] + "!"}))))
            .build()
        )
        from peercolab_engine import Characters
        request = TransportRequest(
            op.id, op.verb, op.type, "c1", "u1",
            "tx-1", "", "en-GB", Characters(), [], [],
            {"q": "hi"}, None,
        ).assign_serializer(json_serializer)
        result = await session.accept_incoming_request(request.serialize())
        assert result.is_success() is True

    @pytest.mark.asyncio
    async def test_accept_incoming_request_routes_message_type(self):
        op = SampleMsgOp()
        session = (
            Transport.session("s1")
            .intercept(op.handle(lambda input, ctx: _async(Result.ok())))
            .build()
        )
        from peercolab_engine import Characters
        request = TransportRequest(
            op.id, op.verb, op.type, "c1", "u1",
            "tx-1", "", "en-GB", Characters(), [], [],
            {"msg": "hello"}, None,
        ).assign_serializer(json_serializer)
        result = await session.accept_incoming_request(request.serialize())
        assert result.is_success() is True

    @pytest.mark.asyncio
    async def test_accept_incoming_request_appends_custom_attributes_no_overwrite(self):
        captured_ctx = [None]
        op = SampleGetOp()

        async def handler(input, ctx):
            captured_ctx[0] = ctx
            return Result.ok({"a": "ok"})

        session = (
            Transport.session("s1")
            .intercept(op.handle(handler))
            .build()
        )
        from peercolab_engine import Characters
        request = TransportRequest(
            op.id, op.verb, op.type, "c1", "u1",
            "tx-1", "", "en-GB", Characters(),
            [Attribute("existing", "keep")],
            [], {"q": "test"}, None,
        ).assign_serializer(json_serializer)

        await session.accept_incoming_request(
            request.serialize(),
            [
                Attribute("existing", "should-not-overwrite"),
                Attribute("new-attr", "added"),
            ],
        )
        assert captured_ctx[0].get_attribute("existing") == "keep"
        assert captured_ctx[0].get_attribute("new-attr") == "added"


async def _async(val):
    return val
