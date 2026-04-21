from __future__ import annotations

import asyncio
import pytest

from peercolab_engine import (
    Attribute,
    Characters,
    DefaultTransportSerializer,
    DispatchOperation,
    EventDispatchRequest,
    OperationInformation,
    OutOfContextEvent,
    Result,
    Transport,
    TransportContext,
    TransportEvent,
)


json_serializer = DefaultTransportSerializer()


class ItemCreatedEvent(DispatchOperation):
    def __init__(self) -> None:
        super().__init__("items.itemCreated", "action")


class ItemUpdatedEvent(DispatchOperation):
    def __init__(self) -> None:
        super().__init__("items.itemUpdated", "action")


# ---------------------------------------------------------------------------
# TransportEvent round-trip
# ---------------------------------------------------------------------------


class TestTransportEvent:
    def test_serialize_deserialize_round_trip(self):
        te = TransportEvent(
            "items.itemCreated", "action", "client-1", "usage-1",
            "tx-1", "tenant-1", "en-GB",
            Characters(),
            [Attribute("a1", "v1")],
            [Attribute("p1", "pv1")],
            {"itemId": "123"},
            None,
            correlation_id="corr-1",
        ).assign_serializer(json_serializer)

        serialized = te.serialize()
        restored = TransportEvent.from_serialized(json_serializer, serialized)

        assert restored.event_id == "items.itemCreated"
        assert restored.event_type == "action"
        assert restored.calling_client == "client-1"
        assert restored.usage_id == "usage-1"
        assert restored.transaction_id == "tx-1"
        assert restored.data_tenant == "tenant-1"
        assert restored.correlation_id == "corr-1"
        assert restored.request_json == {"itemId": "123"}

    def test_correlation_id_is_optional(self):
        te = TransportEvent(
            "e", "action", "c", "u", "tx", "dt", "en",
            Characters(), [], [], {"x": 1}, None,
        ).assign_serializer(json_serializer)

        restored = TransportEvent.from_serialized(json_serializer, te.serialize())
        assert restored.correlation_id is None

    def test_from_context_carries_correlation_id(self):
        ev = ItemCreatedEvent()
        request = EventDispatchRequest("u1", ev, {"itemId": "123"})
        ctx = TransportContext(
            request.as_operation_information("client-1"),
            _call_info_with(correlation_id="corr-xyz"),
            json_serializer,
        )
        te = TransportEvent.from_context({"itemId": "123"}, ctx)
        assert te.correlation_id == "corr-xyz"


# ---------------------------------------------------------------------------
# subscribe + client.dispatch
# ---------------------------------------------------------------------------


class TestSubscribeAndDispatch:
    @pytest.mark.asyncio
    async def test_single_subscriber_receives_dispatched_event(self):
        ev = ItemCreatedEvent()
        received = {}

        async def handler(input, ctx):
            received["input"] = input
            return Result.ok()

        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .subscribe(ev.handle(handler))
            .build()
        )

        client = session.create_client("c1")
        result = await client.dispatch(EventDispatchRequest("u1", ev, {"itemId": "123"}))

        assert result.is_success()
        assert received["input"] == {"itemId": "123"}

    @pytest.mark.asyncio
    async def test_no_subscribers_returns_handler_not_found(self):
        ev = ItemCreatedEvent()
        session = Transport.session("svc").assign_serializer(json_serializer).build()

        client = session.create_client("c1")
        result = await client.dispatch(EventDispatchRequest("u1", ev, {"itemId": "123"}))

        assert not result.is_success()
        assert result.error.code == "TransportSession.HandlerNotFound"

    @pytest.mark.asyncio
    async def test_fan_out_all_subscribers_invoked(self):
        ev = ItemCreatedEvent()
        calls = []

        async def h_a(input, ctx):
            calls.append("a")
            return Result.ok()

        async def h_b(input, ctx):
            calls.append("b")
            return Result.ok()

        async def h_c(input, ctx):
            calls.append("c")
            return Result.ok()

        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .subscribe(ev.handle(h_a))
            .subscribe(ev.handle(h_b))
            .subscribe(ev.handle(h_c))
            .build()
        )

        client = session.create_client("c1")
        result = await client.dispatch(EventDispatchRequest("u1", ev, {"itemId": "123"}))

        assert result.is_success()
        assert sorted(calls) == ["a", "b", "c"]

    @pytest.mark.asyncio
    async def test_failing_subscribers_surface_as_related_errors_while_others_run(self):
        ev = ItemCreatedEvent()
        ok_ran = {"v": False}

        async def fail_a(input, ctx):
            return Result.failed(500, "Subscriber.A.Failed", "a failed")

        async def ok_handler(input, ctx):
            ok_ran["v"] = True
            return Result.ok()

        async def fail_b(input, ctx):
            return Result.failed(500, "Subscriber.B.Failed", "b failed")

        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .subscribe(ev.handle(fail_a))
            .subscribe(ev.handle(ok_handler))
            .subscribe(ev.handle(fail_b))
            .build()
        )

        client = session.create_client("c1")
        result = await client.dispatch(EventDispatchRequest("u1", ev, {"itemId": "123"}))

        assert ok_ran["v"] is True
        assert not result.is_success()
        assert result.error.code == "TransportSession.DispatchPartialFailure"
        assert len(result.error.related) == 2
        codes = sorted([e.code for e in result.error.related])
        assert codes == ["Subscriber.A.Failed", "Subscriber.B.Failed"]

    @pytest.mark.asyncio
    async def test_thrown_exceptions_captured_as_related_unhandled_error(self):
        ev = ItemCreatedEvent()

        async def boom(input, ctx):
            raise RuntimeError("boom")

        async def ok_handler(input, ctx):
            return Result.ok()

        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .subscribe(ev.handle(boom))
            .subscribe(ev.handle(ok_handler))
            .build()
        )

        client = session.create_client("c1")
        result = await client.dispatch(EventDispatchRequest("u1", ev, {"itemId": "123"}))

        assert not result.is_success()
        assert len(result.error.related) == 1
        assert result.error.related[0].code == "TransportSession.UnhandledError"


# ---------------------------------------------------------------------------
# subscribe_pattern
# ---------------------------------------------------------------------------


class TestSubscribePattern:
    @pytest.mark.asyncio
    async def test_pattern_handler_receives_events_by_prefix(self):
        created = ItemCreatedEvent()
        updated = ItemUpdatedEvent()
        received = []

        async def handler(input, ctx):
            received.append(ctx.operation.id)
            return Result.ok()

        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .subscribe_pattern("items.", handler)
            .build()
        )

        client = session.create_client("c1")
        await client.dispatch(EventDispatchRequest("u1", created, {"itemId": "1"}))
        await client.dispatch(EventDispatchRequest("u1", updated, {"itemId": "2"}))

        assert sorted(received) == ["items.itemCreated", "items.itemUpdated"]

    @pytest.mark.asyncio
    async def test_specific_and_pattern_subscribers_both_fire(self):
        ev = ItemCreatedEvent()
        calls = []

        async def specific(input, ctx):
            calls.append("specific")
            return Result.ok()

        async def pattern(input, ctx):
            calls.append("pattern")
            return Result.ok()

        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .subscribe(ev.handle(specific))
            .subscribe_pattern("items.", pattern)
            .build()
        )

        client = session.create_client("c1")
        result = await client.dispatch(EventDispatchRequest("u1", ev, {"itemId": "1"}))

        assert result.is_success()
        assert sorted(calls) == ["pattern", "specific"]


# ---------------------------------------------------------------------------
# accept_incoming_event
# ---------------------------------------------------------------------------


class TestAcceptIncomingEvent:
    @pytest.mark.asyncio
    async def test_routes_serialized_event_to_subscribers(self):
        ev = ItemCreatedEvent()
        received = {}

        async def handler(input, ctx):
            received["input"] = input
            return Result.ok()

        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .subscribe(ev.handle(handler))
            .build()
        )

        incoming = TransportEvent(
            ev.id, ev.verb, "remote-client", "u-99",
            "tx-99", "tenant", "en-GB",
            Characters(), [], [],
            {"itemId": "999"},
            None,
            correlation_id="corr-99",
        ).assign_serializer(json_serializer)

        result = await session.accept_incoming_event(incoming.serialize())

        assert result.is_success()
        assert received["input"] == {"itemId": "999"}

    @pytest.mark.asyncio
    async def test_correlation_id_survives_serialize_accept_boundary(self):
        ev = ItemCreatedEvent()
        captured = {}

        async def handler(input, ctx):
            captured["v"] = ctx.call.correlation_id
            return Result.ok()

        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .subscribe(ev.handle(handler))
            .build()
        )

        incoming = TransportEvent(
            ev.id, ev.verb, "rc", "u", "tx", "dt", "en-GB",
            Characters(), [], [],
            {"itemId": "x"},
            None,
            correlation_id="my-correlation",
        ).assign_serializer(json_serializer)

        await session.accept_incoming_event(incoming.serialize())
        assert captured["v"] == "my-correlation"


# ---------------------------------------------------------------------------
# accept_event (out-of-context)
# ---------------------------------------------------------------------------


class TestAcceptEventOutOfContext:
    @pytest.mark.asyncio
    async def test_routes_out_of_context_event_to_subscribers(self):
        ev = ItemCreatedEvent()
        received = {}

        async def handler(input, ctx):
            received["input"] = input
            return Result.ok()

        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .subscribe(ev.handle(handler))
            .build()
        )

        ooce = OutOfContextEvent(
            usage_id="u-1",
            event_id=ev.id,
            event_type=ev.verb,
            request_json={"itemId": "abc"},
            correlation_id="corr-1",
        )

        result = await session.accept_event(ooce)

        assert result.is_success()
        assert received["input"] == {"itemId": "abc"}


# ---------------------------------------------------------------------------
# dispatch timeout
# ---------------------------------------------------------------------------


class TestDispatchTimeout:
    @pytest.mark.asyncio
    async def test_returns_timeout_error_when_subscriber_slow(self):
        ev = ItemCreatedEvent()

        async def slow(input, ctx):
            await asyncio.sleep(0.2)
            return Result.ok()

        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .subscribe(ev.handle(slow))
            .build()
        )

        client = session.create_client("c1")
        result = await client.dispatch(
            EventDispatchRequest("u1", ev, {"itemId": "x"}),
            timeout_ms=20,
        )

        assert not result.is_success()
        assert result.error.code == "TransportSession.DispatchTimeout"
        assert result.status_code == 504

    @pytest.mark.asyncio
    async def test_succeeds_when_subscriber_fast(self):
        ev = ItemCreatedEvent()

        async def fast(input, ctx):
            return Result.ok()

        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .subscribe(ev.handle(fast))
            .build()
        )

        client = session.create_client("c1")
        result = await client.dispatch(
            EventDispatchRequest("u1", ev, {"itemId": "x"}),
            timeout_ms=500,
        )

        assert result.is_success()


# ---------------------------------------------------------------------------
# with_correlation_id
# ---------------------------------------------------------------------------


class TestWithCorrelationId:
    def test_returns_new_client(self):
        session = Transport.session("svc").assign_serializer(json_serializer).build()
        c1 = session.create_client("c1")
        c2 = c1.with_correlation_id("corr-42")
        assert c2 is not c1

    @pytest.mark.asyncio
    async def test_correlation_id_flows_into_subscriber_context(self):
        ev = ItemCreatedEvent()
        captured = {}

        async def handler(input, ctx):
            captured["v"] = ctx.call.correlation_id
            return Result.ok()

        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .subscribe(ev.handle(handler))
            .build()
        )

        client = session.create_client("c1").with_correlation_id("corr-42")
        await client.dispatch(EventDispatchRequest("u1", ev, {"itemId": "x"}))

        assert captured["v"] == "corr-42"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _call_info_with(correlation_id=None):
    from peercolab_engine import CallInformation
    info = CallInformation(
        locale="en-GB",
        data_tenant="t",
        characters=Characters(),
        attributes=[],
        path_params=[],
        transaction_id="tx-1",
        correlation_id=correlation_id,
    )
    return info
