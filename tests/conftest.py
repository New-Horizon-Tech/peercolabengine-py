from __future__ import annotations
import pytest
from peercolab_engine import (
    DefaultTransportSerializer,
    DefaultLogger,
    Logger,
    Transport,
    TransportSession,
    TransportSessionBuilder,
    RequestOperation,
    MessageOperation,
    RequestOperationRequest,
    MessageOperationRequest,
    Result,
    TransportContext,
)


@pytest.fixture(autouse=True)
def _reset_logger():
    """Reset Logger to default after each test to prevent cross-test pollution."""
    yield
    Logger.assign_logger(DefaultLogger())

json_serializer = DefaultTransportSerializer()


class GetItemOp(RequestOperation):
    def __init__(self):
        super().__init__("items.get", "GET")


class CreateItemOp(RequestOperation):
    def __init__(self):
        super().__init__("items.create", "CREATE")


class NotifyOp(MessageOperation):
    def __init__(self):
        super().__init__("notify.send", "PROCESS")


def build_client_server_pair(configure_server, server_pattern_prefix="TestApp."):
    server_builder = Transport.session("server-session").assign_serializer(json_serializer)
    configure_server(server_builder)
    server_session = server_builder.build()

    client_session = (
        Transport.session("client-session")
        .assign_serializer(json_serializer)
        .intercept_pattern(
            server_pattern_prefix,
            _make_proxy_handler(server_session),
        )
        .build()
    )
    return {"client_session": client_session, "server_session": server_session}


def _make_proxy_handler(server_session):
    async def handler(input, ctx):
        serialized_request = ctx.serialize_request(input)
        result = await server_session.accept_incoming_request(serialized_request)
        serialized_result = result.serialize()
        return ctx.deserialize_result(serialized_result)
    return handler
