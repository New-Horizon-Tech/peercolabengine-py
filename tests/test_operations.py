from __future__ import annotations
import pytest
from peercolab_engine import (
    TransportOperation, RequestOperation, MessageOperation,
    RequestOperationHandler, MessageOperationHandler, Result,
    RequestOperationRequest, MessageOperationRequest, OperationInformation,
    OPERATION_VERBS, TransportOperationSettings, TransportOperationCharacterSetup,
    OutOfContextOperation, OutOfContextOperationPathParameter,
)


class MyRequestOp(RequestOperation):
    def __init__(self):
        super().__init__("my.request", "GET", ["id"],
                         TransportOperationSettings(True, TransportOperationCharacterSetup()))


class MyMessageOp(MessageOperation):
    def __init__(self):
        super().__init__("my.message", "PROCESS")


class TestTransportOperation:
    def test_stores_type_id_verb_path_parameters_settings(self):
        op = TransportOperation("request", "op.id", "GET", ["p1"],
                                TransportOperationSettings(False, TransportOperationCharacterSetup()))
        assert op.type == "request"
        assert op.id == "op.id"
        assert op.verb == "GET"
        assert op.path_parameters == ["p1"]
        assert op.settings.requires_tenant is False


class TestRequestOperation:
    def test_sets_type_to_request(self):
        op = MyRequestOp()
        assert op.type == "request"
        assert op.id == "my.request"
        assert op.verb == "GET"
        assert op.path_parameters == ["id"]
        assert op.settings.requires_tenant is True

    @pytest.mark.asyncio
    async def test_handle_creates_request_operation_handler(self):
        op = MyRequestOp()
        handler = op.handle(lambda input, ctx: Result.ok({"a": "ok"}))
        assert isinstance(handler, RequestOperationHandler)
        assert handler.operation is op


class TestMessageOperation:
    def test_sets_type_to_message(self):
        op = MyMessageOp()
        assert op.type == "message"
        assert op.id == "my.message"

    @pytest.mark.asyncio
    async def test_handle_creates_message_operation_handler(self):
        op = MyMessageOp()
        handler = op.handle(lambda input, ctx: Result.ok())
        assert isinstance(handler, MessageOperationHandler)
        assert handler.operation is op


class TestOperationRequest:
    def test_as_operation_information_maps_correctly(self):
        op = MyRequestOp()
        req = RequestOperationRequest("usage-1", op, {"q": "test"})
        info = req.as_operation_information("client-1")
        assert isinstance(info, OperationInformation)
        assert info.id == "my.request"
        assert info.verb == "GET"
        assert info.type == "request"
        assert info.calling_client == "client-1"
        assert info.usage_id == "usage-1"


class TestTransportOperationSettings:
    def test_stores_requires_tenant_and_character_setup(self):
        op = MyRequestOp()
        assert op.settings.requires_tenant is True
        assert op.settings.character_setup is not None


class TestOutOfContextOperation:
    def test_stores_all_properties(self):
        op = OutOfContextOperation(
            usage_id="u1",
            operation_id="op.1",
            operation_verb="GET",
            operation_type="request",
            request_json={"data": 1},
            path_parameters=[OutOfContextOperationPathParameter("id", "42")],
        )
        assert op.usage_id == "u1"
        assert op.operation_id == "op.1"
        assert op.operation_verb == "GET"
        assert op.operation_type == "request"
        assert op.request_json == {"data": 1}
        assert len(op.path_parameters) == 1
        assert op.path_parameters[0].name == "id"
        assert op.path_parameters[0].value == "42"

    def test_path_parameters_default_to_none(self):
        op = OutOfContextOperation(
            usage_id="u1",
            operation_id="op.1",
            operation_verb="GET",
            operation_type="request",
            request_json={},
        )
        assert op.path_parameters is None


class TestOperationVerbValues:
    def test_all_expected_verbs_exist(self):
        verbs = [
            "GET", "SEARCH", "CREATE", "ADD", "UPDATE", "PATCH",
            "REMOVE", "DELETE", "START", "STOP", "PROCESS", "NAVIGATETO",
        ]
        assert len(verbs) == 12


class TestRequestMessageOperationRequest:
    def test_stores_usage_id_operation_input(self):
        op = MyRequestOp()
        req = RequestOperationRequest("u1", op, {"q": "hi"})
        assert req.usage_id == "u1"
        assert req.operation is op
        assert req.input == {"q": "hi"}

    def test_message_operation_request_stores_fields(self):
        op = MyMessageOp()
        req = MessageOperationRequest("u2", op, {"text": "msg"})
        assert req.usage_id == "u2"
        assert req.input == {"text": "msg"}
