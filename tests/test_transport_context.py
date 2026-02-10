from __future__ import annotations
import pytest
import json
from peercolab_engine import (
    TransportContext, TransportRequest, CallInformation, OperationInformation,
    Attribute, DefaultTransportSerializer, Result, Characters,
)

json_serializer = DefaultTransportSerializer()


def make_transport_request():
    return TransportRequest(
        "op-1", "GET", "request", "client-1", "usage-1",
        "tx-1", "tenant-1", "en-GB",
        Characters(),
        [Attribute("attr1", "val1")],
        [Attribute("param1", "pval1")],
        {"data": "test"},
        None,
    ).assign_serializer(json_serializer)


class TestTransportContext:
    def test_from_creates_context_from_transport_request(self):
        tr = make_transport_request()
        ctx = TransportContext.from_request(tr)
        assert ctx.operation.id == "op-1"
        assert ctx.operation.verb == "GET"
        assert ctx.operation.type == "request"
        assert ctx.operation.calling_client == "client-1"
        assert ctx.call.locale == "en-GB"
        assert ctx.call.data_tenant == "tenant-1"
        assert ctx.call.transaction_id == "tx-1"

    def test_from_throws_without_serializer(self):
        tr = TransportRequest(
            "op", "GET", "request", "c", "u", "tx", "dt", "en",
            Characters(), [], [], {}, None,
        )
        with pytest.raises(RuntimeError, match="Serializer required"):
            TransportContext.from_request(tr)

    def test_has_attribute_get_attribute(self):
        tr = make_transport_request()
        ctx = TransportContext.from_request(tr)
        assert ctx.has_attribute("attr1") is True
        assert ctx.get_attribute("attr1") == "val1"
        assert ctx.has_attribute("missing") is False

    def test_has_path_parameter_get_path_parameter(self):
        tr = make_transport_request()
        ctx = TransportContext.from_request(tr)
        assert ctx.has_path_parameter("param1") is True
        assert ctx.get_path_parameter("param1") == "pval1"
        assert ctx.has_path_parameter("missing") is False

    def test_serialize_request_produces_a_json_string(self):
        tr = make_transport_request()
        ctx = TransportContext.from_request(tr)
        json_str = ctx.serialize_request({"hello": "world"})
        assert isinstance(json_str, str)
        parsed = json.loads(json_str)
        assert parsed["requestJson"] == {"hello": "world"}

    def test_deserialize_result_round_trips(self):
        r = Result.ok({"x": 1})
        r.assign_serializer(json_serializer)
        json_str = r.serialize()
        tr = make_transport_request()
        ctx = TransportContext.from_request(tr)
        restored = ctx.deserialize_result(json_str)
        assert restored.is_success() is True
        assert restored.value == {"x": 1}


class TestAttribute:
    def test_constructor_sets_name_and_value(self):
        attr = Attribute("key", "val")
        assert attr.name == "key"
        assert attr.value == "val"


class TestCallInformation:
    def test_new_creates_with_defaults(self):
        ci = CallInformation.new("en-US")
        assert ci.locale == "en-US"
        assert ci.data_tenant == ""
        assert ci.attributes == []
        assert ci.path_params == []
        assert ci.transaction_id is not None

    def test_new_accepts_optional_data_tenant_and_transaction_id(self):
        ci = CallInformation.new("en-US", "my-tenant", "my-tx")
        assert ci.data_tenant == "my-tenant"
        assert ci.transaction_id == "my-tx"


class TestOperationInformation:
    def test_stores_all_fields(self):
        oi = OperationInformation("id1", "GET", "request", "client", "usage")
        assert oi.id == "id1"
        assert oi.verb == "GET"
        assert oi.type == "request"
        assert oi.calling_client == "client"
        assert oi.usage_id == "usage"
