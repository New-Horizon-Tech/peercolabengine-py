from __future__ import annotations
import pytest
from peercolab_engine import (
    TransportRequest, TransportContext, DefaultTransportSerializer,
    OperationInformation, CallInformation, Attribute, Characters,
)

json_serializer = DefaultTransportSerializer()


class TestTransportRequestConstructor:
    def test_sets_all_properties(self):
        tr = TransportRequest(
            "op-1", "GET", "request", "client-1", "usage-1",
            "tx-1", "tenant-1", "en-GB",
            Characters(),
            [Attribute("a1", "v1")],
            [Attribute("p1", "pv1")],
            {"data": "test"},
            "raw-data",
        )
        assert tr.operation_id == "op-1"
        assert tr.operation_verb == "GET"
        assert tr.operation_type == "request"
        assert tr.calling_client == "client-1"
        assert tr.usage_id == "usage-1"
        assert tr.transaction_id == "tx-1"
        assert tr.data_tenant == "tenant-1"
        assert tr.locale == "en-GB"
        assert len(tr.attributes) == 1
        assert len(tr.path_params) == 1
        assert tr.request_json == {"data": "test"}
        assert tr.raw == "raw-data"

    def test_assign_serializer_sets_serializer(self):
        tr = TransportRequest(
            "op", "GET", "request", "c", "u", "tx", "dt", "en",
            Characters(), [], [], {}, None,
        )
        result = tr.assign_serializer(json_serializer)
        assert result is tr
        assert tr.serializer is json_serializer


class TestTransportRequestFrom:
    def test_creates_from_input_context(self):
        ctx = TransportContext(
            OperationInformation("op1", "CREATE", "request", "client-x", "usage-x"),
            CallInformation.new("en-US", "tenant-a", "tx-abc"),
            json_serializer,
        )
        tr = TransportRequest.from_context({"foo": "bar"}, ctx)
        assert tr.operation_id == "op1"
        assert tr.operation_verb == "CREATE"
        assert tr.calling_client == "client-x"
        assert tr.request_json == {"foo": "bar"}
        assert tr.serializer is json_serializer

    def test_generates_uuid_if_transaction_id_is_falsy(self):
        ctx = TransportContext(
            OperationInformation("op1", "GET", "request", "c", "u"),
            CallInformation("en-GB", "", Characters(), [], [], ""),
            json_serializer,
        )
        tr = TransportRequest.from_context({"data": 1}, ctx)
        assert tr.transaction_id
        assert len(tr.transaction_id) > 0


class TestTransportRequestSerializeDeserialize:
    def test_throws_without_serializer(self):
        tr = TransportRequest(
            "op", "GET", "request", "c", "u", "tx", "dt", "en",
            Characters(), [], [], {}, None,
        )
        with pytest.raises(RuntimeError, match="No serializer assigned"):
            tr.serialize()

    def test_round_trips_through_from_serialized(self):
        ctx = TransportContext(
            OperationInformation("op1", "GET", "request", "c", "u"),
            CallInformation.new("en-GB", "dt", "tx-1"),
            json_serializer,
        )
        original = TransportRequest.from_context({"data": 123}, ctx)
        json_str = original.serialize()
        restored = TransportRequest.from_serialized(json_serializer, json_str)
        assert restored.operation_id == "op1"
        assert restored.request_json["data"] == 123
        assert restored.raw == json_str


class TestTransportRequestDeserialize:
    def test_throws_without_serializer_assigned(self):
        tr = TransportRequest(
            "op", "GET", "request", "c", "u", "tx", "dt", "en",
            Characters(), [], [], {}, None,
        )
        with pytest.raises(RuntimeError, match="No serializer assigned"):
            tr._deserialize("anything")
