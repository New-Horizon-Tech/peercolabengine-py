from __future__ import annotations
from peercolab_engine import TransportError, TransportErrorDetails


class TestTransportErrorConstructor:
    def test_accepts_string_details(self):
        e = TransportError("CODE", "some error")
        assert e.code == "CODE"
        assert e.details.technical_error == "some error"

    def test_accepts_dict_details(self):
        e = TransportError("CODE", {"technicalError": "tech", "userError": "user"})
        assert e.details.technical_error == "tech"
        assert e.details.user_error == "user"

    def test_defaults_related_to_empty_list(self):
        e = TransportError("C", "err")
        assert e.related == []

    def test_defaults_parent_to_none(self):
        e = TransportError("C", "err")
        assert e.parent is None


class TestTransportErrorBasic:
    def test_creates_error_with_code_and_technical_error(self):
        e = TransportError.basic("ERR_CODE", "tech error", "user error")
        assert e.code == "ERR_CODE"
        assert e.details.technical_error == "tech error"
        assert e.details.user_error == "user error"

    def test_creates_error_with_related_errors(self):
        related = TransportError.basic("R1", "related")
        e = TransportError.basic("MAIN", "main", None, [related])
        assert len(e.related) == 1
        assert e.related[0].code == "R1"


class TestTransportErrorFromParent:
    def test_creates_error_with_parent_chain(self):
        parent = TransportError.basic("PARENT", "parent error")
        child = TransportError.from_parent(parent, "CHILD", "child error")
        assert child.code == "CHILD"
        assert child.parent is parent


class TestToShortString:
    def test_returns_code_technical_error(self):
        e = TransportError.basic("CODE", "tech error")
        assert e.to_short_string() == "CODE - tech error"

    def test_returns_just_code_when_no_technical_error(self):
        e = TransportError("CODE", {"technicalError": ""})
        assert e.to_short_string() == "CODE"


class TestToString:
    def test_includes_related_errors(self):
        r = TransportError.basic("R1", "related1")
        e = TransportError.basic("MAIN", "main", None, [r])
        s = e.to_string()
        assert "MAIN - main" in s
        assert "Related errors" in s
        assert "R1 - related1" in s

    def test_returns_short_string_when_no_related(self):
        e = TransportError.basic("CODE", "tech")
        assert e.to_string() == "CODE - tech"


class TestTransportErrorDetailsAllProperties:
    def test_all_properties_can_be_set(self):
        e = TransportError("CODE", {
            "technicalError": "tech",
            "userError": "user",
            "sessionIdentifier": "sess",
            "callingClient": "client",
            "callingUsage": "usage",
            "calledOperation": "op",
            "transactionId": "tx",
        })
        assert e.details.technical_error == "tech"
        assert e.details.user_error == "user"
        assert e.details.session_identifier == "sess"
        assert e.details.calling_client == "client"
        assert e.details.calling_usage == "usage"
        assert e.details.called_operation == "op"
        assert e.details.transaction_id == "tx"


class TestToLongString:
    def test_includes_parent_error_info(self):
        parent = TransportError.basic("PARENT", "parent tech")
        child = TransportError.from_parent(parent, "CHILD", "child tech")
        s = child.to_long_string()
        assert "CHILD - child tech" in s
        assert "Parent error" in s
        assert "PARENT - parent tech" in s

    def test_includes_detail_fields_when_present(self):
        e = TransportError("CODE", {
            "technicalError": "tech",
            "transactionId": "tx-1",
            "sessionIdentifier": "sess-1",
            "callingClient": "client-1",
            "callingUsage": "usage-1",
            "calledOperation": "op-1",
        })
        s = e.to_long_string()
        assert "TransactionId: tx-1" in s
        assert "Session: sess-1" in s
        assert "Client: client-1" in s
        assert "Usage: usage-1" in s
        assert "Operation: op-1" in s
