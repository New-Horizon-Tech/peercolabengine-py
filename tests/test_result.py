from __future__ import annotations
import pytest
from peercolab_engine import (
    Result, Metavalues, Metavalue, TransportError,
    DefaultTransportSerializer, CharacterMetaValues, Identifier,
    ResultPassthroughAsync,
)

json_serializer = DefaultTransportSerializer()


class TestResultOk:
    def test_creates_success_result_without_value(self):
        r = Result.ok()
        assert r.is_success() is True
        assert r.has_error() is False
        assert r.status_code == 200
        assert r.value is None

    def test_creates_success_result_with_value(self):
        r = Result.ok({"name": "test"})
        assert r.is_success() is True
        assert r.value == {"name": "test"}

    def test_creates_success_result_with_meta(self):
        meta = Metavalues()
        meta.set_has_more_values(True)
        r = Result.ok("val", meta)
        assert r.meta.has_more_values is True


class TestResultOkStatus:
    def test_creates_success_with_specific_status_code(self):
        r = Result.ok_status(201)
        assert r.is_success() is True
        assert r.status_code == 201
        assert r.value is None

    def test_creates_success_with_308(self):
        r = Result.ok_status(308)
        assert r.is_success() is True


class TestResultFailed:
    def test_creates_a_failed_result(self):
        r = Result.failed(500, "ERR_CODE", "tech error", "user error")
        assert r.is_success() is False
        assert r.has_error() is True
        assert r.status_code == 500
        assert r.error.code == "ERR_CODE"
        assert r.error.details.technical_error == "tech error"
        assert r.error.details.user_error == "user error"

    def test_accepts_a_transport_error_directly(self):
        err = TransportError.basic("CODE", "tech")
        r = Result.failed(500, err)
        assert r.error is err


class TestResultNotFound:
    def test_returns_404_result(self):
        r = Result.not_found("NOT_FOUND")
        assert r.status_code == 404
        assert r.is_success() is False


class TestResultBadRequest:
    def test_returns_400_result(self):
        r = Result.bad_request("BAD_REQ")
        assert r.status_code == 400
        assert r.is_success() is False


class TestResultInternalServerError:
    def test_returns_500_result(self):
        r = Result.internal_server_error("ISE")
        assert r.status_code == 500
        assert r.is_success() is False


class TestStatusCodeRanges:
    def test_200_308_are_success(self):
        for code in [200, 201, 204, 300, 308]:
            r = Result.ok_status(code)
            assert r.is_success() is True

    def test_auto_creates_error_for_non_success_status_codes(self):
        r = Result(value=None, status_code=400, success=False, meta=Metavalues())
        assert r.has_error() is True
        assert r.error.code == "400"


class TestResultSerializeDeserialize:
    def test_throws_without_serializer(self):
        r = Result.ok("hello")
        with pytest.raises(RuntimeError, match="No serializer assigned to Result"):
            r.serialize()

    def test_throws_deserialize_without_serializer(self):
        r = Result.ok("hello")
        with pytest.raises(RuntimeError, match="No serializer assigned to Result"):
            r.deserialize("anything")

    def test_round_trips_through_serialize_deserialize(self):
        r = Result.ok({"foo": "bar"})
        r.assign_serializer(json_serializer)
        json_str = r.serialize()
        r2 = r.deserialize(json_str)
        assert r2.is_success() is True
        assert r2.value == {"foo": "bar"}


class TestMetaManipulation:
    def test_set_meta_replaces_meta(self):
        r = Result.ok()
        meta = Metavalues()
        meta.set_has_more_values(True)
        r.set_meta(meta)
        assert r.meta.has_more_values is True

    def test_with_meta_allows_handler_to_modify(self):
        r = Result.ok()
        r.with_meta(lambda m: m.set_total_value_count(42))
        assert r.meta.total_value_count == 42

    def test_add_meta_value_adds_a_single_metavalue(self):
        r = Result.ok()
        mv = Metavalue()
        mv.value_id = "v1"
        r.add_meta_value(mv)
        assert len(r.meta.values) == 1
        assert r.meta.values[0].value_id == "v1"

    def test_add_meta_values_adds_multiple(self):
        r = Result.ok()
        mv1 = Metavalue(); mv1.value_id = "a"
        mv2 = Metavalue(); mv2.value_id = "b"
        r.add_meta_values([mv1, mv2])
        assert len(r.meta.values) == 2


class TestConvert:
    def test_returns_error_when_conversion_fails_without_serializer_and_value_present(self):
        r = Result.ok({"x": 1})
        converted = r.convert()
        assert converted.is_success() is False

    def test_convert_returns_self_when_value_is_none(self):
        r = Result.ok(None)
        converted = r.convert()
        assert converted is r


class TestConvertToEmptyAsGeneric:
    def test_convert_to_empty_on_success_returns_ok_with_none_value(self):
        r = Result.ok({"data": 1})
        e = r.convert_to_empty()
        assert e.is_success() is True
        assert e.value is None

    def test_convert_to_empty_on_failure_preserves_error(self):
        r = Result.failed(500, "ERR")
        e = r.convert_to_empty()
        assert e.is_success() is False
        assert e.error.code == "ERR"

    def test_as_generic_returns_result(self):
        r = Result.ok()
        g = r.as_generic()
        assert g.is_success() is True


class TestMaybe:
    def test_calls_on_success_when_result_is_success(self):
        r = Result.ok(10)
        r2 = r.maybe(lambda v, m: Result.ok(v * 2))
        assert r2.value == 20

    def test_skips_on_success_when_result_is_failure(self):
        r = Result.failed(500, "ERR")
        r2 = r.maybe(lambda v, m: Result.ok(v * 2))
        assert r2.is_success() is False

    def test_catches_exceptions_and_returns_error_result(self):
        r = Result.ok(10)
        def boom(v, m):
            raise RuntimeError("boom")
        r2 = r.maybe(boom)
        assert r2.is_success() is False
        assert r2.status_code == 500


class TestMaybeOk:
    def test_runs_handler_on_success_and_returns_original_value(self):
        called = []
        r = Result.ok(42)
        r2 = r.maybe_ok(lambda v, m: called.append(True))
        assert len(called) == 1
        assert r2.value == 42

    def test_skips_handler_on_failure(self):
        called = []
        r = Result.failed(400, "ERR")
        r.maybe_ok(lambda v, m: called.append(True))
        assert len(called) == 0

    def test_catches_exceptions(self):
        def fail(v, m):
            raise RuntimeError("fail")
        r = Result.ok(1)
        r2 = r.maybe_ok(fail)
        assert r2.is_success() is False


class TestMaybePassThrough:
    def test_returns_original_on_success_path(self):
        r = Result.ok(99)
        r2 = r.maybe_pass_through(lambda v, m: Result.ok("ignored"))
        assert r2.value == 99

    def test_returns_failure_if_inner_fails(self):
        r = Result.ok(99)
        r2 = r.maybe_pass_through(lambda v, m: Result.failed(400, "BAD"))
        assert r2.is_success() is False

    def test_returns_self_on_failure_without_calling_handler(self):
        r = Result.failed(500, "ERR")
        called = []
        r2 = r.maybe_pass_through(lambda v, m: (called.append(True), Result.ok())[1])
        assert len(called) == 0
        assert r2 is r

    def test_catches_exceptions(self):
        r = Result.ok(1)
        def oops(v, m):
            raise RuntimeError("oops")
        r2 = r.maybe_pass_through(oops)
        assert r2.is_success() is False


class TestMaybePassThroughOk:
    def test_returns_ok_with_original_value_on_success(self):
        r = Result.ok(5)
        r2 = r.maybe_pass_through_ok(lambda v, m: None)
        assert r2.value == 5
        assert r2.is_success() is True

    def test_returns_self_on_failure(self):
        r = Result.failed(400, "ERR")
        r2 = r.maybe_pass_through_ok(lambda v, m: None)
        assert r2 is r

    def test_catches_exceptions(self):
        r = Result.ok(1)
        def boom(v, m):
            raise RuntimeError("boom")
        r2 = r.maybe_pass_through_ok(boom)
        assert r2.is_success() is False


class TestMaybePassThroughAsyncVariants:
    @pytest.mark.asyncio
    async def test_async_maybe_on_success_calls_handler(self):
        r = Result.ok(10)
        result = await ResultPassthroughAsync.start_with(
            lambda: _async_return(r.maybe(lambda v, m: Result.ok(v * 3)))
        ).run()
        assert result.value == 30

    @pytest.mark.asyncio
    async def test_async_maybe_on_failure_skips_handler(self):
        r = Result.failed(400, "ERR")
        result = await ResultPassthroughAsync.start_with(
            lambda: _async_return(r.maybe(lambda v, m: Result.ok(v * 3)))
        ).run()
        assert result.is_success() is False


class TestCopyConstructorBehavior:
    def test_constructor_copies_all_fields(self):
        meta = Metavalues()
        meta.set_has_more_values(True)
        copy = Result(value="hello", status_code=200, success=True, meta=meta)
        assert copy.value == "hello"
        assert copy.status_code == 200
        assert copy.is_success() is True
        assert copy.meta.has_more_values is True

    def test_constructor_with_error_and_no_status_code_defaults_to_500(self):
        err = TransportError.basic("ERR", "tech")
        r = Result(value=None, status_code=0, success=False, meta=Metavalues(), error=err)
        assert r.status_code == 500
        assert r.error is err


class TestDeserializeResult:
    def test_fully_deserializes_including_meta_characters_and_error_chains(self):
        from datetime import datetime
        original = Result.ok({"id": 1})
        original.add_meta_value(
            Metavalue.with_values("v1", "tenant1", Identifier("p1", "user"), datetime.now(), Identifier("p2", "admin"))
        )
        original.meta.set_has_more_values(True)
        original.meta.set_total_value_count(10)
        original.meta.with_attribute("key", "val")
        original.assign_serializer(json_serializer)
        json_str = original.serialize()
        restored = Result.deserialize_result(json_serializer, json_str)
        assert restored.is_success() is True
        assert restored.value == {"id": 1}
        assert restored.meta.has_more_values is True
        assert restored.meta.total_value_count == 10
        assert len(restored.meta.values) == 1
        assert restored.meta.values[0].value_id == "v1"
        assert restored.meta.values[0].initial_characters.performer.id == "p1"
        assert restored.meta.values[0].current_characters.performer.id == "p2"
        assert restored.meta.has_attribute("key") is True

    def test_deserializes_error_chains(self):
        parent = TransportError.basic("PARENT", "parent tech")
        related = TransportError.basic("RELATED", "related tech")
        err = TransportError("MAIN", {"technicalError": "main tech"}, [related], parent)
        r = Result.failed(500, err)
        r.assign_serializer(json_serializer)
        json_str = r.serialize()
        restored = Result.deserialize_result(json_serializer, json_str)
        assert restored.error.code == "MAIN"
        assert restored.error.parent.code == "PARENT"
        assert len(restored.error.related) == 1
        assert restored.error.related[0].code == "RELATED"


async def _async_return(value):
    return value
