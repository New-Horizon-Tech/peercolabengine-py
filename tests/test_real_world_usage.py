from __future__ import annotations
from datetime import datetime
import json
import pytest
from peercolab_engine import (
    Transport, TransportSession, TransportClient, TransportContext, TransportRequest,
    Result, Metavalues, Metavalue, CharacterMetaValues, Identifier,
    TransportError, Attribute, InMemoryContextCache, CallInformation,
    RequestOperation, MessageOperation, RequestOperationRequest, MessageOperationRequest,
    OutboundSessionBuilder, OutboundClientFactory, ResultPassthroughAsync,
    OutOfContextOperation, OutOfContextOperationPathParameter,
    TransportOperationSettings, TransportOperationCharacterSetup,
    TransportOperationCharacter, TransportSessionBuilder, TransportOperation,
    DefaultTransportSerializer, Characters,
)

json_serializer = DefaultTransportSerializer()


class GetPet(RequestOperation):
    def __init__(self):
        super().__init__(
            "petkey.pet.get", "GET", ["petId"],
            TransportOperationSettings(
                requires_tenant=True,
                character_setup=TransportOperationCharacterSetup(
                    performer=TransportOperationCharacter(required=True, valid_types=["user"]),
                ),
            ),
        )


class UpdatePet(MessageOperation):
    def __init__(self):
        super().__init__(
            "petkey.pet.update", "UPDATE", ["petId"],
            TransportOperationSettings(
                requires_tenant=True,
                character_setup=TransportOperationCharacterSetup(
                    performer=TransportOperationCharacter(required=True),
                ),
            ),
        )


class CreatePet(RequestOperation):
    def __init__(self):
        super().__init__(
            "petkey.pet.create", "CREATE", [],
            TransportOperationSettings(
                requires_tenant=True,
                character_setup=TransportOperationCharacterSetup(
                    performer=TransportOperationCharacter(required=True),
                ),
            ),
        )


class ExtractInfo(RequestOperation):
    def __init__(self):
        super().__init__(
            "petkey.agent.extractInfo", "PROCESS", [],
            TransportOperationSettings(
                requires_tenant=True,
                character_setup=TransportOperationCharacterSetup(
                    performer=TransportOperationCharacter(required=True),
                ),
            ),
        )


class TestOperationHandlePattern:
    @pytest.mark.asyncio
    async def test_registers_request_operation_and_executes(self):
        get_pet = GetPet()
        session = (
            Transport.session("PetKeyServer")
            .assign_serializer(json_serializer)
            .intercept(get_pet.handle(lambda input, ctx: _async(Result.ok({"name": "Buddy", "breed": "Labrador"}))))
            .build()
        )
        client = session.create_client("MobileClient")
        result = await client.request(RequestOperationRequest("u1", get_pet, {"petId": "pet-1"}))
        assert result.is_success() is True
        assert result.value == {"name": "Buddy", "breed": "Labrador"}

    @pytest.mark.asyncio
    async def test_registers_message_operation_and_executes(self):
        update_pet = UpdatePet()
        session = (
            Transport.session("PetKeyServer")
            .assign_serializer(json_serializer)
            .intercept(update_pet.handle(lambda input, ctx: _async(Result.ok())))
            .build()
        )
        client = session.create_client("MobileClient")
        result = await client.request(MessageOperationRequest("u1", update_pet, {"petId": "pet-1", "name": "Rex"}))
        assert result.is_success() is True

    def test_operation_carries_settings(self):
        op = GetPet()
        assert op.settings is not None
        assert op.settings.requires_tenant is True
        assert op.settings.character_setup.performer is not None
        assert op.settings.character_setup.performer.required is True
        assert op.settings.character_setup.performer.valid_types == ["user"]

    def test_operation_carries_path_parameters(self):
        op = GetPet()
        assert op.path_parameters == ["petId"]

    @pytest.mark.asyncio
    async def test_multiple_operations_on_same_session(self):
        get_pet = GetPet()
        create_pet = CreatePet()
        update_pet = UpdatePet()
        session = (
            Transport.session("PetKeyServer")
            .assign_serializer(json_serializer)
            .intercept(get_pet.handle(lambda input, ctx: _async(Result.ok({"name": "Buddy", "breed": "Lab"}))))
            .intercept(create_pet.handle(lambda input, ctx: _async(Result.ok({"id": "new-" + input["name"]}))))
            .intercept(update_pet.handle(lambda input, ctx: _async(Result.ok())))
            .build()
        )
        client = session.create_client("MobileClient")
        r1 = await client.request(RequestOperationRequest("u1", get_pet, {"petId": "p1"}))
        assert r1.value == {"name": "Buddy", "breed": "Lab"}
        r2 = await client.request(RequestOperationRequest("u2", create_pet, {"name": "Rex", "breed": "Poodle"}))
        assert r2.value == {"id": "new-Rex"}
        r3 = await client.request(MessageOperationRequest("u3", update_pet, {"petId": "p1", "name": "New Name"}))
        assert r3.is_success() is True


class TestOutboundBuilderIntercept:
    @pytest.mark.asyncio
    async def test_outbound_builder_intercept_with_handle(self):
        extract_op = ExtractInfo()
        builder = Transport.session("InboundServer").assign_serializer(json_serializer)
        outbound = (
            builder.outbound_session_builder("AgentService")
            .intercept(extract_op.handle(lambda input, ctx: _async(Result.ok({"extracted": "data-" + input["fileId"]}))))
            .build()
        )
        client = outbound.as_independent_requests()
        result = await client.request(RequestOperationRequest("u1", extract_op, {"fileId": "f1"}))
        assert result.is_success() is True
        assert result.value == {"extracted": "data-f1"}

    @pytest.mark.asyncio
    async def test_outbound_intercept_pattern(self):
        builder = Transport.session("InboundServer").assign_serializer(json_serializer)
        outbound = (
            builder.outbound_session_builder("AgentService")
            .intercept_pattern("petkey.agent.", lambda input, ctx: _async(Result.ok({"routed": True})))
            .build()
        )
        client = outbound.as_independent_requests()
        op = TransportOperation("request", "petkey.agent.extractInfo", "PROCESS")
        result = await client.request(RequestOperationRequest("u1", op, {"fileId": "f1"}))
        assert result.is_success() is True
        assert result.value["routed"] is True

    @pytest.mark.asyncio
    async def test_for_incoming_request_propagates_cached_context(self):
        cache = InMemoryContextCache()
        call_info = CallInformation.new("nb-NO", "tenant-1", "tx-inbound")
        call_info.characters = {"performer": {"id": "user-1", "type": "user"}}
        call_info.attributes = [Attribute("userId", "u123")]
        call_info.path_params = [Attribute("petId", "pet-42")]
        await cache.put("tx-inbound", call_info)

        captured_ctx = [None]
        get_pet = GetPet()

        async def handler(input, ctx):
            captured_ctx[0] = ctx
            return Result.ok({"name": "Buddy", "breed": "Lab"})

        outbound = (
            OutboundSessionBuilder("PetService", cache, json_serializer)
            .intercept(get_pet.handle(handler))
            .build()
        )
        client = await outbound.for_incoming_request("tx-inbound")
        await client.request(RequestOperationRequest("u1", get_pet, {"petId": "pet-42"}))
        assert captured_ctx[0] is not None
        assert captured_ctx[0].call.locale == "nb-NO"
        assert captured_ctx[0].call.data_tenant == "tenant-1"
        assert captured_ctx[0].call.characters["performer"]["id"] == "user-1"
        assert captured_ctx[0].has_attribute("userId") is True
        assert captured_ctx[0].get_attribute("userId") == "u123"
        assert captured_ctx[0].call.transaction_id == "tx-inbound"


class TestAcceptIncomingRequestWithCharacters:
    @pytest.mark.asyncio
    async def test_characters_survive_serialize_accept_round_trip(self):
        captured_ctx = [None]
        get_pet = GetPet()

        async def handler(input, ctx):
            captured_ctx[0] = ctx
            return Result.ok({"name": "Buddy", "breed": "Lab"})

        session = (
            Transport.session("PetSystemServer")
            .assign_serializer(json_serializer)
            .intercept(get_pet.handle(handler))
            .build()
        )
        request = TransportRequest(
            "petkey.pet.get", "GET", "request", "MobileClient", "usage-1",
            "tx-1", "tenant-1", "nb-NO",
            {"performer": {"id": "user-123", "type": "user"}, "responsible": {"id": "org-1", "type": "org"}, "subject": {"id": "pet-1", "type": "pet"}},
            [Attribute("userId", "user-123"), Attribute("username", "john")],
            [Attribute("petId", "pet-1")],
            {"petId": "pet-1"}, None,
        ).assign_serializer(json_serializer)
        result = await session.accept_incoming_request(request.serialize())
        assert result.is_success() is True
        assert captured_ctx[0].call.characters.performer.id == "user-123"
        assert captured_ctx[0].call.characters.performer.type == "user"
        assert captured_ctx[0].call.characters.responsible.id == "org-1"
        assert captured_ctx[0].call.characters.subject.id == "pet-1"

    @pytest.mark.asyncio
    async def test_accept_incoming_request_handles_message_type(self):
        update_pet = UpdatePet()
        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .intercept(update_pet.handle(lambda input, ctx: _async(Result.ok())))
            .build()
        )
        request = TransportRequest(
            "petkey.pet.update", "UPDATE", "message", "client", "u1",
            "tx-1", "", "en-GB", Characters(), [], [],
            {"petId": "p1", "name": "Rex"}, None,
        ).assign_serializer(json_serializer)
        result = await session.accept_incoming_request(request.serialize())
        assert result.is_success() is True


class TestSerializeRequestDeserializeResult:
    @pytest.mark.asyncio
    async def test_interceptor_can_serialize_and_deserialize(self):
        serialized_outbound = [None]
        deserialized_result = [None]
        get_pet = GetPet()

        async def handler(input, ctx):
            serialized_outbound[0] = ctx.serialize_request({"fileId": "f-abc"})
            mock_response = Result.ok({"extracted": "data-from-agent"})
            mock_response.assign_serializer(json_serializer)
            mock_json = mock_response.serialize()
            deserialized_result[0] = ctx.deserialize_result(mock_json)
            return Result.ok({"name": "Buddy", "breed": "Lab"})

        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .intercept(get_pet.handle(handler))
            .build()
        )
        client = session.create_client("c1")
        await client.request(RequestOperationRequest("u1", get_pet, {"petId": "p1"}))
        assert serialized_outbound[0] is not None
        parsed = json.loads(serialized_outbound[0])
        assert parsed["requestJson"] == {"fileId": "f-abc"}
        assert deserialized_result[0] is not None
        assert deserialized_result[0].is_success() is True
        assert deserialized_result[0].value == {"extracted": "data-from-agent"}

    @pytest.mark.asyncio
    async def test_serialize_request_preserves_context(self):
        serialized = [None]
        get_pet = GetPet()

        async def handler(input, ctx):
            serialized[0] = ctx.serialize_request({"outboundData": True})
            return Result.ok({"name": "x", "breed": "y"})

        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .intercept(get_pet.handle(handler))
            .build()
        )
        client = (
            session.create_client("c1", "my-tenant")
            .with_locale("nb-NO")
            .with_characters({"performer": {"id": "user-1", "type": "user"}})
        )
        await client.request(RequestOperationRequest("u1", get_pet, {"petId": "p1"}))
        parsed = json.loads(serialized[0])
        assert parsed["locale"] == "nb-NO"
        assert parsed["dataTenant"] == "my-tenant"
        assert parsed["characters"]["performer"]["id"] == "user-1"
        assert parsed["requestJson"] == {"outboundData": True}


class TestResultFactoryMethods:
    def test_internal_server_error(self):
        r = Result.internal_server_error("SERVER_ERR", "DB connection failed", "Something went wrong")
        assert r.is_success() is False
        assert r.status_code == 500
        assert r.error.code == "SERVER_ERR"
        assert r.error.details.technical_error == "DB connection failed"
        assert r.error.details.user_error == "Something went wrong"

    def test_bad_request(self):
        r = Result.bad_request("INVALID_INPUT", "Missing field: name", "Please provide a name")
        assert r.is_success() is False
        assert r.status_code == 400
        assert r.error.code == "INVALID_INPUT"

    def test_not_found(self):
        r = Result.not_found("PET_NOT_FOUND", "No pet with id pet-99", "Pet not found")
        assert r.is_success() is False
        assert r.status_code == 404

    def test_failed_with_3_args(self):
        r = Result.failed(409, "CONFLICT", "Resource already exists")
        assert r.is_success() is False
        assert r.status_code == 409
        assert r.error.code == "CONFLICT"


class TestMetavalueAttributesSerialization:
    def test_metavalue_custom_attributes_round_trip(self):
        mv = Metavalue.with_values("rec-1", "tenant-1", Identifier("user-1", "user"), datetime(2024, 1, 1))
        mv.with_attribute("createdSource", "mobile")
        mv.with_attribute("modifiedSource", "web")
        result = Result.ok({"id": 1})
        result.add_meta_value(mv)
        result.assign_serializer(json_serializer)
        json_str = result.serialize()
        restored = Result.deserialize_result(json_serializer, json_str)
        assert len(restored.meta.values) == 1
        rmv = restored.meta.values[0]
        assert rmv.has_attribute("createdSource") is True
        assert rmv.get_attribute("createdSource") == "mobile"
        assert rmv.get_attribute("modifiedSource") == "web"

    def test_multiple_metavalues_with_attributes_round_trip(self):
        mv1 = Metavalue.with_values("rec-1", "tenant-1", Identifier("u1", "user"))
        mv1.with_attribute("status", "active")
        mv2 = Metavalue.with_values("rec-2", "tenant-1", Identifier("u2", "admin"))
        mv2.with_attribute("status", "archived")
        result = Result.ok({"items": ["a", "b"]})
        result.add_meta_values([mv1, mv2])
        result.assign_serializer(json_serializer)
        json_str = result.serialize()
        restored = Result.deserialize_result(json_serializer, json_str)
        assert len(restored.meta.values) == 2
        assert restored.meta.get_meta_value("rec-1").get_attribute("status") == "active"
        assert restored.meta.get_meta_value("rec-2").get_attribute("status") == "archived"

    def test_metavalues_iteration_after_deserialization(self):
        meta = Metavalues()
        for i in range(5):
            mv = Metavalue()
            mv.value_id = f"item-{i}"
            mv.data_tenant = "tenant-1"
            meta.add(mv)
        result = Result.ok({"data": True}, meta)
        result.assign_serializer(json_serializer)
        json_str = result.serialize()
        restored = Result.deserialize_result(json_serializer, json_str)
        ids = [v.value_id for v in restored.meta.values]
        assert ids == ["item-0", "item-1", "item-2", "item-3", "item-4"]


class TestTransportRequestCharactersRoundTrip:
    def test_characters_survive_serialize_deserialize(self):
        request = TransportRequest(
            "op1", "GET", "request", "client", "usage1",
            "tx-1", "tenant", "en-GB",
            Characters(
                performer=Identifier("user-1", "user"),
                responsible=Identifier("org-1", "org"),
                subject=Identifier("item-1", "item"),
            ),
            [], [], {"data": "test"}, None,
        ).assign_serializer(json_serializer)
        json_str = request.serialize()
        restored = TransportRequest.from_serialized(json_serializer, json_str)
        assert restored.characters.performer.id == "user-1"
        assert restored.characters.performer.type == "user"
        assert restored.characters.responsible.id == "org-1"
        assert restored.characters.subject.id == "item-1"

    def test_attributes_and_path_params_survive(self):
        request = TransportRequest(
            "op1", "GET", "request", "client", "usage1",
            "tx-1", "tenant", "en-GB", Characters(),
            [Attribute("userId", "u1"), Attribute("fullName", "John")],
            [Attribute("petId", "pet-42")],
            {}, None,
        ).assign_serializer(json_serializer)
        json_str = request.serialize()
        restored = TransportRequest.from_serialized(json_serializer, json_str)
        assert len(restored.attributes) == 2
        assert next(a for a in restored.attributes if a.name == "userId").value == "u1"
        assert len(restored.path_params) == 1
        assert next(a for a in restored.path_params if a.name == "petId").value == "pet-42"


class TestTransportClientGetSerializer:
    def test_returns_session_serializer(self):
        session = Transport.session("svc").assign_serializer(json_serializer).build()
        client = session.create_client("c1")
        assert client.get_serializer() is json_serializer


class TestTransportSessionWithLocale:
    @pytest.mark.asyncio
    async def test_sets_locale_and_propagates(self):
        captured_ctx = [None]
        get_pet = GetPet()

        async def handler(input, ctx):
            captured_ctx[0] = ctx
            return Result.ok({"name": "x", "breed": "y"})

        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .intercept(get_pet.handle(handler))
            .build()
            .with_locale("nb-NO")
        )
        client = session.create_client("c1")
        await client.request(RequestOperationRequest("u1", get_pet, {"petId": "p1"}))
        assert captured_ctx[0].call.locale == "nb-NO"


class TestOnLogMessage:
    def test_sets_custom_logger(self):
        messages = []

        class CaptureLogger:
            log_level = 3
            def write(self, msg):
                messages.append(str(msg))

        session = (
            Transport.session("svc")
            .on_log_message(CaptureLogger())
            .assign_serializer(json_serializer)
            .build()
        )
        assert isinstance(session, TransportSession)


class TestAcceptOperationPathParams:
    @pytest.mark.asyncio
    async def test_path_params_accessible_in_handler(self):
        captured_ctx = [None]
        get_pet = GetPet()

        async def handler(input, ctx):
            captured_ctx[0] = ctx
            return Result.ok({"name": "Buddy", "breed": "Lab"})

        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .intercept(get_pet.handle(handler))
            .build()
        )
        client = session.create_client("c1")
        await client.accept_operation(OutOfContextOperation(
            "u1", "petkey.pet.get", "GET", "request",
            {"petId": "pet-1"},
            [
                OutOfContextOperationPathParameter("petId", "pet-1"),
                OutOfContextOperationPathParameter("dataSource", "remote"),
            ],
        ))
        assert captured_ctx[0].has_path_parameter("petId") is True
        assert captured_ctx[0].get_path_parameter("petId") == "pet-1"
        assert captured_ctx[0].has_path_parameter("dataSource") is True
        assert captured_ctx[0].get_path_parameter("dataSource") == "remote"

    @pytest.mark.asyncio
    async def test_accept_operation_message_type(self):
        update_pet = UpdatePet()
        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .intercept(update_pet.handle(lambda input, ctx: _async(Result.ok())))
            .build()
        )
        client = session.create_client("c1")
        result = await client.accept_operation(OutOfContextOperation(
            "u1", "petkey.pet.update", "UPDATE", "message",
            {"petId": "p1", "name": "Rex"},
        ))
        assert result.is_success() is True

    @pytest.mark.asyncio
    async def test_accept_operation_does_not_overwrite_client_path_params(self):
        captured_ctx = [None]
        get_pet = GetPet()

        async def handler(input, ctx):
            captured_ctx[0] = ctx
            return Result.ok({"name": "x", "breed": "y"})

        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .intercept(get_pet.handle(handler))
            .build()
        )
        client = session.create_client("c1").add_path_param("petId", "from-client")
        await client.accept_operation(OutOfContextOperation(
            "u1", "petkey.pet.get", "GET", "request",
            {"petId": "p1"},
            [OutOfContextOperationPathParameter("petId", "from-operation")],
        ))
        assert captured_ctx[0].get_path_parameter("petId") == "from-client"


class TestResultSerializeDeserializeFullRoundTrip:
    def test_ok_with_full_meta(self):
        meta = Metavalues()
        meta.set_has_more_values(True)
        meta.set_total_value_count(200)
        meta.with_attribute("page", 1)
        meta.with_attribute("filter", "active")

        mv1 = Metavalue.with_values(
            "pet-1", "tenant-1",
            Identifier("user-1", "user"), datetime(2024, 1, 15),
            Identifier("admin-1", "admin"), datetime(2024, 7, 20),
        )
        mv1.with_attribute("createdSource", "mobile")

        mv2 = Metavalue()
        mv2.value_id = "pet-2"
        mv2.data_tenant = "tenant-1"
        mv2.with_initial_characters(
            CharacterMetaValues.from_performer(Identifier("user-2", "user"))
            .with_responsible(Identifier("org-1", "org"))
            .with_subject(Identifier("pet-2", "pet"))
            .with_timestamp(datetime(2024, 4, 10))
        )
        meta.add([mv1, mv2])

        original = Result.ok({"pets": [{"id": "pet-1", "name": "Buddy"}]}, meta)
        original.assign_serializer(json_serializer)
        json_str = original.serialize()
        restored = Result.deserialize_result(json_serializer, json_str)

        assert restored.is_success() is True
        assert len(restored.value["pets"]) == 1
        assert restored.value["pets"][0]["name"] == "Buddy"
        assert restored.meta.has_more_values is True
        assert restored.meta.total_value_count == 200
        assert restored.meta.get_attribute("page") == 1
        assert restored.meta.get_attribute("filter") == "active"
        assert len(restored.meta.values) == 2

        rmv1 = restored.meta.get_meta_value("pet-1")
        assert rmv1.initial_characters.performer.id == "user-1"
        assert rmv1.current_characters.performer.id == "admin-1"
        assert rmv1.get_attribute("createdSource") == "mobile"

        rmv2 = restored.meta.get_meta_value("pet-2")
        assert rmv2.initial_characters.performer.id == "user-2"
        assert rmv2.initial_characters.responsible.id == "org-1"
        assert rmv2.initial_characters.subject.id == "pet-2"

    def test_failed_with_nested_error_chain(self):
        root_err = TransportError.basic("DB_ERROR", "Connection refused", "Database unavailable")
        middle_err = TransportError.from_parent(root_err, "SERVICE_ERROR", "Pet service failed")
        top_err = TransportError.from_parent(middle_err, "API_ERROR", "Request failed", "Something went wrong")
        r = Result.failed(503, top_err)
        r.assign_serializer(json_serializer)
        json_str = r.serialize()
        restored = Result.deserialize_result(json_serializer, json_str)
        assert restored.is_success() is False
        assert restored.status_code == 503
        assert restored.error.code == "API_ERROR"
        assert restored.error.details.user_error == "Something went wrong"
        assert restored.error.parent.code == "SERVICE_ERROR"
        assert restored.error.parent.parent.code == "DB_ERROR"
        assert restored.error.parent.parent.details.technical_error == "Connection refused"
        assert restored.error.parent.parent.details.user_error == "Database unavailable"


class TestEndToEndPipeline:
    @pytest.mark.asyncio
    async def test_full_request_pipeline(self):
        get_pet = GetPet()

        async def remote_service(serialized_request):
            request = TransportRequest.from_serialized(json_serializer, serialized_request)
            meta = Metavalues()
            performer_id = "unknown"
            if hasattr(request.characters, 'performer') and request.characters.performer:
                performer_id = request.characters.performer.id
            elif isinstance(request.characters, dict) and request.characters.get("performer"):
                performer_id = request.characters["performer"].get("id", "unknown")
            mv = Metavalue.with_values("extract-1", None, Identifier(performer_id, "user"))
            mv.with_attribute("source", "ai-agent")
            meta.add(mv)
            response = Result.ok({"extracted": "info-for-" + request.request_json["fileId"]}, meta)
            response.assign_serializer(json_serializer)
            return response.serialize()

        async def handler(input, ctx):
            outbound_json = ctx.serialize_request({"fileId": "file-abc"})
            response_json = await remote_service(outbound_json)
            agent_result = ctx.deserialize_result(response_json)
            if not agent_result.is_success():
                return agent_result.convert()
            return Result.ok(
                {"name": agent_result.value["extracted"], "breed": "unknown"},
                agent_result.meta,
            )

        session = (
            Transport.session("PetKeyServer")
            .assign_serializer(json_serializer)
            .intercept(get_pet.handle(handler))
            .build()
        )
        client = (
            session.create_client("MobileClient", "my-tenant")
            .with_locale("nb-NO")
            .with_characters({"performer": {"id": "user-42", "type": "user"}})
        )
        result = await client.request(RequestOperationRequest("u1", get_pet, {"petId": "pet-1"}))
        assert result.is_success() is True
        assert result.value == {"name": "info-for-file-abc", "breed": "unknown"}
        assert len(result.meta.values) == 1
        assert result.meta.get_meta_value("extract-1") is not None
        assert result.meta.get_meta_value("extract-1").get_attribute("source") == "ai-agent"
        assert result.meta.get_meta_value("extract-1").initial_characters.performer.id == "user-42"

    @pytest.mark.asyncio
    async def test_error_from_outbound_propagates(self):
        get_pet = GetPet()

        async def failing_remote():
            err = TransportError.basic("EXTRACTION_FAILED", "File corrupt", "Could not process file")
            response = Result.failed(422, err)
            response.assign_serializer(json_serializer)
            return response.serialize()

        async def handler(input, ctx):
            response_json = await failing_remote()
            agent_result = ctx.deserialize_result(response_json)
            if not agent_result.is_success():
                return agent_result.convert()
            return Result.ok({"name": "should-not-reach", "breed": "x"})

        session = (
            Transport.session("PetKeyServer")
            .assign_serializer(json_serializer)
            .intercept(get_pet.handle(handler))
            .build()
        )
        client = session.create_client("MobileClient")
        result = await client.request(RequestOperationRequest("u1", get_pet, {"petId": "pet-1"}))
        assert result.is_success() is False
        assert result.error.code == "EXTRACTION_FAILED"
        assert result.error.details.technical_error == "File corrupt"
        assert result.error.details.user_error == "Could not process file"


class TestIsSuccessHasError:
    def test_is_success_true_for_ok(self):
        assert Result.ok().is_success() is True
        assert Result.ok("value").is_success() is True
        assert Result.ok_status(201).is_success() is True

    def test_is_success_false_for_failed(self):
        assert Result.failed(400, "ERR").is_success() is False
        assert Result.bad_request("ERR").is_success() is False
        assert Result.not_found("ERR").is_success() is False
        assert Result.internal_server_error("ERR").is_success() is False

    def test_has_error_false_for_ok(self):
        assert Result.ok().has_error() is False
        assert Result.ok("value").has_error() is False

    def test_has_error_true_for_failed(self):
        assert Result.failed(500, "ERR").has_error() is True
        assert Result.bad_request("ERR").has_error() is True


class TestInspectResponse:
    @pytest.mark.asyncio
    async def test_inspect_response_callback(self):
        inspected = []
        get_pet = GetPet()

        async def resp_inspector(result, input, ctx):
            inspected.append(result)
            return result

        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .intercept(get_pet.handle(lambda input, ctx: _async(Result.ok({"name": "Buddy", "breed": "Lab"}))))
            .inspect_response(resp_inspector)
            .build()
        )
        client = session.create_client("c1")
        await client.request(RequestOperationRequest("u1", get_pet, {"petId": "p1"}))
        await client.request(RequestOperationRequest("u2", get_pet, {"petId": "p2"}))
        assert len(inspected) == 2
        assert inspected[0].is_success() is True


class TestWithCharactersFullSet:
    @pytest.mark.asyncio
    async def test_all_characters_propagate(self):
        captured_ctx = [None]
        get_pet = GetPet()

        async def handler(input, ctx):
            captured_ctx[0] = ctx
            return Result.ok({"name": "x", "breed": "y"})

        session = (
            Transport.session("svc")
            .assign_serializer(json_serializer)
            .intercept(get_pet.handle(handler))
            .build()
        )
        client = session.create_client("c1").with_characters({
            "performer": {"id": "user-1", "type": "user"},
            "responsible": {"id": "org-1", "type": "organization"},
            "subject": {"id": "pet-1", "type": "pet"},
        })
        await client.request(RequestOperationRequest("u1", get_pet, {"petId": "p1"}))
        chars = captured_ctx[0].call.characters
        assert chars["performer"]["id"] == "user-1"
        assert chars["responsible"]["id"] == "org-1"
        assert chars["subject"]["id"] == "pet-1"


class TestMaybeChainMetadata:
    def test_meta_passes_through_maybe_chain(self):
        meta = Metavalues()
        meta.set_has_more_values(True)
        mv = Metavalue.with_values("rec-1", "tenant")
        meta.add(mv)
        result = Result.ok({"id": 1}, meta).maybe(lambda value, m: (
            Result.ok({"doubled": value["id"] * 2})
        ))
        assert result.is_success() is True
        assert result.value == {"doubled": 2}

    def test_maybe_pass_through_ok_receives_meta(self):
        meta = Metavalues()
        meta.with_attribute("key", "val")
        received_meta = [None]
        Result.ok({"id": 1}, meta).maybe_pass_through_ok(lambda value, m: received_meta.__setitem__(0, m))
        assert received_meta[0] is not None
        assert received_meta[0].has_attribute("key") is True


class TestSetupOutboundContextCache:
    @pytest.mark.asyncio
    async def test_sharing_context_cache(self):
        shared_cache = InMemoryContextCache()
        captured_tx = [None]
        get_pet = GetPet()
        extract_op = ExtractInfo()

        async def handler(input, ctx):
            captured_tx[0] = ctx.call.transaction_id
            return Result.ok({"name": "Buddy", "breed": "Lab"})

        builder = (
            Transport.session("InboundServer")
            .assign_serializer(json_serializer)
            .setup_outbound_context_cache(shared_cache)
            .intercept(get_pet.handle(handler))
        )
        outbound = (
            builder.outbound_session_builder("OutboundService")
            .intercept(extract_op.handle(lambda input, ctx: _async(Result.ok({"extracted": "data"}))))
            .build()
        )
        session = builder.build()
        client = session.create_client("c1")
        await client.request(RequestOperationRequest("u1", get_pet, {"petId": "p1"}))
        assert captured_tx[0] is not None
        cached = await shared_cache.get(captured_tx[0])
        assert cached is not None


async def _async(val):
    return val
