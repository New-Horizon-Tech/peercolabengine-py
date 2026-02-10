from __future__ import annotations
import pytest
from peercolab_engine import (
    Transport, TransportSession, TransportSessionBuilder, TransportContext,
    TransportRequest, Result, Metavalues, Metavalue, CharacterMetaValues, Identifier,
    TransportError, Attribute, InMemoryContextCache, CallInformation,
    RequestOperation, MessageOperation, RequestOperationRequest, MessageOperationRequest,
    OutboundSessionBuilder, OutboundClientFactory, OutOfContextOperation,
    OutOfContextOperationPathParameter, TransportOperationSettings, TransportOperation,
    DefaultTransportSerializer, Characters,
)

json_serializer = DefaultTransportSerializer()


class GetProduct(RequestOperation):
    def __init__(self):
        super().__init__(
            "TestApp.Products.GetProduct", "GET",
            ["dataSource", "productId"],
            TransportOperationSettings(requires_tenant=True),
        )


class CreateProduct(RequestOperation):
    def __init__(self):
        super().__init__(
            "TestApp.Products.CreateProduct", "CREATE",
            ["dataSource"],
            TransportOperationSettings(requires_tenant=True),
        )


class UpdateProduct(MessageOperation):
    def __init__(self):
        super().__init__(
            "TestApp.Products.UpdateProduct", "UPDATE",
            ["dataSource", "productId"],
            TransportOperationSettings(requires_tenant=True),
        )


class GetProductDetails(RequestOperation):
    def __init__(self):
        super().__init__(
            "TestApp.Products.GetProductDetails", "GET",
            ["dataSource", "productId"],
            TransportOperationSettings(requires_tenant=True),
        )


class SyncTasks(RequestOperation):
    def __init__(self):
        super().__init__(
            "TestApp.Tasks.SyncLocalUpdates", "PROCESS",
            ["dataSource"],
            TransportOperationSettings(requires_tenant=True),
        )


class ProcessChat(RequestOperation):
    def __init__(self):
        super().__init__("PeerColab.Instructions.ProcessChatInstruction", "PROCESS")


def build_client_server_pair(configure_server, server_pattern_prefix="TestApp."):
    server_builder = Transport.session("server-session").assign_serializer(json_serializer)
    configure_server(server_builder)
    server_session = server_builder.build()

    async def proxy(input, ctx):
        serialized_request = ctx.serialize_request(input)
        result = await server_session.accept_incoming_request(serialized_request)
        serialized_result = result.serialize()
        return ctx.deserialize_result(serialized_result)

    client_session = (
        Transport.session("client-session")
        .assign_serializer(json_serializer)
        .intercept_pattern(server_pattern_prefix, proxy)
        .build()
    )
    return client_session, server_session


class TestClientServerSerialization:
    get_product = GetProduct()
    create_product = CreateProduct()
    update_product = UpdateProduct()
    get_product_details = GetProductDetails()
    sync_tasks = SyncTasks()
    process_chat = ProcessChat()

    @pytest.mark.asyncio
    async def test_simple_request_serializes_correctly(self):
        client_session, _ = build_client_server_pair(lambda server: server.intercept(
            self.get_product.handle(lambda input, ctx: _async(Result.ok({
                "id": "prod-1", "name": "Widget Alpha", "category": "electronics",
                "createdDate": "2020-03-15", "tags": ["premium", "certified"],
            })))
        ))
        client = (
            client_session.create_client("mobile-app")
            .with_locale("en-GB")
            .with_data_tenant("tenant1")
            .add_path_param("dataSource", {"type": "manual"})
            .add_path_param("productId", "prod-1")
        )
        result = await client.request(RequestOperationRequest("usage1", self.get_product, "prod-1"))
        assert result.is_success() is True
        assert result.value["id"] == "prod-1"
        assert result.value["name"] == "Widget Alpha"
        assert len(result.value["tags"]) == 2

    @pytest.mark.asyncio
    async def test_complex_nested_object(self):
        client_session, _ = build_client_server_pair(lambda server: server.intercept(
            self.get_product_details.handle(lambda input, ctx: _async(Result.ok({
                "productId": "prod-1",
                "components": [
                    {"id": "comp-1", "name": "Resistor Pack", "description": "Standard 10k ohm", "dateAdded": "2024-01-15", "active": True},
                    {"id": "comp-2", "name": "Capacitor Set", "description": None, "dateAdded": None, "active": False},
                ],
                "owner": {"userId": "user-1", "name": "John"},
                "metrics": {"weight": 14.5, "labels": ["fragile", "heavy"]},
            })))
        ))
        client = client_session.create_client("mobile-app").with_data_tenant("tenant1")
        result = await client.request(RequestOperationRequest("usage1", self.get_product_details, "prod-1"))
        assert result.is_success() is True
        assert len(result.value["components"]) == 2
        assert result.value["components"][0]["name"] == "Resistor Pack"
        assert result.value["components"][1]["description"] is None
        assert result.value["owner"]["userId"] == "user-1"

    @pytest.mark.asyncio
    async def test_null_fields_survive_serialization(self):
        client_session, _ = build_client_server_pair(lambda server: server.intercept(
            self.get_product.handle(lambda input, ctx: _async(Result.ok({
                "id": "prod-1", "name": "Widget", "category": "electronics",
                "createdDate": None, "image": None, "tags": None,
            })))
        ))
        client = client_session.create_client("web-app").with_data_tenant("t1")
        result = await client.request(RequestOperationRequest("u1", self.get_product, "prod-1"))
        assert result.is_success() is True
        assert result.value["createdDate"] is None
        assert result.value["image"] is None
        assert result.value["tags"] is None

    @pytest.mark.asyncio
    async def test_error_result_serializes_correctly(self):
        client_session, _ = build_client_server_pair(lambda server: server.intercept(
            self.get_product.handle(lambda input, ctx: _async(
                Result.not_found("TestApp.Products.ProductNotFound", "Product with id prod-999 not found", "Not found")
            ))
        ))
        client = client_session.create_client("mobile-app").with_data_tenant("t1")
        result = await client.request(RequestOperationRequest("u1", self.get_product, "prod-999"))
        assert result.is_success() is False
        assert result.status_code == 404
        assert result.error.code == "TestApp.Products.ProductNotFound"

    @pytest.mark.asyncio
    async def test_bad_request_error_serializes_with_details(self):
        async def handler(input, ctx):
            if not input.get("name"):
                return Result.bad_request(
                    "TestApp.Products.InvalidName",
                    "Product name cannot be empty",
                    "Please enter a name",
                )
            return Result.ok({"id": "new", "name": input["name"]})

        client_session, _ = build_client_server_pair(lambda server: server.intercept(
            self.create_product.handle(handler)
        ))
        client = client_session.create_client("mobile-app").with_data_tenant("t1")
        result = await client.request(RequestOperationRequest("u1", self.create_product, {"name": "", "categoryId": "electronics"}))
        assert result.is_success() is False
        assert result.status_code == 400
        assert result.error.code == "TestApp.Products.InvalidName"

    @pytest.mark.asyncio
    async def test_characters_propagation(self):
        captured = {}

        async def handler(input, ctx):
            captured["performer_id"] = ctx.call.characters.performer.id if hasattr(ctx.call.characters, 'performer') and ctx.call.characters.performer else (ctx.call.characters.get("performer", {}) or {}).get("id") if isinstance(ctx.call.characters, dict) else None
            return Result.ok({"id": "prod-1", "name": "Widget"})

        client_session, _ = build_client_server_pair(lambda server: server.intercept(
            self.get_product.handle(handler)
        ))
        client = (
            client_session.create_client("mobile-app")
            .with_data_tenant("t1")
            .with_characters({"performer": Identifier("user-123", "user")})
        )
        await client.request(RequestOperationRequest("u1", self.get_product, "prod-1"))
        assert captured["performer_id"] == "user-123"

    @pytest.mark.asyncio
    async def test_metadata_with_values_survives_serialization(self):
        from datetime import datetime

        async def handler(input, ctx):
            meta = Metavalues()
            meta.set_has_more_values(True)
            meta.set_total_value_count(42)
            meta.with_attribute("page", 1)
            meta.add(Metavalue.with_values(
                "prod-1", "tenant1",
                Identifier("creator1", "user"), datetime(2024, 1, 1),
                Identifier("updater1", "user"), datetime(2024, 6, 1),
            ))
            return Result.ok({"id": "prod-1", "name": "Widget"}, meta)

        client_session, _ = build_client_server_pair(lambda server: server.intercept(
            self.get_product.handle(handler)
        ))
        client = client_session.create_client("mobile-app").with_data_tenant("t1")
        result = await client.request(RequestOperationRequest("u1", self.get_product, "prod-1"))
        assert result.is_success() is True
        assert result.meta.has_more_values is True
        assert result.meta.total_value_count == 42
        assert result.meta.has_attribute("page") is True

    @pytest.mark.asyncio
    async def test_inbound_to_outbound_full_round_trip(self):
        cache = InMemoryContextCache()

        downstream_session = (
            Transport.session("downstream-service")
            .assign_serializer(json_serializer)
            .intercept(self.sync_tasks.handle(lambda input, ctx: _async(Result.ok({
                "syncedCount": len(input["taskChanges"]),
                "syncToken": "server-token-123",
            }))))
            .build()
        )

        server_builder = (
            Transport.session("main-server")
            .assign_serializer(json_serializer)
            .setup_outbound_context_cache(cache)
        )

        async def outbound_proxy(input, ctx):
            serialized = ctx.serialize_request(input)
            r = await downstream_session.accept_incoming_request(serialized)
            return ctx.deserialize_result(r.serialize())

        outbound_factory = (
            server_builder.outbound_session_builder("downstream-outbound")
            .intercept_pattern("TestApp.Tasks.", outbound_proxy)
            .build()
        )

        async def main_handler(input, ctx):
            outbound_client = await outbound_factory.for_incoming_request(ctx.call.transaction_id)
            with_tenant = outbound_client.with_data_tenant(ctx.call.data_tenant).with_characters(ctx.call.characters)
            sync_result = await with_tenant.request(RequestOperationRequest("sync-usage", self.sync_tasks, {
                "taskChanges": [{"id": "t1", "name": "Process item", "type": "daily", "isCompleted": False}],
                "completions": [],
            }))
            if not sync_result.is_success():
                return sync_result.convert()
            return Result.ok({
                "id": "prod-1", "name": "Widget",
                "tags": [f"synced:{sync_result.value['syncToken']}"],
            })

        server_builder.intercept(self.get_product.handle(main_handler))
        server_session = server_builder.build()

        async def client_proxy(input, ctx):
            serialized = ctx.serialize_request(input)
            r = await server_session.accept_incoming_request(serialized)
            return ctx.deserialize_result(r.serialize())

        client_session = (
            Transport.session("client-session")
            .assign_serializer(json_serializer)
            .intercept_pattern("TestApp.", client_proxy)
            .build()
        )
        client = (
            client_session.create_client("mobile-app")
            .with_data_tenant("acme-corp")
            .with_characters({"performer": Identifier("user-1", "user")})
        )
        result = await client.request(RequestOperationRequest("u1", self.get_product, "prod-1"))
        assert result.is_success() is True
        assert result.value["name"] == "Widget"
        assert "synced:server-token-123" in result.value["tags"]

    @pytest.mark.asyncio
    async def test_complex_input_dto_serializes(self):
        captured_input = [None]

        async def handler(input, ctx):
            captured_input[0] = input
            return Result.ok({
                "syncedCount": len(input["taskChanges"]),
                "syncToken": "tok-abc",
            })

        client_session, _ = build_client_server_pair(lambda server: server.intercept(
            self.sync_tasks.handle(handler)
        ))
        client = client_session.create_client("mobile-app").with_data_tenant("t1")
        result = await client.request(RequestOperationRequest("u1", self.sync_tasks, {
            "taskChanges": [
                {"id": "t1", "name": "Review report", "type": "daily", "isCompleted": False},
                {"id": "t2", "name": "Update inventory", "type": "health", "isCompleted": True},
            ],
            "completions": ["t3", "t4"],
        }))
        assert result.is_success() is True
        assert result.value["syncedCount"] == 2
        assert result.value["syncToken"] == "tok-abc"
        assert captured_input[0] is not None
        assert len(captured_input[0]["taskChanges"]) == 2

    @pytest.mark.asyncio
    async def test_multiple_sequential_requests_independent(self):
        request_count = [0]

        async def handler(input, ctx):
            request_count[0] += 1
            return Result.ok({"id": f"prod-{request_count[0]}", "name": f"Product {request_count[0]}"})

        client_session, _ = build_client_server_pair(lambda server: server.intercept(
            self.get_product.handle(handler)
        ))
        client = client_session.create_client("mobile-app").with_data_tenant("t1")
        r1 = await client.request(RequestOperationRequest("u1", self.get_product, "1"))
        r2 = await client.request(RequestOperationRequest("u1", self.get_product, "2"))
        r3 = await client.request(RequestOperationRequest("u1", self.get_product, "3"))
        assert r1.value["name"] == "Product 1"
        assert r2.value["name"] == "Product 2"
        assert r3.value["name"] == "Product 3"

    @pytest.mark.asyncio
    async def test_chat_instruction_complex_nested(self):
        async def handler(input, ctx):
            assert len(input["items"]) == 2
            assert input["items"][0]["role"] == "system"
            return Result.ok({
                "message": "I found some tasks to create",
                "operations": [{
                    "operationId": "TestApp.Tasks.CreateTask",
                    "operationType": "request",
                    "operationVerb": "CREATE",
                    "usageId": "PeerColab.Instructions",
                    "requestJson": {"id": "t1", "name": "Review report", "type": "daily"},
                }],
            })

        client_session, _ = build_client_server_pair(
            lambda server: server.intercept(self.process_chat.handle(handler)),
            "PeerColab.",
        )
        client = client_session.create_client("mobile-app").with_data_tenant("t1")
        result = await client.request(RequestOperationRequest("PeerColab.Instructions", self.process_chat, {
            "usageInstructions": "Operation id: TestApp.Tasks.CreateTask...",
            "currentStateSnapshot": "{ tasks: [] }",
            "items": [
                {"type": "message", "role": "system", "content": "You are a helpful assistant"},
                {"type": "message", "role": "user", "content": "Create a task to review the daily report"},
            ],
        }))
        assert result.is_success() is True
        assert result.value["message"] == "I found some tasks to create"
        assert len(result.value["operations"]) == 1

    def test_result_serialize_deserialize_success(self):
        original = Result.ok({"id": "prod-1", "name": "Widget", "tags": ["premium"]})
        original.assign_serializer(json_serializer)
        json_str = original.serialize()
        assert len(json_str) > 0
        deserialized = Result.deserialize_result(json_serializer, json_str)
        assert deserialized.is_success() is True
        assert deserialized.value["id"] == "prod-1"
        assert len(deserialized.value["tags"]) == 1

    def test_result_serialize_deserialize_error(self):
        original = Result.bad_request("TestApp.Products.InvalidName", "Name too long", "Please use a shorter name")
        original.assign_serializer(json_serializer)
        json_str = original.serialize()
        deserialized = Result.deserialize_result(json_serializer, json_str)
        assert deserialized.is_success() is False
        assert deserialized.status_code == 400
        assert deserialized.error.code == "TestApp.Products.InvalidName"

    def test_transport_request_round_trip_preserves_all_fields(self):
        original = TransportRequest(
            "TestApp.Products.CreateProduct", "CREATE", "request",
            "mobile-app", "TestApp.MobileApp.Client.Products",
            "tx-unique-123", "acme-corp", "nb-NO",
            Characters(
                performer=Identifier("user-42", "user"),
                subject=Identifier("prod-1", "product"),
            ),
            [Attribute("apiVersion", "v2"), Attribute("platform", "ios")],
            [Attribute("dataSource", "manual"), Attribute("productId", "prod-1")],
            {"name": "Gadget", "categoryId": "electronics"},
            None,
        ).assign_serializer(json_serializer)
        json_str = original.serialize()
        deserialized = TransportRequest.from_serialized(json_serializer, json_str)
        assert deserialized.operation_id == "TestApp.Products.CreateProduct"
        assert deserialized.operation_verb == "CREATE"
        assert deserialized.calling_client == "mobile-app"
        assert deserialized.transaction_id == "tx-unique-123"
        assert deserialized.data_tenant == "acme-corp"
        assert deserialized.locale == "nb-NO"
        assert deserialized.characters.performer.id == "user-42"
        assert deserialized.characters.subject.id == "prod-1"
        assert len(deserialized.attributes) == 2
        assert len(deserialized.path_params) == 2


async def _async(val):
    return val
