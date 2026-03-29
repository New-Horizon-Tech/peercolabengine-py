from __future__ import annotations
import pytest
from peercolab_engine import (
    ProcessChatInstruction, ProcessChatInstructionInput, ProcessChatInstructionOutput,
    ChatInstruction, PeerColabAI, RequestOperationRequest, RequestOperationHandler,
    Result, OutOfContextOperation, OutOfContextOperationPathParameter,
    Transport, DefaultTransportSerializer,
)

json_serializer = DefaultTransportSerializer()


def build_client_server_pair(configure_server, server_pattern_prefix="PeerColab."):
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


class TestProcessChatInstruction:
    def test_has_correct_operation_id(self):
        op = ProcessChatInstruction()
        assert op.id == "PeerColab.Instructions.ProcessChatInstruction"

    def test_has_correct_verb(self):
        op = ProcessChatInstruction()
        assert op.verb == "PROCESS"

    def test_is_request_type(self):
        op = ProcessChatInstruction()
        assert op.type == "request"

    def test_has_no_path_parameters(self):
        op = ProcessChatInstruction()
        assert op.path_parameters is None or op.path_parameters == []

    def test_creates_handler(self):
        op = ProcessChatInstruction()
        handler = op.handle(lambda input, ctx: Result.ok({
            "message": "test",
            "operations": []
        }))
        assert isinstance(handler, RequestOperationHandler)
        assert handler.operation is op


class TestPeerColabAI:
    def test_process_chat_instructions_creates_request(self):
        input_data = {
            "usageInstructions": "Available operations: ...",
            "currentStateSnapshot": "{}",
            "items": [
                {"type": "message", "role": "user", "content": "Hello"}
            ]
        }
        request = PeerColabAI.process_chat_instructions(input_data)
        assert isinstance(request, RequestOperationRequest)
        assert request.usage_id == "PeerColab.Instructions"
        assert request.operation.id == "PeerColab.Instructions.ProcessChatInstruction"
        assert request.operation.verb == "PROCESS"
        assert request.input is input_data

    def test_camel_case_alias_works(self):
        input_data = {
            "usageInstructions": "test",
            "currentStateSnapshot": "{}",
            "items": []
        }
        request = PeerColabAI.processChatInstructions(input_data)
        assert isinstance(request, RequestOperationRequest)

    def test_preserves_all_input_fields(self):
        items = [
            {"type": "message", "role": "system", "content": "You are a helpful assistant"},
            {"type": "message", "role": "user", "content": "Create a resource called User"},
        ]
        input_data = {
            "usageInstructions": "Operation: CreateResource\nVerb: CREATE",
            "currentStateSnapshot": '{"resources": []}',
            "items": items,
        }
        request = PeerColabAI.process_chat_instructions(input_data)
        assert request.input["usageInstructions"] == input_data["usageInstructions"]
        assert request.input["currentStateSnapshot"] == input_data["currentStateSnapshot"]
        assert len(request.input["items"]) == 2
        assert request.input["items"][0]["role"] == "system"
        assert request.input["items"][1]["content"] == "Create a resource called User"


class TestChatInstruction:
    def test_stores_all_properties(self):
        msg = ChatInstruction(type="message", role="user", content="Hello")
        assert msg.type == "message"
        assert msg.role == "user"
        assert msg.content == "Hello"

    def test_supports_all_standard_roles(self):
        for role in ["user", "assistant", "system", "developer"]:
            msg = ChatInstruction(type="message", role=role, content="test")
            assert msg.role == role
            assert msg.type == "message"


class TestProcessChatInstructionInput:
    def test_stores_all_properties(self):
        items = [ChatInstruction(type="message", role="user", content="Hello")]
        inp = ProcessChatInstructionInput(
            usage_instructions="Available operations: ...",
            current_state_snapshot="{}",
            items=items,
        )
        assert inp.usage_instructions == "Available operations: ..."
        assert inp.current_state_snapshot == "{}"
        assert len(inp.items) == 1

    def test_camel_case_aliases(self):
        inp = ProcessChatInstructionInput(
            usage_instructions="test",
            current_state_snapshot="{}",
            items=[],
        )
        assert inp.usageInstructions == "test"
        assert inp.currentStateSnapshot == "{}"


class TestProcessChatInstructionOutput:
    def test_message_is_optional(self):
        output = ProcessChatInstructionOutput(operations=[])
        assert output.message is None
        assert output.operations == []

    def test_defaults_operations_to_empty_list(self):
        output = ProcessChatInstructionOutput()
        assert output.message is None
        assert output.operations == []

    def test_stores_all_properties(self):
        ops = [OutOfContextOperation(
            usage_id="TestUsage",
            operation_id="TestApp.CreateResource",
            operation_verb="CREATE",
            operation_type="request",
            request_json={"name": "User"},
            path_parameters=[OutOfContextOperationPathParameter(name="SystemId", value="123")],
        )]
        output = ProcessChatInstructionOutput(
            message="Created the resource",
            operations=ops,
        )
        assert output.message == "Created the resource"
        assert len(output.operations) == 1
        assert output.operations[0].operation_id == "TestApp.CreateResource"
        assert len(output.operations[0].path_parameters) == 1
        assert output.operations[0].path_parameters[0].name == "SystemId"


class TestChatInstructionEndToEnd:
    """End-to-end tests that simulate real client-server communication
    with serialization/deserialization at the boundary."""

    process_chat = ProcessChatInstruction()

    @pytest.mark.asyncio
    async def test_client_sends_chat_server_returns_message_and_operations(self):
        async def handler(input, ctx):
            assert input["usageInstructions"] == "Available operations: CreateResource"
            assert input["currentStateSnapshot"] == '{"resources": []}'
            assert len(input["items"]) == 2
            assert input["items"][0]["role"] == "system"
            assert input["items"][1]["role"] == "user"
            assert input["items"][1]["content"] == "Create a resource called User"
            return Result.ok({
                "message": "I created the User resource with two properties.",
                "operations": [
                    {
                        "usageId": "PeerColab.BuilderWebApp.Client.SystemFunctionality",
                        "operationId": "Builder.Client.Models.Model.Resources.CreateResource",
                        "operationVerb": "CREATE",
                        "operationType": "request",
                        "requestJson": {"id": "r1", "name": "User"},
                    },
                    {
                        "usageId": "PeerColab.BuilderWebApp.Client.SystemFunctionality",
                        "operationId": "Builder.Client.Models.Model.Properties.CreateProperty",
                        "operationVerb": "CREATE",
                        "operationType": "request",
                        "requestJson": {
                            "id": "p1",
                            "owner": {"type": "resource", "id": "r1"},
                            "name": "Name",
                            "type": {"type": "native", "id": "string"},
                        },
                    },
                ],
            })

        client_session, _ = build_client_server_pair(
            lambda server: server.intercept(self.process_chat.handle(handler)),
        )
        client = client_session.create_client("builder-app").with_data_tenant("sub1")
        result = await client.request(PeerColabAI.process_chat_instructions({
            "usageInstructions": "Available operations: CreateResource",
            "currentStateSnapshot": '{"resources": []}',
            "items": [
                {"type": "message", "role": "system", "content": "You are a data modelling assistant"},
                {"type": "message", "role": "user", "content": "Create a resource called User"},
            ],
        }))

        assert result.is_success() is True
        assert result.value["message"] == "I created the User resource with two properties."
        assert len(result.value["operations"]) == 2
        assert result.value["operations"][0]["operationId"] == "Builder.Client.Models.Model.Resources.CreateResource"
        assert result.value["operations"][0]["requestJson"]["name"] == "User"
        assert result.value["operations"][1]["operationId"] == "Builder.Client.Models.Model.Properties.CreateProperty"
        assert result.value["operations"][1]["requestJson"]["name"] == "Name"

    @pytest.mark.asyncio
    async def test_client_sends_chat_server_returns_empty_operations(self):
        async def handler(input, ctx):
            return Result.ok({
                "message": "I don't understand your request. Could you clarify?",
                "operations": [],
            })

        client_session, _ = build_client_server_pair(
            lambda server: server.intercept(self.process_chat.handle(handler)),
        )
        client = client_session.create_client("builder-app").with_data_tenant("sub1")
        result = await client.request(PeerColabAI.process_chat_instructions({
            "usageInstructions": "",
            "currentStateSnapshot": "{}",
            "items": [
                {"type": "message", "role": "user", "content": "Hello"},
            ],
        }))

        assert result.is_success() is True
        assert result.value["message"] == "I don't understand your request. Could you clarify?"
        assert result.value["operations"] == []

    @pytest.mark.asyncio
    async def test_client_sends_chat_server_returns_operations_with_path_parameters(self):
        async def handler(input, ctx):
            return Result.ok({
                "message": "Created the resource.",
                "operations": [{
                    "usageId": "PeerColab.BuilderWebApp.Client.SystemFunctionality",
                    "operationId": "Builder.Client.Models.Model.Resources.CreateResource",
                    "operationVerb": "CREATE",
                    "operationType": "request",
                    "requestJson": {"name": "Order"},
                    "pathParameters": [
                        {"name": "SystemId", "value": "sys-1"},
                        {"name": "ModelId", "value": "mdl-1"},
                        {"name": "Version", "value": "1"},
                    ],
                }],
            })

        client_session, _ = build_client_server_pair(
            lambda server: server.intercept(self.process_chat.handle(handler)),
        )
        client = client_session.create_client("builder-app").with_data_tenant("sub1")
        result = await client.request(PeerColabAI.process_chat_instructions({
            "usageInstructions": "Required path parameters: SystemId, ModelId, Version",
            "currentStateSnapshot": "{}",
            "items": [
                {"type": "message", "role": "user", "content": "Create a resource called Order"},
            ],
        }))

        assert result.is_success() is True
        assert len(result.value["operations"]) == 1
        op = result.value["operations"][0]
        assert op["operationId"] == "Builder.Client.Models.Model.Resources.CreateResource"
        assert len(op["pathParameters"]) == 3
        assert op["pathParameters"][0]["name"] == "SystemId"
        assert op["pathParameters"][2]["value"] == "1"

    @pytest.mark.asyncio
    async def test_server_error_propagates_to_client(self):
        async def handler(input, ctx):
            return Result.internal_server_error(
                "PeerColab.AI.ProcessingFailed",
                "LLM API returned an error",
            )

        client_session, _ = build_client_server_pair(
            lambda server: server.intercept(self.process_chat.handle(handler)),
        )
        client = client_session.create_client("builder-app").with_data_tenant("sub1")
        result = await client.request(PeerColabAI.process_chat_instructions({
            "usageInstructions": "",
            "currentStateSnapshot": "{}",
            "items": [
                {"type": "message", "role": "user", "content": "Do something"},
            ],
        }))

        assert result.is_success() is False
        assert result.status_code == 500
        assert result.error.code == "PeerColab.AI.ProcessingFailed"

    @pytest.mark.asyncio
    async def test_multi_turn_conversation_preserves_history(self):
        call_count = 0

        async def handler(input, ctx):
            nonlocal call_count
            call_count += 1
            items = input["items"]
            if call_count == 1:
                assert len(items) == 1
                assert items[0]["role"] == "user"
                return Result.ok({
                    "message": "What properties should the User resource have?",
                    "operations": [],
                })
            else:
                assert len(items) == 3
                assert items[0]["role"] == "user"
                assert items[1]["role"] == "assistant"
                assert items[2]["role"] == "user"
                assert items[2]["content"] == "Name and Email, both strings"
                return Result.ok({
                    "message": "Created User with Name and Email.",
                    "operations": [
                        {
                            "usageId": "SystemFunctionality",
                            "operationId": "Resources.CreateResource",
                            "operationVerb": "CREATE",
                            "operationType": "request",
                            "requestJson": {"name": "User"},
                        },
                    ],
                })

        client_session, _ = build_client_server_pair(
            lambda server: server.intercept(self.process_chat.handle(handler)),
        )
        client = client_session.create_client("builder-app").with_data_tenant("sub1")

        # Turn 1
        result1 = await client.request(PeerColabAI.process_chat_instructions({
            "usageInstructions": "",
            "currentStateSnapshot": "{}",
            "items": [
                {"type": "message", "role": "user", "content": "Create a User resource"},
            ],
        }))
        assert result1.is_success() is True
        assert result1.value["operations"] == []

        # Turn 2 - includes conversation history
        result2 = await client.request(PeerColabAI.process_chat_instructions({
            "usageInstructions": "",
            "currentStateSnapshot": "{}",
            "items": [
                {"type": "message", "role": "user", "content": "Create a User resource"},
                {"type": "message", "role": "assistant", "content": "What properties should the User resource have?"},
                {"type": "message", "role": "user", "content": "Name and Email, both strings"},
            ],
        }))
        assert result2.is_success() is True
        assert len(result2.value["operations"]) == 1
        assert call_count == 2
