"""
"Commons Clause" License Condition v1.0

The Software is provided to you by the Licensor under the License, as defined below, subject to the following condition.

Without limiting other conditions in the License, the grant of rights under the License will not include, and the License
does not grant to you, the right to Sell the Software.

For purposes of the foregoing, "Sell" means practicing any or all of the rights granted to you under the License to provide
to third parties, for a fee or other consideration (including without limitation fees for hosting or consulting/ support
services related to the Software), a product or service whose value derives, entirely or substantially, from the
functionality of the Software. Any license notice or attribution required by the License must also include this Commons
Clause License Condition notice.

Software: PeerColab Engine
License: Apache 2.0
Licensor: New Horizon Invest AS

---------------------------------------------------------------------------------------------------------------------------

The operation verb is not to confuse with HTTP verbs. The operation verbs is to mark the operation
with what type of data processing it is doing
  GET: Reading information
  CREATE: Creating a new record
  ADD: Adding a record to another
  UPDATE: Updating / overwriting a full record
  PATCH: Partially updating a record
  REMOVE: Removing a record from another
  DELETE: Deleting a record
  START: Initiating something
  STOP: Ending / aborting something that was initiated
  PROCESS: Processing information
  SEARCH: Processing information
  NAVIGATETO: UI navigation
"""
from __future__ import annotations

import asyncio
import json
import uuid
import copy
import traceback
from datetime import datetime
from enum import IntEnum
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    TypeVar,
    Union,
)

try:
    from typing import Protocol, runtime_checkable
except ImportError:
    from typing_extensions import Protocol, runtime_checkable

# ---------------------------------------------------------------------------
# Type Variables
# ---------------------------------------------------------------------------
T = TypeVar("T")
R = TypeVar("R")
V = TypeVar("V")

# ---------------------------------------------------------------------------
# UUID helper
# ---------------------------------------------------------------------------
UUID = str


def generate_uuid() -> UUID:
    return str(uuid.uuid4())


generateUUID = generate_uuid

# ---------------------------------------------------------------------------
# OperationVerb - string literal type (Python uses plain strings)
# ---------------------------------------------------------------------------
OPERATION_VERBS = (
    "GET", "SEARCH", "CREATE", "ADD", "UPDATE", "PATCH",
    "REMOVE", "DELETE", "START", "STOP", "PROCESS", "NAVIGATETO",
)

OperationVerb = str

# ---------------------------------------------------------------------------
# Attribute
# ---------------------------------------------------------------------------


class Attribute:
    __slots__ = ("name", "value")

    def __init__(self, name: str, value: Any) -> None:
        self.name = name
        self.value = value


# ---------------------------------------------------------------------------
# OutOfContextOperationPathParameter / OutOfContextOperation
# ---------------------------------------------------------------------------


class OutOfContextOperationPathParameter:
    __slots__ = ("name", "value")

    def __init__(self, name: str, value: str) -> None:
        self.name = name
        self.value = value


class OutOfContextOperation:
    __slots__ = (
        "usage_id", "operation_id", "operation_verb",
        "operation_type", "request_json", "path_parameters",
    )

    def __init__(
        self,
        usage_id: str,
        operation_id: str,
        operation_verb: str,
        operation_type: str,
        request_json: Any,
        path_parameters: Optional[List[OutOfContextOperationPathParameter]] = None,
    ) -> None:
        self.usage_id = usage_id
        self.operation_id = operation_id
        self.operation_verb = operation_verb
        self.operation_type = operation_type
        self.request_json = request_json
        self.path_parameters = path_parameters

    # camelCase aliases
    @property
    def usageId(self) -> str:
        return self.usage_id

    @property
    def operationId(self) -> str:
        return self.operation_id

    @property
    def operationVerb(self) -> str:
        return self.operation_verb

    @property
    def operationType(self) -> str:
        return self.operation_type

    @property
    def requestJson(self) -> Any:
        return self.request_json

    @property
    def pathParameters(self) -> Optional[List[OutOfContextOperationPathParameter]]:
        return self.path_parameters


class OutOfContextEvent:
    __slots__ = (
        "usage_id", "event_id", "event_type", "request_json",
        "path_parameters", "correlation_id",
    )

    def __init__(
        self,
        usage_id: str,
        event_id: str,
        event_type: str,
        request_json: Any,
        path_parameters: Optional[List[OutOfContextOperationPathParameter]] = None,
        correlation_id: Optional[str] = None,
    ) -> None:
        self.usage_id = usage_id
        self.event_id = event_id
        self.event_type = event_type
        self.request_json = request_json
        self.path_parameters = path_parameters
        self.correlation_id = correlation_id

    # camelCase aliases
    @property
    def usageId(self) -> str:
        return self.usage_id

    @property
    def eventId(self) -> str:
        return self.event_id

    @property
    def eventType(self) -> str:
        return self.event_type

    @property
    def requestJson(self) -> Any:
        return self.request_json

    @property
    def pathParameters(self) -> Optional[List[OutOfContextOperationPathParameter]]:
        return self.path_parameters

    @property
    def correlationId(self) -> Optional[str]:
        return self.correlation_id


# ---------------------------------------------------------------------------
# Identity: Identifier, ICharacters (Protocol), Characters, CharacterMetaValues
# ---------------------------------------------------------------------------


class Identifier:
    __slots__ = ("id", "type")

    def __init__(self, id: str, type: Optional[str] = None) -> None:
        self.id = id
        self.type = type


@runtime_checkable
class ICharacters(Protocol):
    subject: Optional[Identifier]
    responsible: Optional[Identifier]
    performer: Optional[Identifier]


class Characters:
    def __init__(
        self,
        performer: Optional[Identifier] = None,
        responsible: Optional[Identifier] = None,
        subject: Optional[Identifier] = None,
    ) -> None:
        self.performer = performer
        self.responsible = responsible
        self.subject = subject


class CharacterMetaValues:
    def __init__(self) -> None:
        self.subject: Optional[Identifier] = None
        self.responsible: Optional[Identifier] = None
        self.performer: Optional[Identifier] = None
        self.timestamp: Optional[datetime] = None

    def has_subject(self) -> bool:
        return self.subject is not None

    hasSubject = has_subject

    def has_responsible(self) -> bool:
        return self.responsible is not None

    hasResponsible = has_responsible

    def has_performer(self) -> bool:
        return self.performer is not None

    hasPerformer = has_performer

    def has_timestamp(self) -> bool:
        return self.timestamp is not None

    hasTimestamp = has_timestamp

    @staticmethod
    def from_subject(subject: Identifier) -> CharacterMetaValues:
        return CharacterMetaValues().with_subject(subject)

    fromSubject = from_subject

    @staticmethod
    def from_responsible(responsible: Identifier) -> CharacterMetaValues:
        return CharacterMetaValues().with_responsible(responsible)

    fromResponsible = from_responsible

    @staticmethod
    def from_performer(performer: Identifier) -> CharacterMetaValues:
        return CharacterMetaValues().with_performer(performer)

    fromPerformer = from_performer

    @staticmethod
    def from_timestamp(timestamp: Optional[datetime] = None) -> CharacterMetaValues:
        return CharacterMetaValues().with_timestamp(timestamp)

    fromTimestamp = from_timestamp

    def with_subject(self, subject: Identifier) -> CharacterMetaValues:
        self.subject = subject
        return self

    withSubject = with_subject

    def with_responsible(self, responsible: Identifier) -> CharacterMetaValues:
        self.responsible = responsible
        return self

    withResponsible = with_responsible

    def with_performer(self, performer: Identifier) -> CharacterMetaValues:
        self.performer = performer
        return self

    withPerformer = with_performer

    def with_timestamp(self, timestamp: Optional[datetime] = None) -> CharacterMetaValues:
        self.timestamp = timestamp
        return self

    withTimestamp = with_timestamp


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


class TransportOperationCharacter:
    __slots__ = ("required", "valid_types")

    def __init__(self, required: bool, valid_types: Optional[List[str]] = None) -> None:
        self.required = required
        self.valid_types = valid_types

    @property
    def validTypes(self) -> Optional[List[str]]:
        return self.valid_types


class TransportOperationCharacterSetup:
    __slots__ = ("performer", "responsible", "subject")

    def __init__(
        self,
        performer: Optional[TransportOperationCharacter] = None,
        responsible: Optional[TransportOperationCharacter] = None,
        subject: Optional[TransportOperationCharacter] = None,
    ) -> None:
        self.performer = performer
        self.responsible = responsible
        self.subject = subject


class TransportOperationSettings:
    __slots__ = ("requires_tenant", "character_setup")

    def __init__(
        self,
        requires_tenant: bool = False,
        character_setup: Optional[TransportOperationCharacterSetup] = None,
    ) -> None:
        self.requires_tenant = requires_tenant
        self.character_setup = character_setup or TransportOperationCharacterSetup()

    @property
    def requiresTenant(self) -> bool:
        return self.requires_tenant

    @property
    def characterSetup(self) -> TransportOperationCharacterSetup:
        return self.character_setup


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


@runtime_checkable
class TransportSerializer(Protocol):
    def serialize(self, obj: Any) -> str:
        ...

    def deserialize(self, serialized: str) -> Any:
        ...


class _AttributeEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, Attribute):
            return {"name": obj.name, "value": obj.value}
        if isinstance(obj, Identifier):
            d: Dict[str, Any] = {"id": obj.id}
            if obj.type is not None:
                d["type"] = obj.type
            return d
        if isinstance(obj, CharacterMetaValues):
            d = {}
            if obj.performer is not None:
                d["performer"] = obj.performer
            if obj.responsible is not None:
                d["responsible"] = obj.responsible
            if obj.subject is not None:
                d["subject"] = obj.subject
            if obj.timestamp is not None:
                d["timestamp"] = obj.timestamp.isoformat()
            return d
        if isinstance(obj, Characters):
            d = {}
            if obj.performer is not None:
                d["performer"] = obj.performer
            if obj.responsible is not None:
                d["responsible"] = obj.responsible
            if obj.subject is not None:
                d["subject"] = obj.subject
            return d
        if isinstance(obj, Metavalue):
            d = {}
            if obj.value_id is not None:
                d["valueId"] = obj.value_id
            if obj.data_tenant is not None:
                d["dataTenant"] = obj.data_tenant
            if obj.initial_characters is not None:
                d["initialCharacters"] = obj.initial_characters
            if obj.current_characters is not None:
                d["currentCharacters"] = obj.current_characters
            if obj.attributes:
                d["attributes"] = obj.attributes
            return d
        if isinstance(obj, Metavalues):
            d = {
                "hasMoreValues": obj.has_more_values,
                "values": obj.values,
                "attributes": obj.attributes,
            }
            if obj.total_value_count is not None:
                d["totalValueCount"] = obj.total_value_count
            return d
        if isinstance(obj, TransportError):
            d = {"code": obj.code, "details": obj.details_dict()}
            if obj.related:
                d["related"] = obj.related
            if obj.parent is not None:
                d["parent"] = obj.parent
            return d
        if isinstance(obj, TransportErrorDetails):
            return obj.to_dict()
        if isinstance(obj, TransportRequest):
            return obj._to_dict()
        if isinstance(obj, TransportEvent):
            return obj._to_dict()
        if isinstance(obj, Result):
            return obj._to_dict()
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class DefaultTransportSerializer:
    def serialize(self, obj: Any) -> str:
        return json.dumps(obj, cls=_AttributeEncoder)

    def deserialize(self, serialized: str) -> Any:
        return json.loads(serialized)


class GlobalSerializer:
    _instance: Optional[TransportSerializer] = None

    @classmethod
    def get(cls) -> TransportSerializer:
        if cls._instance is None:
            cls._instance = DefaultTransportSerializer()
        return cls._instance

    @classmethod
    def set(cls, serializer: TransportSerializer) -> None:
        cls._instance = serializer


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


class LogLevel(IntEnum):
    FATAL = 0
    ERROR = 1
    WARNING = 2
    INFO = 3
    DEBUG = 4
    TRACE = 5


@runtime_checkable
class TransportSessionLogger(Protocol):
    log_level: LogLevel

    def write(self, message: LogMessage) -> None:
        ...


class LogMessage:
    __slots__ = ("source", "timestamp", "level", "message", "error")

    def __init__(
        self,
        source: str,
        timestamp: datetime,
        level: LogLevel,
        message: str,
        error: Optional[Exception] = None,
    ) -> None:
        self.source = source
        self.timestamp = timestamp
        self.level = level
        self.message = message
        self.error = error

    def is_within(self, level: LogLevel) -> bool:
        return self.level <= level

    isWithin = is_within

    def __str__(self) -> str:
        ts = self.timestamp.strftime("%H:%M:%S.") + f"{self.timestamp.microsecond // 1000:03d}"
        s = f"{ts} {self.level.name} - {self.message}\n"
        if self.error:
            s += f"{ts} {self.level.name} - {self.error}\n"
        return s

    def to_json(self) -> str:
        return str(self)

    toJSON = to_json


class DefaultLogger:
    def __init__(self) -> None:
        self.log_level = LogLevel.DEBUG

    def write(self, message: LogMessage) -> None:
        if message.is_within(self.log_level):
            print(str(message), end="")


class Logger:
    _source: str = ""
    _logger: Any = DefaultLogger()

    @classmethod
    def assign_logger(cls, logger: Any) -> None:
        cls._logger = logger

    assignLogger = assign_logger

    @classmethod
    def update_source(cls, source: str) -> None:
        cls._source = source

    updateSource = update_source

    @classmethod
    def write(cls, message: str, level: LogLevel, error: Optional[Exception] = None) -> None:
        if cls._logger is None:
            raise RuntimeError("Logger has not been assigned")
        cls._logger.write(LogMessage(cls._source, datetime.now(), level, message, error))

    @classmethod
    def trace(cls, message: str, error: Optional[Exception] = None) -> None:
        cls.write(message, LogLevel.TRACE, error)

    @classmethod
    def info(cls, message: str, error: Optional[Exception] = None) -> None:
        cls.write(message, LogLevel.INFO, error)

    @classmethod
    def debug(cls, message: str, error: Optional[Exception] = None) -> None:
        cls.write(message, LogLevel.DEBUG, error)

    @classmethod
    def warning(cls, message: str, error: Optional[Exception] = None) -> None:
        cls.write(message, LogLevel.WARNING, error)

    @classmethod
    def error(cls, message: str, error: Optional[Exception] = None) -> None:
        cls.write(message, LogLevel.ERROR, error)

    @classmethod
    def fatal(cls, message: str, error: Optional[Exception] = None) -> None:
        cls.write(message, LogLevel.FATAL, error)


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------


@runtime_checkable
class ContextCache(Protocol):
    async def put(self, transaction_id: UUID, ctx: CallInformation) -> bool:
        ...

    async def get(self, transaction_id: UUID) -> Optional[CallInformation]:
        ...


class InMemoryContextCache:
    def __init__(self, max_lifetime_ms: int = 3_000_000) -> None:
        self._max_lifetime_ms = max_lifetime_ms
        self._cache: Dict[UUID, Dict[str, Any]] = {}

    async def put(self, transaction_id: UUID, ctx: CallInformation) -> bool:
        import time
        expires_at = time.time() * 1000 + self._max_lifetime_ms
        self._cache[transaction_id] = {"ctx": ctx, "expires_at": expires_at}
        return True

    async def get(self, transaction_id: UUID) -> Optional[CallInformation]:
        import time
        entry = self._cache.get(transaction_id)
        if entry is None:
            return None
        if time.time() * 1000 > entry["expires_at"]:
            del self._cache[transaction_id]
            return None
        return entry["ctx"]


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class TransportErrorDetails:
    __slots__ = (
        "technical_error", "user_error", "session_identifier",
        "calling_client", "calling_usage", "called_operation", "transaction_id",
    )

    def __init__(
        self,
        technical_error: Optional[str] = None,
        user_error: Optional[str] = None,
        session_identifier: Optional[str] = None,
        calling_client: Optional[str] = None,
        calling_usage: Optional[str] = None,
        called_operation: Optional[str] = None,
        transaction_id: Optional[str] = None,
    ) -> None:
        self.technical_error = technical_error
        self.user_error = user_error
        self.session_identifier = session_identifier
        self.calling_client = calling_client
        self.calling_usage = calling_usage
        self.called_operation = called_operation
        self.transaction_id = transaction_id

    # camelCase aliases
    @property
    def technicalError(self) -> Optional[str]:
        return self.technical_error

    @technicalError.setter
    def technicalError(self, v: Optional[str]) -> None:
        self.technical_error = v

    @property
    def userError(self) -> Optional[str]:
        return self.user_error

    @userError.setter
    def userError(self, v: Optional[str]) -> None:
        self.user_error = v

    @property
    def sessionIdentifier(self) -> Optional[str]:
        return self.session_identifier

    @sessionIdentifier.setter
    def sessionIdentifier(self, v: Optional[str]) -> None:
        self.session_identifier = v

    @property
    def callingClient(self) -> Optional[str]:
        return self.calling_client

    @callingClient.setter
    def callingClient(self, v: Optional[str]) -> None:
        self.calling_client = v

    @property
    def callingUsage(self) -> Optional[str]:
        return self.calling_usage

    @callingUsage.setter
    def callingUsage(self, v: Optional[str]) -> None:
        self.calling_usage = v

    @property
    def calledOperation(self) -> Optional[str]:
        return self.called_operation

    @calledOperation.setter
    def calledOperation(self, v: Optional[str]) -> None:
        self.called_operation = v

    @property
    def transactionId(self) -> Optional[str]:
        return self.transaction_id

    @transactionId.setter
    def transactionId(self, v: Optional[str]) -> None:
        self.transaction_id = v

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {}
        if self.technical_error is not None:
            d["technicalError"] = self.technical_error
        if self.user_error is not None:
            d["userError"] = self.user_error
        if self.session_identifier is not None:
            d["sessionIdentifier"] = self.session_identifier
        if self.calling_client is not None:
            d["callingClient"] = self.calling_client
        if self.calling_usage is not None:
            d["callingUsage"] = self.calling_usage
        if self.called_operation is not None:
            d["calledOperation"] = self.called_operation
        if self.transaction_id is not None:
            d["transactionId"] = self.transaction_id
        return d


class TransportError:
    def __init__(
        self,
        code: str,
        details: Union[TransportErrorDetails, str, Dict[str, Any], None] = None,
        related: Optional[List[TransportError]] = None,
        parent: Optional[TransportError] = None,
    ) -> None:
        self.code = code
        if isinstance(details, str):
            self.details = TransportErrorDetails(technical_error=details)
        elif isinstance(details, dict):
            self.details = TransportErrorDetails(
                technical_error=details.get("technicalError") or details.get("technical_error"),
                user_error=details.get("userError") or details.get("user_error"),
                session_identifier=details.get("sessionIdentifier") or details.get("session_identifier"),
                calling_client=details.get("callingClient") or details.get("calling_client"),
                calling_usage=details.get("callingUsage") or details.get("calling_usage"),
                called_operation=details.get("calledOperation") or details.get("called_operation"),
                transaction_id=details.get("transactionId") or details.get("transaction_id"),
            )
        elif isinstance(details, TransportErrorDetails):
            self.details = details
        elif details is None:
            self.details = TransportErrorDetails(technical_error="Unknown error")
        else:
            self.details = TransportErrorDetails(technical_error="Unknown error")
        self.related: List[TransportError] = related if related is not None else []
        self.parent: Optional[TransportError] = parent

    @staticmethod
    def basic(
        code: str,
        technical_error: str,
        user_error: Optional[str] = None,
        related_errors: Optional[List[TransportError]] = None,
    ) -> TransportError:
        return TransportError(
            code,
            TransportErrorDetails(
                technical_error=technical_error,
                user_error=user_error,
            ),
            related_errors,
            None,
        )

    @staticmethod
    def from_parent(
        parent_error: TransportError,
        code: str,
        technical_error: str,
        user_error: Optional[str] = None,
        related_errors: Optional[List[TransportError]] = None,
    ) -> TransportError:
        return TransportError(
            code,
            TransportErrorDetails(
                technical_error=technical_error,
                user_error=user_error,
            ),
            related_errors,
            parent_error,
        )

    fromParent = from_parent

    def details_dict(self) -> Dict[str, Any]:
        return self.details.to_dict() if self.details else {}

    def to_short_string(self) -> str:
        return TransportError._get_short_string(self, "")

    toShortString = to_short_string

    def __str__(self) -> str:
        return TransportError._get_string(self, "")

    def to_string(self) -> str:
        return TransportError._get_string(self, "")

    toString = to_string

    def to_long_string(self) -> str:
        return TransportError._get_long_string(self, "")

    toLongString = to_long_string

    @staticmethod
    def _get_short_string(error: TransportError, initial_tabs: str) -> str:
        tech = error.details.technical_error if error.details else None
        if not tech or len(tech.strip()) == 0:
            return f"{initial_tabs}{error.code}"
        return f"{initial_tabs}{error.code} - {tech}"

    @staticmethod
    def _get_string(error: TransportError, initial_tabs: str) -> str:
        sb = TransportError._get_short_string(error, initial_tabs)
        if not error.related or len(error.related) == 0:
            return sb
        sb += f"\n{initial_tabs}    Related errors:"
        for r in error.related:
            sb += f"\n{TransportError._get_short_string(r, initial_tabs + '        ')}"
        return sb

    @staticmethod
    def _get_long_string(error: TransportError, initial_tabs: str) -> str:
        sb = ""
        if error.details:
            if error.details.transaction_id:
                sb += f"\n{initial_tabs}TransactionId: {error.details.transaction_id}"
            if error.details.session_identifier:
                sb += f"\n{initial_tabs}Session: {error.details.session_identifier}"
            if error.details.calling_client:
                sb += f"\n{initial_tabs}Client: {error.details.calling_client}"
            if error.details.calling_usage:
                sb += f"\n{initial_tabs}Usage: {error.details.calling_usage}"
            if error.details.called_operation:
                sb += f"\n{initial_tabs}Operation: {error.details.called_operation}"
        sb += f"\n{initial_tabs}{TransportError._get_string(error, initial_tabs)}"
        if not error.parent:
            return sb
        sb += f"\n{initial_tabs}Parent error:"
        parent_str = TransportError._get_long_string(error.parent, initial_tabs)
        lines = parent_str.split("\n")
        indented = "\n".join(
            (initial_tabs + "   " + line if len(line) > 0 else line) for line in lines
        )
        # Remove trailing newlines
        import re
        indented = re.sub(r"[\n\r]+$", "", indented)
        # Remove leading empty lines
        indented = re.sub(r"^\s*\n", "", indented)
        sb += f"\n{indented}"
        # Trim leading empty lines
        sb = re.sub(r"^\s*\n", "", sb)
        return sb


# ---------------------------------------------------------------------------
# Metadata: Metavalue, Metavalues
# ---------------------------------------------------------------------------


class Metavalue:
    def __init__(self) -> None:
        self.value_id: Optional[str] = None
        self.data_tenant: Optional[str] = None
        self.initial_characters: Optional[CharacterMetaValues] = None
        self.current_characters: Optional[CharacterMetaValues] = None
        self.attributes: List[Attribute] = []

    # camelCase aliases
    @property
    def valueId(self) -> Optional[str]:
        return self.value_id

    @valueId.setter
    def valueId(self, v: Optional[str]) -> None:
        self.value_id = v

    @property
    def dataTenant(self) -> Optional[str]:
        return self.data_tenant

    @dataTenant.setter
    def dataTenant(self, v: Optional[str]) -> None:
        self.data_tenant = v

    @property
    def initialCharacters(self) -> Optional[CharacterMetaValues]:
        return self.initial_characters

    @initialCharacters.setter
    def initialCharacters(self, v: Optional[CharacterMetaValues]) -> None:
        self.initial_characters = v

    @property
    def currentCharacters(self) -> Optional[CharacterMetaValues]:
        return self.current_characters

    @currentCharacters.setter
    def currentCharacters(self, v: Optional[CharacterMetaValues]) -> None:
        self.current_characters = v

    def knows_initial_characters(self) -> bool:
        return self.initial_characters is None

    knowsInitialCharacters = knows_initial_characters

    def knows_current_characters(self) -> bool:
        return self.current_characters is None

    knowsCurrentCharacters = knows_current_characters

    def with_initial_characters(self, characters: CharacterMetaValues) -> Metavalue:
        self.initial_characters = characters
        return self

    withInitialCharacters = with_initial_characters

    def with_current_characters(self, characters: CharacterMetaValues) -> Metavalue:
        self.current_characters = characters
        return self

    withCurrentCharacters = with_current_characters

    def with_attribute(self, name: str, value: Any) -> Metavalue:
        for attr in self.attributes:
            if attr.name == name:
                attr.value = value
                return self
        self.attributes.append(Attribute(name, value))
        return self

    withAttribute = with_attribute

    def has_attribute(self, name: str) -> bool:
        return any(a.name == name for a in self.attributes)

    hasAttribute = has_attribute

    def get_attribute(self, name: str) -> Any:
        for attr in self.attributes:
            if attr.name == name:
                return attr.value
        return None

    getAttribute = get_attribute

    @staticmethod
    def with_values(
        value_id: str,
        data_tenant: Optional[str] = None,
        initial_performer: Optional[Identifier] = None,
        created_at: Optional[datetime] = None,
        current_performer: Optional[Identifier] = None,
        updated_at: Optional[datetime] = None,
    ) -> Metavalue:
        ret = Metavalue()
        if initial_performer is not None:
            ret.with_initial_characters(
                CharacterMetaValues.from_performer(initial_performer).with_timestamp(created_at)
            )
        if current_performer is not None:
            ret.with_current_characters(
                CharacterMetaValues.from_performer(current_performer).with_timestamp(updated_at)
            )
        ret.value_id = value_id
        ret.data_tenant = data_tenant
        return ret

    # Alias: Metavalue.with() in TS maps to Metavalue.with_values() in Python (with is reserved)
    # For compat also expose as class method named 'create'


class Metavalues:
    def __init__(self) -> None:
        self.has_more_values: bool = False
        self.values: List[Metavalue] = []
        self.total_value_count: Optional[int] = None
        self.attributes: List[Attribute] = []

    # camelCase aliases
    @property
    def hasMoreValues(self) -> bool:
        return self.has_more_values

    @hasMoreValues.setter
    def hasMoreValues(self, v: bool) -> None:
        self.has_more_values = v

    @property
    def totalValueCount(self) -> Optional[int]:
        return self.total_value_count

    @totalValueCount.setter
    def totalValueCount(self, v: Optional[int]) -> None:
        self.total_value_count = v

    def has_meta_value(self, value_id: str) -> bool:
        return any(i.value_id == value_id for i in self.values)

    hasMetaValue = has_meta_value

    def get_meta_value(self, value_id: str) -> Optional[Metavalue]:
        for i in self.values:
            if i.value_id == value_id:
                return i
        return None

    getMetaValue = get_meta_value

    def set_has_more_values(self, more_values: Optional[bool] = None) -> Metavalues:
        self.has_more_values = more_values if more_values is not None else True
        return self

    setHasMoreValues = set_has_more_values

    def set_total_value_count(self, count: Optional[int] = None) -> Metavalues:
        self.total_value_count = count
        return self

    setTotalValueCount = set_total_value_count

    def add(self, value: Union[Metavalue, List[Metavalue]]) -> Metavalues:
        if isinstance(value, list):
            self.values.extend(value)
        else:
            self.values.append(value)
        return self

    def with_attribute(self, name: str, value: Any) -> Metavalues:
        for attr in self.attributes:
            if attr.name == name:
                attr.value = value
                return self
        self.attributes.append(Attribute(name, value))
        return self

    withAttribute = with_attribute

    def has_attribute(self, name: str) -> bool:
        return any(a.name == name for a in self.attributes)

    hasAttribute = has_attribute

    def get_attribute(self, name: str) -> Any:
        for item in self.attributes:
            if item.name == name:
                return item.value
        return None

    getAttribute = get_attribute


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------


class Result(Generic[T]):
    @staticmethod
    def deserialize_result(serializer: Any, data: str) -> Result:
        plain = serializer.deserialize(data)
        result = Result._from_plain(plain)
        return result

    deserializeResult = deserialize_result

    @staticmethod
    def _from_plain(plain: Dict[str, Any]) -> Result:
        value = plain.get("value")
        status_code = plain.get("statusCode", 200)
        success = plain.get("success", True)
        meta_data = plain.get("meta")
        error_data = plain.get("error")

        result = Result.__new__(Result)
        result.value = value
        result.status_code = status_code if status_code else (500 if error_data else 200)
        result.success = success if success is not None else Result._is_status_code_success(result.status_code)
        result.meta = Metavalues()
        result.error = None
        result._serializer = None

        if meta_data:
            if meta_data.get("hasMoreValues") is not None:
                result.meta.set_has_more_values(meta_data["hasMoreValues"])
            if meta_data.get("totalValueCount") is not None:
                result.meta.set_total_value_count(meta_data["totalValueCount"])
            if meta_data.get("attributes"):
                for attr in meta_data["attributes"]:
                    result.meta.with_attribute(attr["name"], attr["value"])
            for v in meta_data.get("values", []):
                mv = Metavalue()
                mv.data_tenant = v.get("dataTenant")
                mv.value_id = v.get("valueId")
                ic = v.get("initialCharacters")
                if ic:
                    c = CharacterMetaValues()
                    if ic.get("performer"):
                        c.with_performer(Identifier(ic["performer"]["id"], ic["performer"].get("type")))
                    if ic.get("responsible"):
                        c.with_responsible(Identifier(ic["responsible"]["id"], ic["responsible"].get("type")))
                    if ic.get("subject"):
                        c.with_subject(Identifier(ic["subject"]["id"], ic["subject"].get("type")))
                    mv.with_initial_characters(c)
                cc = v.get("currentCharacters")
                if cc:
                    c = CharacterMetaValues()
                    if cc.get("performer"):
                        c.with_performer(Identifier(cc["performer"]["id"], cc["performer"].get("type")))
                    if cc.get("responsible"):
                        c.with_responsible(Identifier(cc["responsible"]["id"], cc["responsible"].get("type")))
                    if cc.get("subject"):
                        c.with_subject(Identifier(cc["subject"]["id"], cc["subject"].get("type")))
                    mv.with_current_characters(c)
                if v.get("attributes"):
                    for attr in v["attributes"]:
                        mv.with_attribute(attr["name"], attr["value"])
                result.add_meta_value(mv)

        if error_data:
            result.error = Result._convert_error(error_data)

        return result

    @staticmethod
    def _convert_error(plain: Any) -> TransportError:
        if isinstance(plain, TransportError):
            return plain
        if isinstance(plain, dict):
            details = plain.get("details", {})
            if isinstance(details, str):
                details_obj = TransportErrorDetails(technical_error=details)
            elif isinstance(details, dict):
                details_obj = TransportErrorDetails(
                    technical_error=details.get("technicalError"),
                    user_error=details.get("userError"),
                    session_identifier=details.get("sessionIdentifier"),
                    called_operation=details.get("calledOperation"),
                    calling_client=details.get("callingClient"),
                    calling_usage=details.get("callingUsage"),
                    transaction_id=details.get("transactionId"),
                )
            else:
                details_obj = TransportErrorDetails()
            related = [Result._convert_error(e) for e in plain.get("related", [])]
            parent = Result._convert_error(plain["parent"]) if plain.get("parent") else None
            return TransportError(plain.get("code", ""), details_obj, related, parent)
        return TransportError(str(plain))

    def __init__(
        self,
        value: Any = None,
        status_code: Optional[int] = None,
        success: Optional[bool] = None,
        meta: Optional[Metavalues] = None,
        error: Optional[TransportError] = None,
        # camelCase kwargs for deserialization compat
        statusCode: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        self.value: Any = value
        sc = status_code if status_code is not None else statusCode
        if not sc:
            sc = 500 if error else 200
        self.status_code: int = sc

        if success is not None:
            self.success: bool = success
        else:
            self.success = self._is_status_code_success(self.status_code)

        self.meta: Metavalues = meta if meta is not None else Metavalues()

        if error is not None:
            self.error: Optional[TransportError] = error
        elif not self._is_status_code_success(self.status_code):
            self.error = TransportError(str(self.status_code), "Unknown error")
        else:
            self.error = None

        self._serializer: Optional[Any] = None

    # camelCase property aliases
    @property
    def statusCode(self) -> int:
        return self.status_code

    @statusCode.setter
    def statusCode(self, v: int) -> None:
        self.status_code = v

    @staticmethod
    def _is_status_code_success(status_code: int) -> bool:
        return 200 <= status_code <= 308

    def is_success(self) -> bool:
        return self.success

    isSuccess = is_success

    def has_error(self) -> bool:
        return self.error is not None

    hasError = has_error

    def assign_serializer(self, serializer: Any) -> Result:
        self._serializer = serializer
        return self

    assignSerializer = assign_serializer

    def serialize(self) -> str:
        if not self._serializer:
            raise RuntimeError("No serializer assigned to Result")
        return self._serializer.serialize(self)

    def deserialize(self, serialized: str) -> Result:
        if not self._serializer:
            raise RuntimeError("No serializer assigned to Result")
        return Result.deserialize_result(self._serializer, serialized)

    def _to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "value": self.value,
            "statusCode": self.status_code,
            "success": self.success,
            "meta": self.meta,
        }
        if self.error is not None:
            d["error"] = self.error
        return d

    @staticmethod
    def ok(value: Any = None, meta: Optional[Metavalues] = None) -> Result:
        return Result(
            value=value,
            success=True,
            status_code=200,
            meta=meta if meta is not None else Metavalues(),
        )

    @staticmethod
    def ok_status(code: int, meta: Optional[Metavalues] = None) -> Result:
        return Result(
            value=None,
            success=True,
            status_code=code,
            meta=meta if meta is not None else Metavalues(),
        )

    okStatus = ok_status

    @staticmethod
    def not_found(
        error_data: Union[str, TransportError],
        technical_error: Optional[str] = None,
        user_error: Optional[str] = None,
    ) -> Result:
        return Result.failed(404, error_data, technical_error, user_error)

    notFound = not_found

    @staticmethod
    def bad_request(
        error_data: Union[str, TransportError],
        technical_error: Optional[str] = None,
        user_error: Optional[str] = None,
    ) -> Result:
        return Result.failed(400, error_data, technical_error, user_error)

    badRequest = bad_request

    @staticmethod
    def internal_server_error(
        error_data: Union[str, TransportError],
        technical_error: Optional[str] = None,
        user_error: Optional[str] = None,
    ) -> Result:
        return Result.failed(500, error_data, technical_error, user_error)

    internalServerError = internal_server_error

    @staticmethod
    def failed(
        status_code: int,
        error_data: Union[str, TransportError],
        technical_error: Optional[str] = None,
        user_error: Optional[str] = None,
    ) -> Result:
        if isinstance(error_data, str):
            err = TransportError(
                error_data,
                TransportErrorDetails(
                    technical_error=technical_error or "",
                    session_identifier="",
                    user_error=user_error or "",
                ),
                [],
                None,
            )
        else:
            err = error_data
        return Result(
            value=None,
            status_code=status_code,
            meta=Metavalues(),
            success=False,
            error=err,
        )

    def as_generic(self) -> Result:
        return self.convert(None)

    asGeneric = as_generic

    def set_meta(self, meta: Metavalues) -> Result:
        self.meta = meta
        return self

    setMeta = set_meta

    def with_meta(self, handler: Callable[[Metavalues], None]) -> Result:
        if not isinstance(self.meta, Metavalues):
            self.meta = Metavalues()
        handler(self.meta)
        return self

    withMeta = with_meta

    def add_meta_value(self, value: Metavalue) -> Result:
        if not isinstance(self.meta, Metavalues):
            self.meta = Metavalues()
        self.meta.add(value)
        return self

    AddMetaValue = add_meta_value
    addMetaValue = add_meta_value

    def add_meta_values(self, values: List[Metavalue]) -> Result:
        if not isinstance(self.meta, Metavalues):
            self.meta = Metavalues()
        self.meta.add(values)
        return self

    AddMetaValues = add_meta_values
    addMetaValues = add_meta_values

    def convert_to_empty(self) -> Result:
        if self.error:
            return Result.failed(self.status_code, self.error).set_meta(self.meta)
        return Result.ok().set_meta(self.meta)

    convertToEmpty = convert_to_empty

    def convert(self, result_type: Any = None) -> Result:
        if self.value is None:
            return self
        if self._serializer:
            try:
                serialized = self._serializer.serialize(self)
                deserialized = self._serializer.deserialize(serialized)
                return Result._from_plain(deserialized).set_meta(self.meta if self.meta else Metavalues())
            except Exception:
                return Result.internal_server_error(
                    "TransportSession.Serialization.DeserializeError",
                    "Could not deserialize response",
                )
        return Result.internal_server_error(
            "TransportSession.Serialization.DeserializeError",
            "Could not convert Result",
        )

    def maybe(self, on_success: Callable[[Any, Metavalues], Result]) -> Result:
        try:
            if not self.success:
                return self.convert()
            return on_success(self.value, self.meta if self.meta else Metavalues())
        except Exception as e:
            return self._maybe_error(e)

    def maybe_ok(self, on_success: Callable[[Any, Metavalues], Any]) -> Result:
        try:
            if not self.success:
                return self.convert()
            on_success(self.value, self.meta if self.meta else Metavalues())
            return Result.ok(self.value).set_meta(self.meta if self.meta else Metavalues())
        except Exception as e:
            return self._maybe_error(e)

    maybeOk = maybe_ok

    def maybe_pass_through(self, on_success: Callable[[Any, Metavalues], Result]) -> Result:
        try:
            if not self.success:
                return self
            result = on_success(self.value, self.meta if self.meta else Metavalues())
            if not result.success:
                return result.convert()
            return self
        except Exception as e:
            return self._maybe_error(e)

    maybePassThrough = maybe_pass_through

    def maybe_pass_through_ok(self, on_success: Callable[[Any, Metavalues], Any]) -> Result:
        try:
            if not self.success:
                return self
            on_success(self.value, self.meta if self.meta else Metavalues())
            return Result.ok(self.value).set_meta(self.meta if self.meta else Metavalues())
        except Exception as e:
            return self._maybe_error(e)

    maybePassThroughOk = maybe_pass_through_ok

    def _maybe_error(self, e: Optional[Exception]) -> Result:
        if e:
            Logger.error("MaybeException: ", e)
            tb = traceback.format_exc()
            return Result.failed(
                500,
                "TransportSession.MaybeException",
                f"{e}: {type(e).__name__}" + (f"\n{tb}" if tb else ""),
            )
        Logger.error("MaybeException: Unknown error")
        return Result.failed(500, "TransportSession.MaybeException", "Unknown error")


# ---------------------------------------------------------------------------
# ResultPassthroughAsync
# ---------------------------------------------------------------------------


class ResultPassthroughAsync(Generic[T]):
    def __init__(self, action: Callable[[], Awaitable[Result]]) -> None:
        self._initial_action = action
        self._actions: List[Callable[[], Awaitable[Result]]] = []

    @staticmethod
    def start_with(action: Callable[[], Awaitable[Result]]) -> ResultPassthroughAsync:
        return ResultPassthroughAsync(action)

    startWith = start_with

    def then(self, action: Callable[[], Awaitable[Result]]) -> ResultPassthroughAsync:
        self._actions.append(action)
        return self

    async def run(self) -> Result:
        initial_result: Result = Result.failed(500, "TransportSession.MaybeException", "Unknown error")
        try:
            initial_result = await self._initial_action()
        except Exception as e:
            return self._maybe_error(e)

        if not initial_result.success:
            return initial_result

        for action in self._actions:
            try:
                result = await action()
                if not result.success:
                    return result
            except Exception as e:
                return self._maybe_error(e)

        return initial_result

    def _maybe_error(self, e: Optional[Exception]) -> Result:
        if e:
            Logger.error("MaybeException: ", e)
            tb = traceback.format_exc()
            return Result.failed(
                500,
                "TransportSession.MaybeException",
                f"{e}: {type(e).__name__}" + (f"\n{tb}" if tb else ""),
            )
        Logger.error("MaybeException: Unknown error")
        return Result.failed(500, "TransportSession.MaybeException", "Unknown error")


# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------


class TransportOperation(Generic[T, R]):
    def __init__(
        self,
        type: str,
        id: str,
        verb: str,
        path_parameters: Optional[List[str]] = None,
        settings: Optional[TransportOperationSettings] = None,
    ) -> None:
        self.type = type
        self.id = id
        self.verb = verb
        self.path_parameters = path_parameters
        self.settings = settings

    @property
    def pathParameters(self) -> Optional[List[str]]:
        return self.path_parameters


class OperationHandler(Generic[T, R]):
    def __init__(self, operation: TransportOperation) -> None:
        self.operation = operation


class RequestOperationHandler(OperationHandler[T, R]):
    def __init__(
        self,
        operation: RequestOperation,
        handler: Callable[[Any, TransportContext], Awaitable[Result]],
    ) -> None:
        super().__init__(operation)
        self.handler = handler


class MessageOperationHandler(OperationHandler[T, R]):
    def __init__(
        self,
        operation: MessageOperation,
        handler: Callable[[Any, TransportContext], Awaitable[Result]],
    ) -> None:
        super().__init__(operation)
        self.handler = handler


class EventOperationHandler(OperationHandler[T, R]):
    def __init__(
        self,
        operation: "DispatchOperation",
        handler: Callable[[Any, TransportContext], Awaitable[Result]],
    ) -> None:
        super().__init__(operation)
        self.handler = handler


class OperationRequest(Generic[T, R]):
    def __init__(
        self,
        usage_id: str,
        operation: TransportOperation,
        input: Any,
    ) -> None:
        self.usage_id = usage_id
        self.operation = operation
        self.input = input

    @property
    def usageId(self) -> str:
        return self.usage_id

    def as_operation_information(self, calling_client: str) -> OperationInformation:
        return OperationInformation(
            self.operation.id,
            self.operation.verb,
            self.operation.type,
            calling_client,
            self.usage_id,
        )

    asOperationInformation = as_operation_information


class RequestOperationRequest(OperationRequest[T, R]):
    pass


class MessageOperationRequest(OperationRequest[T, R]):
    pass


class EventDispatchRequest(OperationRequest[T, R]):
    pass


class RequestOperation(TransportOperation[T, R]):
    def __init__(
        self,
        id: str,
        verb: str,
        path_parameters: Optional[List[str]] = None,
        settings: Optional[TransportOperationSettings] = None,
    ) -> None:
        super().__init__("request", id, verb, path_parameters, settings)

    def handle(
        self, interceptor: Callable[[Any, TransportContext], Awaitable[Result]]
    ) -> RequestOperationHandler:
        return RequestOperationHandler(self, interceptor)


class MessageOperation(TransportOperation[T, R]):
    def __init__(
        self,
        id: str,
        verb: str,
        path_parameters: Optional[List[str]] = None,
        settings: Optional[TransportOperationSettings] = None,
    ) -> None:
        super().__init__("message", id, verb, path_parameters, settings)

    def handle(
        self, interceptor: Callable[[Any, TransportContext], Awaitable[Result]]
    ) -> MessageOperationHandler:
        return MessageOperationHandler(self, interceptor)


class DispatchOperation(TransportOperation[T, R]):
    def __init__(
        self,
        id: str,
        event_type: str,
        path_parameters: Optional[List[str]] = None,
        settings: Optional[TransportOperationSettings] = None,
    ) -> None:
        super().__init__("event", id, event_type, path_parameters, settings)

    def handle(
        self, interceptor: Callable[[Any, TransportContext], Awaitable[Result]]
    ) -> EventOperationHandler:
        return EventOperationHandler(self, interceptor)


# ---------------------------------------------------------------------------
# Transport Core: OperationInformation, CallInformation, TransportContext, TransportRequest
# ---------------------------------------------------------------------------


class OperationInformation:
    __slots__ = ("id", "verb", "type", "calling_client", "usage_id")

    def __init__(
        self,
        id: str,
        verb: str,
        type: str,
        calling_client: str,
        usage_id: str,
    ) -> None:
        self.id = id
        self.verb = verb
        self.type = type
        self.calling_client = calling_client
        self.usage_id = usage_id

    @property
    def callingClient(self) -> str:
        return self.calling_client

    @property
    def usageId(self) -> str:
        return self.usage_id


class CallInformation:
    def __init__(
        self,
        locale: str,
        data_tenant: str,
        characters: Any,
        attributes: List[Attribute],
        path_params: List[Attribute],
        transaction_id: UUID,
        correlation_id: Optional[UUID] = None,
    ) -> None:
        self.locale = locale
        self.data_tenant = data_tenant
        self.characters = characters
        self.attributes = attributes
        self.path_params = path_params
        self.transaction_id = transaction_id
        self.correlation_id = correlation_id

    # camelCase aliases
    @property
    def dataTenant(self) -> str:
        return self.data_tenant

    @dataTenant.setter
    def dataTenant(self, v: str) -> None:
        self.data_tenant = v

    @property
    def pathParams(self) -> List[Attribute]:
        return self.path_params

    @pathParams.setter
    def pathParams(self, v: List[Attribute]) -> None:
        self.path_params = v

    @property
    def transactionId(self) -> UUID:
        return self.transaction_id

    @transactionId.setter
    def transactionId(self, v: UUID) -> None:
        self.transaction_id = v

    @property
    def correlationId(self) -> Optional[UUID]:
        return self.correlation_id

    @correlationId.setter
    def correlationId(self, v: Optional[UUID]) -> None:
        self.correlation_id = v

    @staticmethod
    def new(locale: str, data_tenant: Optional[str] = None, transaction_id: Optional[UUID] = None) -> CallInformation:
        return CallInformation(
            locale,
            data_tenant if data_tenant else "",
            Characters(),
            [],
            [],
            transaction_id if transaction_id else generate_uuid(),
        )

    def clone(self) -> CallInformation:
        return CallInformation(
            self.locale,
            self.data_tenant,
            self.characters,
            list(self.attributes),
            list(self.path_params),
            self.transaction_id,
            self.correlation_id,
        )


class TransportContext:
    def __init__(
        self,
        operation: OperationInformation,
        call: CallInformation,
        serializer: Any,
    ) -> None:
        self.operation = operation
        self.call = call
        self.serializer = serializer

    def has_attribute(self, name: str) -> bool:
        return any(item.name == name for item in self.call.attributes)

    hasAttribute = has_attribute

    def get_attribute(self, name: str) -> Any:
        for item in self.call.attributes:
            if item.name == name:
                return item.value
        return None

    getAttribute = get_attribute

    def has_path_parameter(self, name: str) -> bool:
        return any(item.name == name for item in self.call.path_params)

    hasPathParameter = has_path_parameter

    def get_path_parameter(self, name: str) -> Any:
        for item in self.call.path_params:
            if item.name == name:
                return item.value
        return None

    getPathParameter = get_path_parameter

    @staticmethod
    def from_request(gateway_request: TransportRequest) -> TransportContext:
        if not gateway_request.serializer:
            raise RuntimeError("Serializer required to convert from gateway request")
        return TransportContext(
            OperationInformation(
                gateway_request.operation_id,
                gateway_request.operation_verb,
                gateway_request.operation_type,
                gateway_request.calling_client,
                gateway_request.usage_id,
            ),
            CallInformation(
                gateway_request.locale,
                gateway_request.data_tenant,
                gateway_request.characters,
                gateway_request.attributes,
                gateway_request.path_params,
                gateway_request.transaction_id,
                gateway_request.correlation_id,
            ),
            gateway_request.serializer,
        )

    # Alias: TS uses TransportContext.from()
    @staticmethod
    def from_(gateway_request: TransportRequest) -> TransportContext:
        return TransportContext.from_request(gateway_request)

    def deserialize_result(self, data: str) -> Result:
        return Result.deserialize_result(self.serializer, data)

    deserializeResult = deserialize_result

    def serialize_request(self, input: Any) -> str:
        return TransportRequest.from_context(input, self).serialize()

    serializeRequest = serialize_request

    def serialize_event(self, input: Any) -> str:
        return TransportEvent.from_context(input, self).serialize()

    serializeEvent = serialize_event

    @staticmethod
    def from_event(transport_event: TransportEvent) -> TransportContext:
        if not transport_event.serializer:
            raise RuntimeError("Serializer required to convert from transport event")
        return TransportContext(
            OperationInformation(
                transport_event.event_id,
                transport_event.event_type,
                "event",
                transport_event.calling_client,
                transport_event.usage_id,
            ),
            CallInformation(
                transport_event.locale,
                transport_event.data_tenant,
                transport_event.characters,
                transport_event.attributes,
                transport_event.path_params,
                transport_event.transaction_id,
                transport_event.correlation_id,
            ),
            transport_event.serializer,
        )

    fromEvent = from_event


class TransportRequest(Generic[T]):
    def __init__(
        self,
        operation_id: str,
        operation_verb: str,
        operation_type: str,
        calling_client: str,
        usage_id: str,
        transaction_id: UUID,
        data_tenant: str,
        locale: str,
        characters: Any,
        attributes: List[Attribute],
        path_params: List[Attribute],
        request_json: Any,
        raw: Optional[str] = None,
        correlation_id: Optional[UUID] = None,
    ) -> None:
        self.operation_id = operation_id
        self.operation_verb = operation_verb
        self.operation_type = operation_type
        self.calling_client = calling_client
        self.usage_id = usage_id
        self.transaction_id = transaction_id
        self.data_tenant = data_tenant
        self.locale = locale
        self.characters = characters
        self.attributes = attributes
        self.path_params = path_params
        self.request_json = request_json
        self.raw = raw
        self.correlation_id = correlation_id
        self.serializer: Optional[Any] = None

    # camelCase aliases
    @property
    def operationId(self) -> str:
        return self.operation_id

    @property
    def operationVerb(self) -> str:
        return self.operation_verb

    @property
    def operationType(self) -> str:
        return self.operation_type

    @property
    def callingClient(self) -> str:
        return self.calling_client

    @property
    def usageId(self) -> str:
        return self.usage_id

    @property
    def transactionId(self) -> UUID:
        return self.transaction_id

    @property
    def dataTenant(self) -> str:
        return self.data_tenant

    @property
    def requestJson(self) -> Any:
        return self.request_json

    @property
    def pathParams(self) -> List[Attribute]:
        return self.path_params

    @property
    def correlationId(self) -> Optional[UUID]:
        return self.correlation_id

    @staticmethod
    def from_serialized(serializer: Any, serialized: str) -> TransportRequest:
        tr = TransportRequest(
            "", "", "", "", "", "", "", "",
            Characters(), [], [], {},
            None,
        )
        tr.serializer = serializer
        return tr._deserialize(serialized)

    fromSerialized = from_serialized

    @staticmethod
    def from_context(input: Any, ctx: TransportContext) -> TransportRequest:
        tr = TransportRequest(
            ctx.operation.id,
            ctx.operation.verb,
            ctx.operation.type,
            ctx.operation.calling_client,
            ctx.operation.usage_id,
            ctx.call.transaction_id if ctx.call.transaction_id else generate_uuid(),
            ctx.call.data_tenant or "",
            ctx.call.locale,
            ctx.call.characters,
            ctx.call.attributes,
            ctx.call.path_params,
            input,
            correlation_id=ctx.call.correlation_id,
        )
        tr.serializer = ctx.serializer
        return tr

    # Alias: TS uses TransportRequest.from()
    @staticmethod
    def from_(input: Any, ctx: TransportContext) -> TransportRequest:
        return TransportRequest.from_context(input, ctx)

    def assign_serializer(self, serializer: Any) -> TransportRequest:
        self.serializer = serializer
        return self

    assignSerializer = assign_serializer

    def serialize(self) -> str:
        if not self.serializer:
            raise RuntimeError("No serializer assigned to TransportRequest")
        return self.serializer.serialize(self)

    def _to_dict(self) -> Dict[str, Any]:
        chars: Any = self.characters
        if isinstance(chars, Characters):
            chars_d: Dict[str, Any] = {}
            if chars.performer is not None:
                chars_d["performer"] = chars.performer
            if chars.responsible is not None:
                chars_d["responsible"] = chars.responsible
            if chars.subject is not None:
                chars_d["subject"] = chars.subject
            chars = chars_d
        elif isinstance(chars, CharacterMetaValues):
            chars_d = {}
            if chars.performer is not None:
                chars_d["performer"] = chars.performer
            if chars.responsible is not None:
                chars_d["responsible"] = chars.responsible
            if chars.subject is not None:
                chars_d["subject"] = chars.subject
            chars = chars_d

        d: Dict[str, Any] = {
            "operationId": self.operation_id,
            "operationVerb": self.operation_verb,
            "operationType": self.operation_type,
            "callingClient": self.calling_client,
            "usageId": self.usage_id,
            "transactionId": self.transaction_id,
            "dataTenant": self.data_tenant,
            "locale": self.locale,
            "characters": chars,
            "attributes": self.attributes,
            "pathParams": self.path_params,
            "requestJson": self.request_json,
        }
        if self.correlation_id is not None:
            d["correlationId"] = self.correlation_id
        return d

    def _deserialize(self, serialized: str) -> TransportRequest:
        if not self.serializer:
            raise RuntimeError("No serializer assigned to TransportRequest")
        d = self.serializer.deserialize(serialized)

        # Parse characters
        chars_data = d.get("characters", {})
        characters = Characters(
            performer=_parse_identifier(chars_data.get("performer")) if chars_data.get("performer") else None,
            responsible=_parse_identifier(chars_data.get("responsible")) if chars_data.get("responsible") else None,
            subject=_parse_identifier(chars_data.get("subject")) if chars_data.get("subject") else None,
        )

        # Parse attributes
        attrs = [Attribute(a["name"], a["value"]) for a in d.get("attributes", [])]
        path_params = [Attribute(a["name"], a["value"]) for a in d.get("pathParams", [])]

        new_tr = TransportRequest(
            d.get("operationId", ""),
            d.get("operationVerb", ""),
            d.get("operationType", ""),
            d.get("callingClient", ""),
            d.get("usageId", ""),
            d.get("transactionId", ""),
            d.get("dataTenant", ""),
            d.get("locale", ""),
            characters,
            attrs,
            path_params,
            d.get("requestJson"),
            serialized,
            correlation_id=d.get("correlationId"),
        )
        new_tr.serializer = self.serializer
        return new_tr


def _parse_identifier(data: Any) -> Optional[Identifier]:
    if data is None:
        return None
    if isinstance(data, dict):
        return Identifier(data.get("id", ""), data.get("type"))
    return None


class TransportEvent(Generic[T]):
    def __init__(
        self,
        event_id: str,
        event_type: str,
        calling_client: str,
        usage_id: str,
        transaction_id: UUID,
        data_tenant: str,
        locale: str,
        characters: Any,
        attributes: List[Attribute],
        path_params: List[Attribute],
        request_json: Any,
        raw: Optional[str] = None,
        correlation_id: Optional[UUID] = None,
    ) -> None:
        self.event_id = event_id
        self.event_type = event_type
        self.calling_client = calling_client
        self.usage_id = usage_id
        self.transaction_id = transaction_id
        self.data_tenant = data_tenant
        self.locale = locale
        self.characters = characters
        self.attributes = attributes
        self.path_params = path_params
        self.request_json = request_json
        self.raw = raw
        self.correlation_id = correlation_id
        self.serializer: Optional[Any] = None

    # camelCase aliases
    @property
    def eventId(self) -> str:
        return self.event_id

    @property
    def eventType(self) -> str:
        return self.event_type

    @property
    def callingClient(self) -> str:
        return self.calling_client

    @property
    def usageId(self) -> str:
        return self.usage_id

    @property
    def transactionId(self) -> UUID:
        return self.transaction_id

    @property
    def dataTenant(self) -> str:
        return self.data_tenant

    @property
    def requestJson(self) -> Any:
        return self.request_json

    @property
    def pathParams(self) -> List[Attribute]:
        return self.path_params

    @property
    def correlationId(self) -> Optional[UUID]:
        return self.correlation_id

    @staticmethod
    def from_serialized(serializer: Any, serialized: str) -> TransportEvent:
        te = TransportEvent(
            "", "", "", "", "", "", "",
            Characters(), [], [], {},
            None,
        )
        te.serializer = serializer
        return te._deserialize(serialized)

    fromSerialized = from_serialized

    @staticmethod
    def from_context(input: Any, ctx: TransportContext) -> TransportEvent:
        te = TransportEvent(
            ctx.operation.id,
            ctx.operation.verb,
            ctx.operation.calling_client,
            ctx.operation.usage_id,
            ctx.call.transaction_id if ctx.call.transaction_id else generate_uuid(),
            ctx.call.data_tenant or "",
            ctx.call.locale,
            ctx.call.characters,
            ctx.call.attributes,
            ctx.call.path_params,
            input,
            correlation_id=ctx.call.correlation_id,
        )
        te.serializer = ctx.serializer
        return te

    # Alias: TS uses TransportEvent.from()
    @staticmethod
    def from_(input: Any, ctx: TransportContext) -> TransportEvent:
        return TransportEvent.from_context(input, ctx)

    def assign_serializer(self, serializer: Any) -> TransportEvent:
        self.serializer = serializer
        return self

    assignSerializer = assign_serializer

    def serialize(self) -> str:
        if not self.serializer:
            raise RuntimeError("No serializer assigned to TransportEvent")
        return self.serializer.serialize(self)

    def _to_dict(self) -> Dict[str, Any]:
        chars: Any = self.characters
        if isinstance(chars, Characters):
            chars_d: Dict[str, Any] = {}
            if chars.performer is not None:
                chars_d["performer"] = chars.performer
            if chars.responsible is not None:
                chars_d["responsible"] = chars.responsible
            if chars.subject is not None:
                chars_d["subject"] = chars.subject
            chars = chars_d
        elif isinstance(chars, CharacterMetaValues):
            chars_d = {}
            if chars.performer is not None:
                chars_d["performer"] = chars.performer
            if chars.responsible is not None:
                chars_d["responsible"] = chars.responsible
            if chars.subject is not None:
                chars_d["subject"] = chars.subject
            chars = chars_d

        d: Dict[str, Any] = {
            "eventId": self.event_id,
            "eventType": self.event_type,
            "callingClient": self.calling_client,
            "usageId": self.usage_id,
            "transactionId": self.transaction_id,
            "dataTenant": self.data_tenant,
            "locale": self.locale,
            "characters": chars,
            "attributes": self.attributes,
            "pathParams": self.path_params,
            "requestJson": self.request_json,
        }
        if self.correlation_id is not None:
            d["correlationId"] = self.correlation_id
        return d

    def _deserialize(self, serialized: str) -> TransportEvent:
        if not self.serializer:
            raise RuntimeError("No serializer assigned to TransportEvent")
        d = self.serializer.deserialize(serialized)

        chars_data = d.get("characters", {})
        characters = Characters(
            performer=_parse_identifier(chars_data.get("performer")) if chars_data.get("performer") else None,
            responsible=_parse_identifier(chars_data.get("responsible")) if chars_data.get("responsible") else None,
            subject=_parse_identifier(chars_data.get("subject")) if chars_data.get("subject") else None,
        )

        attrs = [Attribute(a["name"], a["value"]) for a in d.get("attributes", [])]
        path_params = [Attribute(a["name"], a["value"]) for a in d.get("pathParams", [])]

        new_te = TransportEvent(
            d.get("eventId", ""),
            d.get("eventType", ""),
            d.get("callingClient", ""),
            d.get("usageId", ""),
            d.get("transactionId", ""),
            d.get("dataTenant", ""),
            d.get("locale", ""),
            characters,
            attrs,
            path_params,
            d.get("requestJson"),
            serialized,
            correlation_id=d.get("correlationId"),
        )
        new_te.serializer = self.serializer
        return new_te


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

# Type aliases for interceptors
RequestInterceptor = Callable[[Any, TransportContext], Awaitable[Result]]
MessageInterceptor = Callable[[Any, TransportContext], Awaitable[Result]]
EventInterceptor = Callable[[Any, TransportContext], Awaitable[Result]]
RequestInspector = Callable[[Any, TransportContext], Awaitable[Optional[Result]]]
ResponseInspector = Callable[[Result, Any, TransportContext], Awaitable[Result]]


class TransportDispatcher:
    def __init__(
        self,
        session_identifier: str,
        context_cache: Any,
        cache_reads: bool,
    ) -> None:
        self.session_identifier = session_identifier
        self.context_cache = context_cache
        self.cache_reads = cache_reads
        self.requests_inspector: Optional[RequestInspector] = None
        self.responses_inspector: Optional[ResponseInspector] = None
        self._request_handlers: Dict[str, RequestInterceptor] = {}
        self._message_handlers: Dict[str, MessageInterceptor] = {}
        self._pattern_handlers: Dict[str, RequestInterceptor] = {}
        self._sort_patterns = False
        self._sorted_patterns: List[str] = []
        self._event_handlers: Dict[str, List[EventInterceptor]] = {}
        self._event_pattern_handlers: Dict[str, List[EventInterceptor]] = {}
        self._sort_event_patterns = False
        self._sorted_event_patterns: List[str] = []

    # camelCase aliases
    @property
    def requestsInspector(self) -> Optional[RequestInspector]:
        return self.requests_inspector

    @requestsInspector.setter
    def requestsInspector(self, v: Optional[RequestInspector]) -> None:
        self.requests_inspector = v

    @property
    def responsesInspector(self) -> Optional[ResponseInspector]:
        return self.responses_inspector

    @responsesInspector.setter
    def responsesInspector(self, v: Optional[ResponseInspector]) -> None:
        self.responses_inspector = v

    @property
    def contextCache(self) -> Any:
        return self.context_cache

    @contextCache.setter
    def contextCache(self, v: Any) -> None:
        self.context_cache = v

    @property
    def sessionIdentifier(self) -> str:
        return self.session_identifier

    @property
    def cacheReads(self) -> bool:
        return self.cache_reads

    def add_request_handler(self, operation_id: str, handler: RequestInterceptor) -> None:
        self._validate_unique_handler(operation_id)
        self._request_handlers[operation_id] = handler

    addRequestHandler = add_request_handler

    def add_message_handler(self, operation_id: str, handler: MessageInterceptor) -> None:
        self._validate_unique_handler(operation_id)
        self._message_handlers[operation_id] = handler

    addMessageHandler = add_message_handler

    def add_pattern_handler(self, pattern: str, handler: RequestInterceptor) -> None:
        self._validate_unique_handler(pattern)
        self._pattern_handlers[pattern] = handler
        self._sort_patterns = True

    addPatternHandler = add_pattern_handler

    def add_event_handler(self, event_id: str, handler: EventInterceptor) -> None:
        existing = self._event_handlers.get(event_id)
        if existing is not None:
            existing.append(handler)
        else:
            self._event_handlers[event_id] = [handler]

    addEventHandler = add_event_handler

    def add_event_pattern_handler(self, pattern: str, handler: EventInterceptor) -> None:
        existing = self._event_pattern_handlers.get(pattern)
        if existing is not None:
            existing.append(handler)
        else:
            self._event_pattern_handlers[pattern] = [handler]
            self._sort_event_patterns = True

    addEventPatternHandler = add_event_pattern_handler

    async def route_from_gateway_request(self, input: Any, ctx: TransportContext) -> Result:
        if ctx.operation.type == "request":
            return await self.handle_as_request(input, ctx)
        else:
            return (await self.handle_as_message(input, ctx)).as_generic()

    routeFromGatewayRequest = route_from_gateway_request

    async def handle_as_message(self, input: Any, ctx: TransportContext, match_sessions: bool = False) -> Result:
        inspection_result = await self._inspect_request(input, ctx)
        cache_result = await self._handle_cache(ctx, match_sessions)
        if not cache_result.success:
            return cache_result
        if inspection_result:
            return inspection_result
        handler = self._message_handlers.get(cache_result.value.operation.id)
        if handler:
            return await self._run_message_handler(handler, input, cache_result.value)
        return await self._run_pattern_handler(input, cache_result.value)

    handleAsMessage = handle_as_message

    async def handle_as_request(self, input: Any, ctx: TransportContext, match_sessions: bool = False) -> Result:
        inspection_result = await self._inspect_request(input, ctx)
        cache_result = await self._handle_cache(ctx, match_sessions)
        if not cache_result.success:
            return cache_result
        if inspection_result:
            return inspection_result
        handler = self._request_handlers.get(cache_result.value.operation.id)
        if handler:
            return await self._run_request_handler(handler, input, cache_result.value)
        return await self._run_pattern_handler(input, cache_result.value)

    handleAsRequest = handle_as_request

    async def handle_as_event(self, input: Any, ctx: TransportContext, match_sessions: bool = False) -> Result:
        inspection_result = await self._inspect_request(input, ctx)
        cache_result = await self._handle_cache(ctx, match_sessions)
        if not cache_result.success:
            return cache_result.as_generic()
        if inspection_result:
            return inspection_result

        event_id = cache_result.value.operation.id
        specific = self._event_handlers.get(event_id, [])
        matching_pattern = self._find_matching_event_pattern(event_id)
        pattern_handlers = (
            self._event_pattern_handlers.get(matching_pattern, []) if matching_pattern else []
        )

        all_handlers: List[Dict[str, Any]] = []
        for idx, h in enumerate(specific):
            sub_id = f"{event_id}#{idx}" if len(specific) > 1 else event_id
            all_handlers.append({"id": sub_id, "handler": h})
        for idx, h in enumerate(pattern_handlers):
            sub_id = (
                f"{matching_pattern}#{idx}" if len(pattern_handlers) > 1 else matching_pattern
            )
            all_handlers.append({"id": sub_id, "handler": h})

        if not all_handlers:
            return await self._inspect_response(
                self._handler_not_found(event_id).as_generic(), input, cache_result.value
            )

        async def run_one(sub_id: str, handler: EventInterceptor) -> Dict[str, Any]:
            try:
                r = await handler(input, cache_result.value)
                return {"id": sub_id, "result": r}
            except Exception as e:
                return {"id": sub_id, "result": self._generic_error(e)}

        outcomes = await asyncio.gather(
            *[run_one(entry["id"], entry["handler"]) for entry in all_handlers]
        )

        failed: List[TransportError] = []
        for outcome in outcomes:
            r: Result = outcome["result"]
            if not r.success:
                err = r.error if r.error is not None else TransportError(
                    "TransportSession.DispatchHandlerFailed",
                    TransportErrorDetails(
                        technical_error="Handler returned failed result without error",
                    ),
                )
                cloned_details = TransportErrorDetails(
                    technical_error=err.details.technical_error if err.details else None,
                    user_error=err.details.user_error if err.details else None,
                    session_identifier=err.details.session_identifier if err.details else None,
                    calling_client=err.details.calling_client if err.details else None,
                    calling_usage=err.details.calling_usage if err.details else None,
                    called_operation=outcome["id"],
                    transaction_id=err.details.transaction_id if err.details else None,
                )
                failed.append(TransportError(
                    err.code,
                    cloned_details,
                    list(err.related) if err.related else [],
                    err.parent,
                ))

        if not failed:
            aggregated = Result.ok().as_generic()
        else:
            aggregated = Result.failed(
                500,
                TransportError(
                    "TransportSession.DispatchPartialFailure",
                    TransportErrorDetails(
                        technical_error=f"{len(failed)} of {len(all_handlers)} subscriber(s) failed",
                    ),
                    failed,
                    None,
                ),
            )

        return await self._inspect_response(aggregated, input, cache_result.value)

    handleAsEvent = handle_as_event

    def _find_matching_event_pattern(self, event_id: str) -> Optional[str]:
        if self._sort_event_patterns:
            keys = list(self._event_pattern_handlers.keys())
            keys.sort(key=lambda a: -len(a.lower()))
            self._sorted_event_patterns = keys
            self._sort_event_patterns = False
        for key in self._sorted_event_patterns:
            if event_id.startswith(key):
                return key
        return None

    def _validate_unique_handler(self, id: str) -> None:
        if id in self._request_handlers or id in self._message_handlers or id in self._pattern_handlers:
            raise RuntimeError(f"The path {id} already has a handler")

    async def _handle_cache(self, ctx: TransportContext, match_sessions: bool) -> Result:
        if self.cache_reads:
            return Result.ok(ctx)
        try:
            result = await self.context_cache.put(ctx.call.transaction_id, ctx.call)
            if not result:
                return Result.failed(
                    500,
                    "TransportSession.ContextCachePersistance",
                    f"The incoming context could not be presisted for transaction {ctx.call.transaction_id}",
                )
            return Result.ok(ctx)
        except Exception as e:
            Logger.error(str(e))
            return self._generic_error(e)

    async def get_call_info_from_cache(
        self, new_transaction_id: str, call_info: CallInformation, match_sessions: bool
    ) -> CallInformation:
        if not self.cache_reads:
            return call_info
        if not match_sessions:
            return call_info
        try:
            result = await self.context_cache.get(new_transaction_id)
            if not result:
                Logger.error(f"Failed to read context cache for recrod {call_info.transaction_id}")
                return call_info
            return result
        except Exception as e:
            Logger.error(str(e))
            return call_info

    getCallInfoFromCache = get_call_info_from_cache

    async def _run_pattern_handler(self, input: Any, ctx: TransportContext) -> Result:
        matching_pattern = self._find_matching_pattern(ctx.operation.id)
        if matching_pattern:
            pattern_handler = self._pattern_handlers.get(matching_pattern)
            if pattern_handler:
                return await self._run_request_handler(pattern_handler, input, ctx)
            return await self._inspect_response(self._handler_not_found(ctx.operation.id).as_generic(), input, ctx)
        return await self._inspect_response(self._handler_not_found(ctx.operation.id).as_generic(), input, ctx)

    async def _run_message_handler(self, handler: MessageInterceptor, input: Any, ctx: TransportContext) -> Result:
        try:
            result = (await handler(input, ctx)).convert()
        except Exception as e:
            result = self._generic_error(e)
        return await self._inspect_response(result, input, ctx)

    async def _run_request_handler(self, handler: RequestInterceptor, input: Any, ctx: TransportContext) -> Result:
        try:
            result = await handler(input, ctx)
        except Exception as e:
            result = self._generic_error(e)
        return await self._inspect_response(result, input, ctx)

    def _find_matching_pattern(self, feature_id: str) -> Optional[str]:
        if self._sort_patterns:
            self._re_sort_patterns()
        for key in self._sorted_patterns:
            if feature_id.startswith(key):
                return key
        return None

    def _re_sort_patterns(self) -> None:
        keys = list(self._pattern_handlers.keys())
        keys.sort(key=lambda a: -len(a.lower()))
        self._sorted_patterns = keys
        self._sort_patterns = False

    async def _inspect_request(self, cinput: Any, ctx: TransportContext) -> Optional[Result]:
        if self.requests_inspector is None:
            return None
        try:
            return await self.requests_inspector(cinput, ctx)
        except Exception as e:
            Logger.error(str(e))
        return None

    async def inspect_message_response(self, result: Result, cinput: Any, ctx: TransportContext) -> Result:
        if self.responses_inspector is None:
            return result
        try:
            return await self.responses_inspector(result, cinput, ctx)
        except Exception as e:
            Logger.error(str(e))
        return result

    inspectMessageResponse = inspect_message_response

    async def _inspect_response(self, result: Result, cinput: Any, ctx: TransportContext) -> Result:
        self._enrich_error(result, ctx)
        if self.responses_inspector is None:
            return result
        try:
            await self.responses_inspector(result, cinput, ctx)
        except Exception as e:
            Logger.error(str(e))
        return result

    inspectResponse = _inspect_response

    def _enrich_error(self, result: Result, ctx: TransportContext) -> None:
        if result.error and result.error.details:
            result.error.details.called_operation = ctx.operation.id
            result.error.details.calling_client = ctx.operation.calling_client
            result.error.details.calling_usage = ctx.operation.usage_id

    def _handler_not_found(self, operation_id: str) -> Result:
        return Result.bad_request(
            "TransportSession.HandlerNotFound",
            f"There are no matching handlers for the operation: {operation_id}",
        )

    def _generic_error(self, e: Optional[Exception]) -> Result:
        if e:
            return Result.failed(
                500,
                "TransportSession.UnhandledError",
                f"{e}: {type(e).__name__}" + (f"\n{traceback.format_exc()}" if traceback.format_exc() else ""),
            )
        return Result.failed(500, "TransportSession.UnhandledError", "Unknown error")


# ---------------------------------------------------------------------------
# Session / Client
# ---------------------------------------------------------------------------


class TransportSessionConfiguration:
    def __init__(
        self,
        locale: str,
        interceptors: TransportDispatcher,
        serializer: Any,
    ) -> None:
        self.locale = locale
        self.interceptors = interceptors
        self.serializer = serializer


class TransportSession:
    def __init__(self, config: TransportSessionConfiguration, match_sessions: bool = False) -> None:
        self._config = config
        self._match_sessions = match_sessions

    def with_locale(self, locale: str) -> TransportSession:
        self._config.locale = locale
        return self

    withLocale = with_locale

    async def accept_incoming_request(self, json_str: str, custom_attributes: Optional[List[Attribute]] = None) -> Result:
        if custom_attributes is None:
            custom_attributes = []
        tr = TransportRequest.from_serialized(self._config.serializer, json_str)
        ctx = TransportContext.from_request(tr)
        for attribute in custom_attributes:
            if ctx.get_attribute(attribute.name) is not None:
                continue
            ctx.call.attributes.append(attribute)
        if ctx.operation.type == "request":
            result = await self._config.interceptors.handle_as_request(tr.request_json, ctx)
            return result.assign_serializer(self._config.serializer)
        else:
            result = await self._config.interceptors.handle_as_message(tr.request_json, ctx)
            return result.assign_serializer(self._config.serializer)

    acceptIncomingRequest = accept_incoming_request

    async def accept_incoming_event(self, json_str: str, custom_attributes: Optional[List[Attribute]] = None) -> Result:
        if custom_attributes is None:
            custom_attributes = []
        te = TransportEvent.from_serialized(self._config.serializer, json_str)
        ctx = TransportContext.from_event(te)
        for attribute in custom_attributes:
            if ctx.get_attribute(attribute.name) is not None:
                continue
            ctx.call.attributes.append(attribute)
        result = await self._config.interceptors.handle_as_event(te.request_json, ctx)
        return result.assign_serializer(self._config.serializer)

    acceptIncomingEvent = accept_incoming_event

    async def accept_event(self, event: OutOfContextEvent, custom_attributes: Optional[List[Attribute]] = None) -> Result:
        if custom_attributes is None:
            custom_attributes = []
        call_info = CallInformation.new(self._config.locale)
        call_info.correlation_id = event.correlation_id
        ctx = TransportContext(
            OperationInformation(
                event.event_id,
                event.event_type,
                "event",
                "",
                event.usage_id,
            ),
            call_info,
            self._config.serializer,
        )
        if event.path_parameters:
            for param in event.path_parameters:
                if ctx.has_path_parameter(param.name):
                    continue
                ctx.call.path_params.append(Attribute(param.name, param.value))
        for attribute in custom_attributes:
            if ctx.get_attribute(attribute.name) is not None:
                continue
            ctx.call.attributes.append(attribute)
        result = await self._config.interceptors.handle_as_event(event.request_json, ctx)
        return result.assign_serializer(self._config.serializer)

    acceptEvent = accept_event

    def create_client(self, client_identifier: str, data_tenant: Optional[str] = None) -> TransportClient:
        info = CallInformation.new(self._config.locale, data_tenant)
        return TransportClient(client_identifier, self._config, info, self._match_sessions)

    createClient = create_client

    def get_serializer(self) -> Any:
        return self._config.serializer

    getSerializer = get_serializer


class TransportClient:
    def __init__(
        self,
        client_identifier: str,
        config: TransportSessionConfiguration,
        call_information: CallInformation,
        match_sessions: bool = False,
    ) -> None:
        self._client_identifier = client_identifier
        self._config = config
        self._call_info = call_information
        self._match_sessions = match_sessions

    async def with_transaction_id(self, transaction_id: UUID) -> TransportClient:
        new_call_info = (
            await self._config.interceptors.get_call_info_from_cache(
                transaction_id, self._call_info, self._match_sessions
            )
        ).clone()
        new_call_info.transaction_id = transaction_id
        return TransportClient(self._client_identifier, self._config, new_call_info, self._match_sessions)

    withTransactionId = with_transaction_id

    def with_locale(self, locale: str) -> TransportClient:
        new_call_info = self._call_info.clone()
        new_call_info.locale = locale
        return TransportClient(self._client_identifier, self._config, new_call_info, self._match_sessions)

    withLocale = with_locale

    def with_data_tenant(self, tenant: str) -> TransportClient:
        new_call_info = self._call_info.clone()
        new_call_info.data_tenant = tenant
        return TransportClient(self._client_identifier, self._config, new_call_info, self._match_sessions)

    withDataTenant = with_data_tenant

    def with_correlation_id(self, correlation_id: UUID) -> TransportClient:
        new_call_info = self._call_info.clone()
        new_call_info.correlation_id = correlation_id
        return TransportClient(self._client_identifier, self._config, new_call_info, self._match_sessions)

    withCorrelationId = with_correlation_id

    def with_characters(self, characters: Any) -> TransportClient:
        new_call_info = self._call_info.clone()
        new_call_info.characters = characters
        return TransportClient(self._client_identifier, self._config, new_call_info, self._match_sessions)

    withCharacters = with_characters

    def add_attribute(self, name: str, value: Any) -> TransportClient:
        new_call_info = self._call_info.clone()
        for attr in new_call_info.attributes:
            if attr.name == name:
                attr.value = value
                return TransportClient(self._client_identifier, self._config, new_call_info, self._match_sessions)
        new_call_info.attributes.append(Attribute(name, value))
        return TransportClient(self._client_identifier, self._config, new_call_info, self._match_sessions)

    addAttribute = add_attribute

    def remove_attribute(self, name: str) -> TransportClient:
        new_call_info = self._call_info.clone()
        new_call_info.attributes = [a for a in new_call_info.attributes if a.name != name]
        return TransportClient(self._client_identifier, self._config, new_call_info, self._match_sessions)

    removeAttribute = remove_attribute

    def add_path_param(self, name: str, value: Any) -> TransportClient:
        new_call_info = self._call_info.clone()
        for param in new_call_info.path_params:
            if param.name == name:
                param.value = value
                return TransportClient(self._client_identifier, self._config, new_call_info, self._match_sessions)
        new_call_info.path_params.append(Attribute(name, value))
        return TransportClient(self._client_identifier, self._config, new_call_info, self._match_sessions)

    addPathParam = add_path_param

    def remove_path_param(self, name: str) -> TransportClient:
        new_call_info = self._call_info.clone()
        new_call_info.path_params = [a for a in new_call_info.path_params if a.name != name]
        return TransportClient(self._client_identifier, self._config, new_call_info, self._match_sessions)

    removePathParam = remove_path_param

    def get_serializer(self) -> Any:
        return self._config.serializer

    getSerializer = get_serializer

    async def request(self, call: OperationRequest) -> Result:
        request_call_info = self._call_info.clone()
        if not request_call_info.transaction_id:
            request_call_info.transaction_id = generate_uuid()
        ctx = TransportContext(
            call.as_operation_information(self._client_identifier),
            request_call_info,
            self._config.serializer,
        )
        if isinstance(call, RequestOperationRequest):
            return await self._config.interceptors.handle_as_request(call.input, ctx, self._match_sessions)
        else:
            return await self._config.interceptors.handle_as_message(call.input, ctx, self._match_sessions)

    async def dispatch(self, call: EventDispatchRequest, timeout_ms: int = 30_000) -> Result:
        request_call_info = self._call_info.clone()
        request_call_info.transaction_id = generate_uuid()
        ctx = TransportContext(
            call.as_operation_information(self._client_identifier),
            request_call_info,
            self._config.serializer,
        )
        try:
            return await asyncio.wait_for(
                self._config.interceptors.handle_as_event(call.input, ctx, self._match_sessions),
                timeout=timeout_ms / 1000,
            )
        except asyncio.TimeoutError:
            return Result.failed(
                504,
                TransportError(
                    "TransportSession.DispatchTimeout",
                    TransportErrorDetails(
                        technical_error=f"Dispatch exceeded {timeout_ms}ms",
                    ),
                ),
            )

    async def accept_operation(self, operation: OutOfContextOperation, custom_attributes: Optional[List[Attribute]] = None) -> Result:
        if custom_attributes is None:
            custom_attributes = []

        if operation.operation_type == "request":
            op = TransportOperation("request", operation.operation_id, operation.operation_verb, [], TransportOperationSettings(False))
            call: OperationRequest = RequestOperationRequest(
                operation.usage_id,
                op,
                operation.request_json,
            )
        else:
            op = TransportOperation("message", operation.operation_id, operation.operation_verb, [], TransportOperationSettings(False))
            call = MessageOperationRequest(
                operation.usage_id,
                op,
                operation.request_json,
            )

        request_call_info = self._call_info.clone()
        if not request_call_info.transaction_id:
            request_call_info.transaction_id = generate_uuid()

        ctx = TransportContext(
            call.as_operation_information(self._client_identifier),
            request_call_info,
            self._config.serializer,
        )

        # Add path parameters from operation
        operation_path_params = []
        if operation.path_parameters:
            for param in operation.path_parameters:
                operation_path_params.append(Attribute(param.name, param.value))
        for param in operation_path_params:
            if ctx.has_path_parameter(param.name):
                continue
            ctx.call.path_params.append(param)

        # Append missing attributes
        for attribute in custom_attributes:
            if ctx.get_attribute(attribute.name) is not None:
                continue
            ctx.call.attributes.append(attribute)

        if ctx.operation.type == "request":
            result = await self._config.interceptors.handle_as_request(operation.request_json, ctx)
            return result.assign_serializer(self._config.serializer)
        else:
            result = await self._config.interceptors.handle_as_message(operation.request_json, ctx)
            return result.assign_serializer(self._config.serializer)

    acceptOperation = accept_operation


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


class TransportSessionBuilder:
    def __init__(self, identifier: str) -> None:
        self._config = TransportSessionConfiguration(
            locale="en-GB",
            interceptors=TransportDispatcher(identifier, InMemoryContextCache(), False),
            serializer=DefaultTransportSerializer(),
        )

    def setup_outbound_context_cache(self, cache: Any) -> TransportSessionBuilder:
        self._config.interceptors.context_cache = cache
        return self

    setupOutboundContextCache = setup_outbound_context_cache

    def assign_serializer(self, serializer: Any) -> TransportSessionBuilder:
        self._config.serializer = serializer
        return self

    assignSerializer = assign_serializer

    def intercept(self, handler: OperationHandler) -> TransportSessionBuilder:
        if isinstance(handler, RequestOperationHandler):
            self._config.interceptors.add_request_handler(handler.operation.id, handler.handler)
        elif isinstance(handler, MessageOperationHandler):
            self._config.interceptors.add_message_handler(handler.operation.id, handler.handler)
        return self

    def intercept_pattern(self, pattern: str, handler: RequestInterceptor) -> TransportSessionBuilder:
        self._config.interceptors.add_pattern_handler(pattern, handler)
        return self

    interceptPattern = intercept_pattern

    def subscribe(self, handler: EventOperationHandler) -> TransportSessionBuilder:
        self._config.interceptors.add_event_handler(handler.operation.id, handler.handler)
        return self

    def subscribe_pattern(self, pattern: str, handler: EventInterceptor) -> TransportSessionBuilder:
        self._config.interceptors.add_event_pattern_handler(pattern, handler)
        return self

    subscribePattern = subscribe_pattern

    def inspect_request(self, inspector: RequestInspector) -> TransportSessionBuilder:
        self._config.interceptors.requests_inspector = inspector
        return self

    inspectRequest = inspect_request

    def inspect_response(self, inspector: ResponseInspector) -> TransportSessionBuilder:
        self._config.interceptors.responses_inspector = inspector
        return self

    inspectResponse = inspect_response

    def outbound_session_builder(self, client_identifier: str) -> OutboundSessionBuilder:
        return OutboundSessionBuilder(
            client_identifier,
            self._config.interceptors.context_cache,
            self._config.serializer,
        )

    outboundSessionBuilder = outbound_session_builder

    def build(self) -> TransportSession:
        return TransportSession(self._config)

    def on_log_message(self, logger: Any) -> TransportSessionBuilder:
        Logger.assign_logger(logger)
        return self

    onLogMessage = on_log_message


# Alias
TransportAbstractionBuilder = TransportSessionBuilder


class OutboundSessionBuilder:
    def __init__(self, service_id: str, context_cache: Any, serializer: Any) -> None:
        self._service_id = service_id
        self._config = TransportSessionConfiguration(
            locale="en-GB",
            interceptors=TransportDispatcher(service_id, context_cache, True),
            serializer=serializer,
        )

    def intercept(self, handler: OperationHandler) -> OutboundSessionBuilder:
        if isinstance(handler, RequestOperationHandler):
            self._config.interceptors.add_request_handler(handler.operation.id, handler.handler)
        elif isinstance(handler, MessageOperationHandler):
            self._config.interceptors.add_message_handler(handler.operation.id, handler.handler)
        return self

    def intercept_pattern(self, pattern: str, handler: RequestInterceptor) -> OutboundSessionBuilder:
        self._config.interceptors.add_pattern_handler(pattern, handler)
        return self

    interceptPattern = intercept_pattern

    def subscribe(self, handler: EventOperationHandler) -> OutboundSessionBuilder:
        self._config.interceptors.add_event_handler(handler.operation.id, handler.handler)
        return self

    def subscribe_pattern(self, pattern: str, handler: EventInterceptor) -> OutboundSessionBuilder:
        self._config.interceptors.add_event_pattern_handler(pattern, handler)
        return self

    subscribePattern = subscribe_pattern

    def inspect_request(self, inspector: RequestInspector) -> OutboundSessionBuilder:
        self._config.interceptors.requests_inspector = inspector
        return self

    inspectRequest = inspect_request

    def inspect_response(self, inspector: ResponseInspector) -> OutboundSessionBuilder:
        self._config.interceptors.responses_inspector = inspector
        return self

    inspectResponse = inspect_response

    def build(self) -> OutboundClientFactory:
        return OutboundClientFactory(self._service_id, self._config)


class OutboundClientFactory:
    def __init__(self, service_id: str, config: TransportSessionConfiguration) -> None:
        self._service_id = service_id
        self._config = config

    async def for_incoming_request(self, transaction_id: UUID) -> TransportClient:
        return await (
            TransportSession(self._config, True)
            .create_client(self._service_id)
            .with_transaction_id(transaction_id)
        )

    forIncomingRequest = for_incoming_request

    def as_independent_requests(self) -> TransportClient:
        return TransportSession(self._config).create_client(self._service_id)

    asIndependentRequests = as_independent_requests


class Transport:
    @staticmethod
    def session(identifier: str) -> TransportSessionBuilder:
        return TransportSessionBuilder(identifier)


# ---------------------------------------------------------------------------
# Chat Instructions (built-in AI/LLM integration)
# ---------------------------------------------------------------------------


class ChatInstruction:
    __slots__ = ("type", "role", "content")

    def __init__(self, type: str, role: str, content: str) -> None:
        self.type = type
        self.role = role
        self.content = content


class ProcessChatInstructionInput:
    __slots__ = ("usage_instructions", "current_state_snapshot", "items")

    def __init__(
        self,
        usage_instructions: str,
        current_state_snapshot: str,
        items: List[Any],
    ) -> None:
        self.usage_instructions = usage_instructions
        self.current_state_snapshot = current_state_snapshot
        self.items = items

    # camelCase aliases
    @property
    def usageInstructions(self) -> str:
        return self.usage_instructions

    @property
    def currentStateSnapshot(self) -> str:
        return self.current_state_snapshot


class ProcessChatInstructionOutput:
    __slots__ = ("message", "operations")

    def __init__(
        self,
        message: Optional[str] = None,
        operations: Optional[List[Any]] = None,
    ) -> None:
        self.message = message
        self.operations = operations or []


class ProcessChatInstruction(RequestOperation):
    def __init__(self) -> None:
        super().__init__(
            "PeerColab.Instructions.ProcessChatInstruction",
            "PROCESS",
        )


class PeerColabAI:
    @staticmethod
    def process_chat_instructions(
        input: Any,
    ) -> RequestOperationRequest:
        operation = ProcessChatInstruction()
        return RequestOperationRequest(
            "PeerColab.Instructions",
            operation,
            input,
        )

    # camelCase alias
    processChatInstructions = process_chat_instructions
