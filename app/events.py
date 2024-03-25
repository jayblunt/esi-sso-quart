import dataclasses
import datetime
import functools
import itertools
import typing

counter: typing.Final = itertools.count()


@dataclasses.dataclass(frozen=True)
class AppEvent:
    id: int = dataclasses.field(default_factory=lambda: next(counter))
    ts: datetime.datetime = dataclasses.field(default_factory=functools.partial(datetime.datetime.now, tz=datetime.UTC))


@dataclasses.dataclass(frozen=True)
class SSOEvent(AppEvent):
    character_id: int = 0
    # corporation_id: int = 0
    # alliance_id: int = 0
    # is_permitted: bool = False
    # is_trusted: bool = False
    # is_contributor: bool = False


@dataclasses.dataclass(frozen=True)
class SSOLoginEvent(SSOEvent):
    session_id: str | None = None
    login_type: str | None = None


@dataclasses.dataclass(frozen=True)
class SSOTokenRefreshEvent(SSOEvent):
    session_id: str | None = None


@dataclasses.dataclass(frozen=True)
class SSOLogoutEvent(SSOEvent):
    session_id: str | None = None


@dataclasses.dataclass(frozen=True)
class AppAccessEvent(AppEvent):
    character_id: int = 0
    url: str | None = None
    permitted: bool | None = None


@dataclasses.dataclass(frozen=True)
class AppStructureEvent(AppEvent):
    structure_id: int = 0
    corporation_id: int = 0


@dataclasses.dataclass(frozen=True)
class MoonExtractionScheduledEvent(AppStructureEvent):
    moon_id: int = 0
    extraction_start_time: datetime.datetime | None = None
    chunk_arrival_time: datetime.datetime | None = None


@dataclasses.dataclass(frozen=True)
class MoonExtractionCompletedEvent(AppStructureEvent):
    moon_id: int = 0
    extraction_start_time: datetime.datetime | None = None
    chunk_arrival_time: datetime.datetime | None = None
    belt_decay_time: datetime.datetime | None = None


@dataclasses.dataclass(frozen=True)
class StructureStateChangedEvent(AppStructureEvent):
    system_id: int = 0
    exists: bool = False
    state: str | None = None
    state_timer_start: datetime.datetime | None = None
    state_timer_end: datetime.datetime | None = None
    fuel_expires: datetime.datetime | None = None
