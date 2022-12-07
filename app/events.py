import dataclasses
import datetime
import functools
import itertools

counter = itertools.count()


@dataclasses.dataclass(frozen=True)
class AppEvent:
    # id: int = dataclasses.field(default_factory=lambda: next(counter))
    ts: datetime.datetime = dataclasses.field(default_factory=functools.partial(datetime.datetime.now, tz=datetime.timezone.utc))


@dataclasses.dataclass(frozen=True)
class AppCharacterEvent:
    character_id: int = 0


@dataclasses.dataclass(frozen=True)
class SSOLoginEvent(AppCharacterEvent):
    session_id: str = None
    login_type: str = None


@dataclasses.dataclass(frozen=True)
class SSOTokenRefreshEvent(AppCharacterEvent):
    session_id: str = None


@dataclasses.dataclass(frozen=True)
class SSOLogoutEvent(AppCharacterEvent):
    session_id: str = None


@dataclasses.dataclass(frozen=True)
class AppStructureEvent(AppEvent):
    structure_id: int = 0
    corporation_id: int = 0


@dataclasses.dataclass(frozen=True)
class MoonExtractionScheduledEvent(AppStructureEvent):
    moon_id: int = 0
    extraction_start_time: datetime.datetime = None
    chunk_arrival_time: datetime.datetime = None


@dataclasses.dataclass(frozen=True)
class MoonExtractionCompletedEvent(AppStructureEvent):
    moon_id: int = 0
    extraction_start_time: datetime.datetime = None
    chunk_arrival_time: datetime.datetime = None
    belt_decay_time: datetime.datetime = None


@dataclasses.dataclass(frozen=True)
class StructureStateChangedEvent(AppStructureEvent):
    system_id: int = 0
    exists: bool = False
    state: str = None
    state_timer_start: datetime.datetime = None
    state_timer_end: datetime.datetime = None
    fuel_expires: datetime.datetime = None
