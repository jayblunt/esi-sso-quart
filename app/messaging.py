import dataclasses
import datetime
import functools
import typing


@dataclasses.dataclass(frozen=True)
class ApplicationMessage:
    ts: datetime.datetime = dataclasses.field(default_factory=functools.partial(datetime.datetime.now, tz=datetime.timezone.utc))


@dataclasses.dataclass(frozen=True)
class MoonExtractionScheduled(ApplicationMessage):
    structure_id: int = 0
    corporation_id: int = 0
    moon_id: int = 0
    extraction_start_time: datetime.datetime = dataclasses.field(default_factory=functools.partial(datetime.datetime, 1970, 1, 1, 0, 0, 0, tz=datetime.timezone.utc))
    chunk_arrival_time: datetime.datetime = dataclasses.field(default_factory=functools.partial(datetime.datetime, 1970, 1, 1, 0, 0, 0, tz=datetime.timezone.utc))


@dataclasses.dataclass(frozen=True)
class MoonExtractionArrived(ApplicationMessage):
    structure_id: int = 0
    corporation_id: int = 0
    moon_id: int = 0
    extraction_start_time: datetime.datetime = dataclasses.field(default_factory=functools.partial(datetime.datetime, 1970, 1, 1, 0, 0, 0, tz=datetime.timezone.utc))
    chunk_arrival_time: datetime.datetime = dataclasses.field(default_factory=functools.partial(datetime.datetime, 1970, 1, 1, 0, 0, 0, tz=datetime.timezone.utc))
    belt_decay_time: datetime.datetime = dataclasses.field(default_factory=functools.partial(datetime.datetime, 1970, 1, 1, 0, 0, 0, tz=datetime.timezone.utc))
