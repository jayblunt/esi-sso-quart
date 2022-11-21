import dataclasses
import datetime
import typing

import quart
import quart.sessions
import sqlalchemy
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql

from db import EveAccessType, EveDatabase, EveTables
from sso import EveSSO
from telemetry import otel


@dataclasses.dataclass(frozen=True)
class AppRequest:
    session: quart.sessions.SessionMixin = None
    character_id: int = 0
    corpporation_id: int = 0
    alliance_id: int = 0
    permitted: bool = False
    trusted: bool = False
    contributor: bool = False
    ts: datetime.datetime = datetime.datetime.now(tz=datetime.timezone.utc)


class AppFunctions:

    @staticmethod
    @otel
    async def get_app_request(evedb: EveDatabase, session: quart.sessions.SessionMixin, request: quart.Request) -> AppRequest:

        character_id: typing.Final = session.get(EveSSO.ESI_CHARACTER_ID, 0)
        corpporation_id: typing.Final = session.get(EveSSO.ESI_CORPORATION_ID, 0)
        alliance_id: typing.Final = session.get(EveSSO.ESI_ALLIANCE_ID, 0)

        permitted: typing.Final = await AppFunctions.is_permitted(evedb, character_id, corpporation_id, alliance_id)
        trusted: typing.Final = await AppFunctions.is_trusted(evedb, character_id, corpporation_id, alliance_id)
        contributor: typing.Final = await AppFunctions.is_contributor(evedb, character_id, corpporation_id, alliance_id)

        ar = AppRequest(session=session,
                        character_id=character_id,
                        corpporation_id=corpporation_id,
                        alliance_id=alliance_id,
                        permitted=permitted,
                        trusted=trusted,
                        contributor=contributor)

        if ar.character_id > 0 and ar.character_id not in [92923556]:
            try:
                async with await evedb.sessionmaker() as session, session.begin():
                    session.add(EveTables.AccessHistory(character_id=ar.character_id, permitted=bool(ar.permitted), path=request.path))
                    await session.commit()
            except Exception:
                pass

        return ar

    @staticmethod
    @otel
    async def get_active_timers(session: sqlalchemy.ext.asyncio.AsyncSession, now: datetime.datetime) -> list:
        timer_query: typing.Final = (
            sqlalchemy.select(EveTables.Structure)
            .where(
                EveTables.Structure.state_timer_end >= now,
            )
            .join(EveTables.Structure.system)
            .join(EveTables.Structure.corporation)
            .order_by(EveTables.Structure.state_timer_end)
            .options(sqlalchemy.orm.selectinload(EveTables.Structure.system))
            .options(sqlalchemy.orm.selectinload(EveTables.Structure.corporation))
        )
        timer_query_result = await session.execute(timer_query)
        return [x for x in timer_query_result.scalars()]

    @staticmethod
    @otel
    async def get_completed_extractions(session: sqlalchemy.ext.asyncio.AsyncSession, now: datetime.datetime) -> list:
        extraction_query: typing.Final = (
            sqlalchemy.select(EveTables.CompletedExtraction)
            .where(
                EveTables.CompletedExtraction.belt_decay_time >= now,
                EveTables.CompletedExtraction.chunk_arrival_time <= now,
            )
            .order_by(EveTables.CompletedExtraction.chunk_arrival_time)
            .options(sqlalchemy.orm.selectinload(EveTables.CompletedExtraction.structure))
            .options(sqlalchemy.orm.selectinload(EveTables.CompletedExtraction.corporation))
            .options(sqlalchemy.orm.selectinload(EveTables.CompletedExtraction.moon))
        )

        extraction_query_result = await session.execute(extraction_query)
        return [x for x in extraction_query_result.scalars()]

    @staticmethod
    @otel
    async def get_scheduled_extractions(session: sqlalchemy.ext.asyncio.AsyncSession, now: datetime.datetime) -> list:
        extraction_query: typing.Final = (
            sqlalchemy.select(EveTables.ScheduledExtraction)
            .where(
                EveTables.ScheduledExtraction.chunk_arrival_time >= now,
                EveTables.ScheduledExtraction.extraction_start_time <= now,
            )
            .order_by(EveTables.ScheduledExtraction.chunk_arrival_time)
            .options(sqlalchemy.orm.selectinload(EveTables.ScheduledExtraction.structure))
            .options(sqlalchemy.orm.selectinload(EveTables.ScheduledExtraction.corporation))
            .options(sqlalchemy.orm.selectinload(EveTables.ScheduledExtraction.moon))
        )

        extraction_query_result = await session.execute(extraction_query)
        return [x for x in extraction_query_result.scalars()]

    @staticmethod
    @otel
    async def get_structure_fuel_expiries(session: sqlalchemy.ext.asyncio.AsyncSession, now: datetime.datetime) -> list:
        structure_query: typing.Final = (
            sqlalchemy.select(EveTables.Structure)
            .where(
                EveTables.Structure.fuel_expires != sqlalchemy.sql.expression.null(),
                EveTables.Structure.fuel_expires > now,
            )
            .join(EveTables.Structure.system)
            .join(EveTables.Structure.corporation)
            .order_by(EveTables.Structure.fuel_expires)
            .options(sqlalchemy.orm.selectinload(EveTables.Structure.system))
            .options(sqlalchemy.orm.selectinload(EveTables.Structure.corporation))
        )
        structure_query_result = await session.execute(structure_query)
        return [x for x in structure_query_result.scalars()]

    @staticmethod
    @otel
    async def get_moon_yield(session: sqlalchemy.ext.asyncio.AsyncSession, moon_id: int, now: datetime.datetime) -> list:
        query: typing.Final = (
            sqlalchemy.select(EveTables.MoonYield)
            .where(EveTables.MoonYield.moon_id == moon_id)
            .order_by(sqlalchemy.desc(EveTables.MoonYield.yield_percent))
        )

        result = await session.execute(query)
        return [x for x in result.scalars() if x is not None]

    @staticmethod
    @otel
    async def get_moon_history(session: sqlalchemy.ext.asyncio.AsyncSession, moon_id: int, now: datetime.datetime) -> list:
        query: typing.Final = (
            sqlalchemy.select(EveTables.ExtractionHistory)
            .where(
                sqlalchemy.and_(
                    EveTables.ExtractionHistory.exists == sqlalchemy.sql.expression.true(),
                    EveTables.ExtractionHistory.moon_id == moon_id,
                    EveTables.ExtractionHistory.chunk_arrival_time < now,
                )
            )
            .order_by(sqlalchemy.desc(EveTables.ExtractionHistory.chunk_arrival_time))
            # .options(sqlalchemy.orm.selectinload(EveTables.ExtractionHistory.corporation))
            # .options(sqlalchemy.orm.selectinload(EveTables.ExtractionHistory.moon))
            .limit(12)
        )

        result = await session.execute(query)
        results = list()
        for x in result.scalars():
            x: EveTables.ExtractionHistory
            cat: datetime.datetime = x.chunk_arrival_time
            results.append({
                'extraction': x,
                'dow': cat.isoweekday() - 1,
                'tod': cat.hour // 3,
            })
        return results

    @staticmethod
    @otel
    async def get_usage(session: sqlalchemy.ext.asyncio.AsyncSession, permitted: bool, now: datetime.datetime) -> list:

        permitted_condition: typing.Final = sqlalchemy.sql.expression.true() if permitted else sqlalchemy.sql.expression.false()

        timer_query: typing.Final = (
            sqlalchemy.select((EveTables.Character.character_id, sqlalchemy.func.count(EveTables.AccessHistory.timestamp).label("count"), sqlalchemy.func.max(EveTables.AccessHistory.timestamp).label("last")))
            .join(EveTables.Character, EveTables.AccessHistory.character_id == EveTables.Character.character_id)
            .where(EveTables.AccessHistory.permitted == permitted_condition)
            .group_by(EveTables.Character.character_id)
            .order_by(sqlalchemy.desc(sqlalchemy.func.max(EveTables.AccessHistory.timestamp)))
            .limit(20)
        )
        timer_query_result: typing.Final[sqlalchemy.engine.Result] = await session.execute(timer_query)
        colnames: typing.Final = ["id", "count", "last"]
        return [dict(zip(colnames, x)) for x in timer_query_result.all()]

    @staticmethod
    @otel
    async def get_character_name(evedb: EveDatabase, character_id: int) -> str | None:
        async with await evedb.sessionmaker() as session:
            query = (
                sqlalchemy.select(EveTables.Character.name)
                .where(EveTables.Character.character_id == character_id)
                .limit(1)
            )
            result: sqlalchemy.engine.Result = await session.execute(query)
            return result.scalar_one_or_none()

    @staticmethod
    @otel
    async def get_mmon_name(evedb: EveDatabase, moon_id: int) -> str | None:
        async with await evedb.sessionmaker() as session:
            query = (
                sqlalchemy.select(EveTables.UniverseMoon.name)
                .where(EveTables.UniverseMoon.moon_id == moon_id)
                .limit(1)
            )
            result: sqlalchemy.engine.Result = await session.execute(query)
            return result.scalar_one_or_none()

    @staticmethod
    @otel
    async def get_type_name(evedb: EveDatabase, type_id: int) -> str | None:
        async with await evedb.sessionmaker() as session:
            query = (
                sqlalchemy.select(EveTables.UniverseType.name)
                .where(EveTables.UniverseType.type_id == type_id)
                .limit(1)
            )
            result: sqlalchemy.engine.Result = await session.execute(query)
            return result.scalar_one_or_none()

    @staticmethod
    @otel
    async def is_permitted(evedb: EveDatabase, character_id: int, corpporation_id: int, alliance_id: int, check_trust: bool = False) -> bool:
        acl_pass = False

        acl_set: typing.Final = set()
        async with await evedb.sessionmaker() as session:

            acl_query = sqlalchemy.select(EveTables.AccessControls)
            acl_query_result = await session.execute(acl_query)
            acl_set |= {x for x in acl_query_result.scalars()}

        acl_pass = False
        acl_evaluations = [
            (EveAccessType.ALLIANCE, alliance_id),
            (EveAccessType.CORPORATION, corpporation_id),
            (EveAccessType.CHARACTER, character_id),
        ]

        for acl_type, acl_id in acl_evaluations:
            for acl in filter(lambda x: x.type == acl_type, acl_set):
                if not isinstance(acl, EveTables.AccessControls):
                    continue
                if acl_id == acl.id:
                    acl_pass = acl.permit
                    if check_trust:
                        acl_pass = acl_pass and acl.trust

        return acl_pass

    @staticmethod
    @otel
    async def is_contributor(evedb: EveDatabase, character_id: int, corpporation_id: int, alliance_id: int) -> bool:
        if character_id in [92923556]:
            return True

        is_permitted = await AppFunctions.is_permitted(evedb, character_id, corpporation_id, alliance_id, check_trust=False)
        if is_permitted:
            async with await evedb.sessionmaker() as session:
                query = (
                    sqlalchemy.select(EveTables.PeriodicCredentials)
                    .where(
                        sqlalchemy.and_(
                            EveTables.PeriodicCredentials.is_enabled == sqlalchemy.sql.expression.true(),
                            EveTables.PeriodicCredentials.character_id == character_id,
                        )
                    )
                )
                result: sqlalchemy.engine.Result = await session.execute(query)
                obj = result.scalar_one_or_none()
                if obj is not None:
                    return True
        return False

    @staticmethod
    @otel
    async def is_trusted(evedb: EveDatabase, character_id: int, corpporation_id: int, alliance_id: int) -> bool:
        is_permitted = await AppFunctions.is_permitted(evedb, character_id, corpporation_id, alliance_id, check_trust=False)
        is_trusted = await AppFunctions.is_permitted(evedb, character_id, corpporation_id, alliance_id, check_trust=True)
        if is_permitted and not is_trusted:
            async with await evedb.sessionmaker() as session:
                query = (
                    sqlalchemy.select(EveTables.PeriodicCredentials)
                    .where(
                        sqlalchemy.and_(
                            EveTables.PeriodicCredentials.is_enabled == sqlalchemy.sql.expression.true(),
                            EveTables.PeriodicCredentials.character_id == character_id,
                        )
                    )
                )
                result: sqlalchemy.engine.Result = await session.execute(query)
                obj = result.scalar_one_or_none()
                if obj is not None:
                    is_trusted = True
        return is_trusted
