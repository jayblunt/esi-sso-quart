import datetime
import typing

import sqlalchemy
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql

from db import EveAccessType, EveDatabase, EveTables
from telemetry import otel


class AppFunctions:


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
    async def get_usage(session: sqlalchemy.ext.asyncio.AsyncSession, permitted: bool, now: datetime.datetime) -> list:

        permitted_condition: typing.Final = sqlalchemy.sql.expression.true() if permitted else sqlalchemy.sql.expression.false()

        timer_query: typing.Final = (
            sqlalchemy.select((EveTables.Character.character_id, sqlalchemy.func.count(EveTables.AccessHistory.timestamp).label("count"), sqlalchemy.func.max(EveTables.AccessHistory.timestamp).label("last")))
            .join(EveTables.Character, EveTables.AccessHistory.character_id == EveTables.Character.character_id)
            .where(EveTables.AccessHistory.permitted == permitted_condition)
            .group_by(EveTables.Character.character_id)
            .order_by(sqlalchemy.desc(sqlalchemy.func.max(EveTables.AccessHistory.timestamp)))
            .limit(25)
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
    async def is_permitted(evedb: EveDatabase, character_id: int, corpporation_id: int, alliance_id: int) -> bool:
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

        return acl_pass
