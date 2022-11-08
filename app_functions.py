import datetime
from typing import Final

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
        timer_query: Final = (
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
        extraction_query: Final = (
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
        extraction_query: Final = (
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
        structure_query: Final = (
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
    async def get_usage(session: sqlalchemy.ext.asyncio.AsyncSession, now: datetime.datetime) -> list:
        """
        select
            esi_characters.name,
            count(app_access_history.timestamp) as count,
            min(app_access_history.timestamp) as first,
            max(app_access_history.timestamp) as last
        from app_access_history
        join esi_characters on app_access_history.character_id = esi_characters.character_id
        group by esi_characters.name
        order by last desc
        limit 25;
        """
        timer_query: Final = (
            sqlalchemy.select((EveTables.Character.name, sqlalchemy.func.count(EveTables.AccessHistory.timestamp).label("count"), sqlalchemy.func.max(EveTables.AccessHistory.timestamp).label("last")))
            .join(EveTables.Character, EveTables.AccessHistory.character_id == EveTables.Character.character_id)
            .group_by(EveTables.Character.name)
            .order_by(sqlalchemy.desc(sqlalchemy.func.max(EveTables.AccessHistory.timestamp)))
            .limit(25)
        )
        timer_query_result: Final[sqlalchemy.engine.Result] = await session.execute(timer_query)
        colnames: Final = ["name", "count", "last"]
        return [dict(zip(colnames, x)) for x in timer_query_result.all()]

    @staticmethod
    @otel
    async def is_permitted(evedb: EveDatabase, character_id: int, corpporation_id: int, alliance_id: int) -> bool:
        acl_pass = False

        acl_set: Final = set()
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
