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
    async def get_timers(db: sqlalchemy.ext.asyncio.AsyncSession, now: datetime.datetime) -> list:
        timer_query: Final = (
            sqlalchemy.select(
                (
                    EveTables.Structure,
                )
            )
            .where(
                EveTables.Structure.state_timer_end >= now,
            )
            .join(EveTables.Structure.system)
            .join(EveTables.Structure.corporation)
            .order_by(EveTables.Structure.state_timer_end)
            .options(sqlalchemy.orm.selectinload(EveTables.Structure.system))
            .options(sqlalchemy.orm.selectinload(EveTables.Structure.corporation))
        )
        timer_query_result = await db.execute(timer_query)
        return [result for result in timer_query_result.scalars()]

    @staticmethod
    @otel
    async def get_extractions(db: sqlalchemy.ext.asyncio.AsyncSession, now: datetime.datetime) -> list:
        extraction_query: Final = (
            sqlalchemy.select(EveTables.Extraction)
            .where(
                EveTables.Extraction.natural_decay_time >= now,
                EveTables.Extraction.extraction_start_time < now,
            )
            .order_by(EveTables.Extraction.chunk_arrival_time)
            .options(sqlalchemy.orm.selectinload(EveTables.Extraction.structure))
            .options(sqlalchemy.orm.selectinload(EveTables.Extraction.corporation))
            .options(sqlalchemy.orm.selectinload(EveTables.Extraction.moon))
        )

        extraction_query_result = await db.execute(extraction_query)
        return [
            result for result in extraction_query_result.scalars()
        ]

    @staticmethod
    @otel
    async def get_fuel_expiries(db: sqlalchemy.ext.asyncio.AsyncSession, now: datetime.datetime) -> list:
        structure_query: Final = (
            sqlalchemy.select(
                (
                    EveTables.Structure,
                )
            )
            .join(EveTables.Structure.system)
            .join(EveTables.Structure.corporation)
            .order_by(EveTables.Structure.fuel_expires)
            .options(sqlalchemy.orm.selectinload(EveTables.Structure.system))
            .options(sqlalchemy.orm.selectinload(EveTables.Structure.corporation))
        )
        structure_query_result = await db.execute(structure_query)
        return [result for result in structure_query_result.scalars()]

    @staticmethod
    @otel
    async def is_permitted(evedb: EveDatabase, character_id: int, corpporation_id: int, alliance_id: int) -> bool:
        acl_pass = False

        acl_set: Final = set()
        async with await evedb.sessionmaker() as db, db.begin():
            acl_query = sqlalchemy.select(EveTables.AccessControls)
            acl_query_result = await db.execute(acl_query)
            acl_set |= {result for result in acl_query_result.scalars()}

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
