import asyncio
import collections
import collections.abc
import datetime
import inspect
import logging
import random
import typing

import dateutil.parser
import opentelemetry.trace
import sqlalchemy
import sqlalchemy.engine
import sqlalchemy.exc
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql

from db import EveDatabase, EveTables
from sso import EveSSO
from telemetry import otel, otel_add_exception

from .task import EveDatabaseTask


class EveCommonState:

    def __init__(self, db: EveDatabase, logger: logging.Logger = logging.getLogger()) -> None:
        self.db: typing.Final = db
        self.logger: typing.Final = logger
        self.name: typing.Final = self.__class__.__name__

    async def check_changes(self, structure_id: int, prior_exists: bool, now_exists: bool, prior_edict: dict, now_edict: dict) -> dict:
        result = None
        if not now_exists:
            if prior_exists:
                result = prior_edict | {'exists': now_exists}
        elif now_edict:
            for k, v in prior_edict.items():
                if k in ['character_id', 'corporation_id']:
                    continue
                if v != now_edict.get(k):
                    self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {structure_id} {k} changed from {v} to {now_edict.get(k)}")
                    result = now_edict | {'exists': now_exists}
                    break
        return result


class EveStructureState(EveCommonState):

    async def structure_ids(self, corporation_id: int) -> set[int]:
        corporation_structure_ids = set()
        try:
            async with await self.db.sessionmaker() as session:

                # Collect the exists=True structures
                query = (
                    sqlalchemy.select(sqlalchemy.distinct(EveTables.StructureHistory.structure_id))
                    .where(
                        sqlalchemy.and_(
                            EveTables.StructureHistory.corporation_id == corporation_id,
                            EveTables.StructureHistory.exists == sqlalchemy.sql.expression.true(),
                            EveTables.StructureHistory.structure_id.not_in(
                                sqlalchemy.select(sqlalchemy.distinct(EveTables.StructureHistory.structure_id))
                                .where(
                                    sqlalchemy.and_(
                                        EveTables.StructureHistory.corporation_id == corporation_id,
                                        EveTables.StructureHistory.exists == sqlalchemy.sql.expression.false()
                                    )
                                )
                            )
                        )
                    )
                )
                result: sqlalchemy.engine.Result = await session.execute(query)
                for x in result.scalars():
                    corporation_structure_ids.add(x)

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        return corporation_structure_ids

    async def get(self, structure_id: int) -> EveTables.StructureHistory | None:
        structure_obj = None
        try:
            async with await self.db.sessionmaker() as session:
                query = (
                    sqlalchemy.select(EveTables.StructureHistory)
                    .where(
                        EveTables.StructureHistory.structure_id == structure_id
                    )
                    .order_by(sqlalchemy.desc(EveTables.StructureHistory.id))
                    .limit(1)
                )
                result: sqlalchemy.engine.Result = await session.execute(query)
                row: sqlalchemy.engine.Row = result.fetchone()
                if row:
                    structure_obj: EveTables.StructureHistory = row[0]

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        return structure_obj

    async def set(self, structure_id: int, now_edict: dict, now_exists: bool, now_timestamp: datetime.datetime | None = None) -> None:

        try:
            structure_changed = now_exists and now_edict
            structure_obj = await self.get(structure_id)
            if not structure_obj:
                self.logger.warning(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {structure_id} is {structure_obj}")

            if structure_obj:
                prior_edict: typing.Final = {x: getattr(structure_obj, x) for x in structure_obj.__table__.columns.keys() if x not in ['id', 'exists', 'timestamp']}
                change_edict = await self.check_changes(structure_id, structure_obj.exists, now_exists, prior_edict, now_edict)
                structure_changed = change_edict is not None
                if structure_changed:
                    now_edict = change_edict

            if structure_changed and now_edict:
                now_edict |= {'exists': now_exists}
                if now_timestamp is not None:
                    now_edict |= {'timestamp': now_timestamp}
                structure_obj = EveTables.StructureHistory(**now_edict)
                async with await self.db.sessionmaker() as session, session.begin():
                    session.add(structure_obj)
                    await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")


class EveExtractionState(EveCommonState):

    async def structure_ids(self, corporation_id: int) -> set[int]:
        corporation_structure_ids = set()
        try:
            async with await self.db.sessionmaker() as session:

                # Collect the exists=True structures
                query = (
                    sqlalchemy.select(sqlalchemy.distinct(EveTables.ExtractionHistory.structure_id))
                    .where(
                        sqlalchemy.and_(
                            EveTables.ExtractionHistory.corporation_id == corporation_id,
                            EveTables.ExtractionHistory.exists == sqlalchemy.sql.expression.true(),
                            EveTables.ExtractionHistory.structure_id.not_in(
                                sqlalchemy.select(sqlalchemy.distinct(EveTables.ExtractionHistory.structure_id))
                                .where(
                                    sqlalchemy.and_(
                                        EveTables.ExtractionHistory.corporation_id == corporation_id,
                                        EveTables.ExtractionHistory.exists == sqlalchemy.sql.expression.false()
                                    )
                                )
                            )
                        )
                    )
                )
                result: sqlalchemy.engine.Result = await session.execute(query)
                for x in result.scalars():
                    corporation_structure_ids.add(x)

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        return corporation_structure_ids

    async def get(self, structure_id: int) -> EveTables.ExtractionHistory | None:
        structure_obj = None
        try:
            async with await self.db.sessionmaker() as session:
                query = (
                    sqlalchemy.select(EveTables.ExtractionHistory)
                    .where(
                        EveTables.ExtractionHistory.structure_id == structure_id
                    )
                    .order_by(sqlalchemy.desc(EveTables.ExtractionHistory.id))
                    .limit(1)
                )
                result: sqlalchemy.engine.Result = await session.execute(query)
                row: sqlalchemy.engine.Row = result.fetchone()
                if row:
                    structure_obj: EveTables.ExtractionHistory = row[0]

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        return structure_obj

    async def set(self, structure_id: int, now_edict: dict, now_exists: bool, now_timestamp: datetime.datetime | None = None) -> None:

        try:
            structure_changed = now_exists and now_edict
            structure_obj = await self.get(structure_id)
            if not structure_obj:
                self.logger.warning(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {structure_id} is {structure_obj}")

            if structure_obj:
                prior_edict: typing.Final = {x: getattr(structure_obj, x) for x in structure_obj.__table__.columns.keys() if x not in ['id', 'exists', 'timestamp']}
                change_edict = await self.check_changes(structure_id, structure_obj.exists, now_exists, prior_edict, now_edict)
                structure_changed = change_edict is not None
                if structure_changed:
                    now_edict = change_edict

            if structure_changed and now_edict:
                now_edict |= {'exists': now_exists}
                if now_timestamp is not None:
                    now_edict |= {'timestamp': now_timestamp}
                structure_obj = EveTables.ExtractionHistory(**now_edict)
                async with await self.db.sessionmaker() as session, session.begin():
                    session.add(structure_obj)
                    await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")


class EveStructureTask(EveDatabaseTask):

    @otel
    def __init__(self, client_session: collections.abc.MutableMapping, db: EveDatabase, logger: logging.Logger = logging.getLogger()):
        super().__init__(client_session, db, logger)
        self.structure_state: typing.Final = EveStructureState(db, logger)
        self.extraction_state: typing.Final = EveExtractionState(db, logger)

    @otel
    async def run_structures(self, now: datetime.datetime, character_id: int, corporation_id: int, access_token: str) -> None:

        prior_structure_id_set: typing.Final = await self.structure_state.structure_ids(corporation_id)

        url = f"https://esi.evetech.net/latest/corporations/{corporation_id}/structures/"
        structures: typing.Final = await self.get_pages(url, access_token)

        if structures is None:
            self.logger.warning(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: corporation_id:{corporation_id} structures:{structures}")
            return

        if len(structures) > 0 and len(structures) != len(list(filter(None, structures))):
            self.logger.warning(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: corporation_id:{corporation_id} structures:{structures}")
            return

        # Add the structures to the query log
        try:
            async with await self.db.sessionmaker() as session, session.begin():
                session.add(EveTables.StructurQueryLog(corporation_id=corporation_id, character_id=character_id, json=structures))
                await session.commit()
        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        structure_obj_dict: typing.Final = dict()
        structure_type_id_set: typing.Final = set()
        structure_corporation_id_set: typing.Final = set({corporation_id})

        for x in structures:
            edict: typing.Final = dict({
                "character_id": character_id,
            })
            for k, v in x.items():
                if k in ["fuel_expires", "state_timer_end", "state_timer_start", "unanchors_at"]:
                    # v = dateutil.parser.parse(v).replace(tzinfo=zoneinfo.ZoneInfo("UTC"))
                    v = dateutil.parser.parse(v).replace(tzinfo=datetime.timezone.utc)
                elif k in ["corporation_id", "structure_id", "system_id", "type_id"]:
                    v = int(v)
                elif k in ["services"]:
                    v = list(filter(lambda x: x.get('name', '') == "Moon Drilling", v))
                    edict["has_moon_drill"] = bool(len(v) > 0)
                    continue
                elif k not in ["name", "state"]:
                    continue
                edict[k] = v

            obj = EveTables.Structure(**edict)

            structure_obj_dict[obj.structure_id] = obj
            structure_type_id_set.add(obj.type_id)
            structure_corporation_id_set.add(obj.corporation_id)

            await self.structure_state.set(obj.structure_id, edict, now_exists=True)

        # Remove deleted
        for structure_id in set(prior_structure_id_set) - set(structure_obj_dict.keys()):
            await self.structure_state.set(structure_id, None, now_exists=False)

        # Stop here if there are no structures
        if not len(structure_obj_dict) > 0:
            return

        await self.backfill_types(structure_type_id_set)
        await self.backfill_corporations(structure_corporation_id_set)

        # Ugly hack to remove structures that no longer exist. If I was better with the ORM I would not have to do this ..
        try:
            want_commit = False
            async with await self.db.sessionmaker() as session, session.begin():

                completed_extraction_query: typing.Final = (
                    sqlalchemy.select(EveTables.CompletedExtraction)
                    .where(
                        sqlalchemy.and_(
                            EveTables.CompletedExtraction.corporation_id == corporation_id,
                            EveTables.CompletedExtraction.structure_id.not_in(structure_obj_dict.keys())
                        )
                    )
                    .options(sqlalchemy.orm.selectinload(EveTables.CompletedExtraction.structure))
                    .options(sqlalchemy.orm.selectinload(EveTables.CompletedExtraction.corporation))
                    .options(sqlalchemy.orm.selectinload(EveTables.CompletedExtraction.moon))
                )
                completed_extraction_query_result: typing.Final[sqlalchemy.engine.Result] = await session.execute(completed_extraction_query)
                completed_extraction_obj_list: typing.Final[list[EveTables.CompletedExtraction]] = [x for x in completed_extraction_query_result.scalars()]

                if len(completed_extraction_obj_list) > 0:
                    # self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: completed_extraction_obj_list: {sorted(list(map(lambda x: x.structure_id, completed_extraction_obj_list)))}")
                    [await session.delete(x) for x in completed_extraction_obj_list]
                    want_commit = True


                scheduled_extraction_query: typing.Final = (
                    sqlalchemy.select(EveTables.ScheduledExtraction)
                    .where(
                        sqlalchemy.and_(
                            EveTables.ScheduledExtraction.corporation_id == corporation_id,
                            EveTables.ScheduledExtraction.structure_id.not_in(structure_obj_dict.keys())
                        )
                    )
                    .options(sqlalchemy.orm.selectinload(EveTables.ScheduledExtraction.structure))
                    .options(sqlalchemy.orm.selectinload(EveTables.ScheduledExtraction.corporation))
                    .options(sqlalchemy.orm.selectinload(EveTables.ScheduledExtraction.moon))
                )
                scheduled_extraction_query_result: typing.Final[sqlalchemy.engine.Result] = await session.execute(scheduled_extraction_query)
                scheduled_extraction_obj_list: typing.Final[list[EveTables.ScheduledExtraction]] = [x for x in scheduled_extraction_query_result.scalars()]

                if len(scheduled_extraction_obj_list) > 0:
                    # self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: scheduled_extraction_obj_list: {sorted(list(map(lambda x: x.structure_id, scheduled_extraction_obj_list)))}")
                    [await session.delete(x) for x in scheduled_extraction_obj_list]
                    want_commit = True

                if want_commit:
                    await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")


        try:
            async with await self.db.sessionmaker() as session, session.begin():
                session: sqlalchemy.ext.asyncio.AsyncSession
                obj_update_set = set()
                obj_delete_set = set()
                obj_add_set = set()

                existing_structure_obj_dict: typing.Final[dict[int, EveTables.Structure]] = dict()
                query = (
                    sqlalchemy.select(EveTables.Structure)
                    .where(EveTables.Structure.corporation_id == corporation_id)
                    .options(sqlalchemy.orm.selectinload(EveTables.Structure.system))
                    .options(sqlalchemy.orm.selectinload(EveTables.Structure.corporation))
                )
                query_result: sqlalchemy.engine.Result = await session.execute(query)
                for obj in query_result.scalars():
                    obj: EveTables.Structure
                    existing_structure_obj_dict[obj.structure_id] = obj

                for structure_id, obj in structure_obj_dict.items():
                    existing_obj = existing_structure_obj_dict.get(structure_id, None)

                    if not existing_obj:
                        self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: add({obj})")
                        obj_add_set.add(obj)
                        continue

                    same_attributes = True
                    for attribute in [x for x in obj.__table__.columns.keys() if x not in ['timestamp']]:
                        if getattr(obj, attribute) != getattr(existing_obj, attribute):
                            self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {structure_id} {attribute} {getattr(existing_obj, attribute)} -> {getattr(obj, attribute)}")
                            setattr(existing_obj, attribute, getattr(obj, attribute))
                            same_attributes = False
                    if not same_attributes:
                        existing_obj.timestamp = now
                        obj_update_set.add(existing_obj)

                for structure_id, existing_obj in existing_structure_obj_dict.items():
                    if structure_obj_dict.get(structure_id) is None:
                        self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: delete({existing_obj})")
                        obj_delete_set.add(existing_obj)

                if any([len(obj_add_set) > 0, len(obj_delete_set) > 0, len(obj_update_set) > 0]):
                    if len(obj_delete_set) > 0:
                        [await session.delete(obj) for obj in obj_delete_set]
                    if len(obj_add_set) > 0:
                        session.add_all(obj_add_set)
                    await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")
            
    @otel
    async def roll_extractions(self, now: datetime.datetime, character_id: int, corporation_id: int, access_token: str) -> None:

        # Migrated scheduled extractions to completed extractions
        try:
            async with await self.db.sessionmaker() as session, session.begin():
                session: sqlalchemy.ext.asyncio.AsyncSession

                query = (
                    sqlalchemy.select(EveTables.CompletedExtraction)
                    .where(EveTables.CompletedExtraction.corporation_id == corporation_id)
                    .options(sqlalchemy.orm.selectinload(EveTables.CompletedExtraction.structure))
                    .options(sqlalchemy.orm.selectinload(EveTables.CompletedExtraction.corporation))
                    .options(sqlalchemy.orm.selectinload(EveTables.CompletedExtraction.moon))
                )
                query_result: sqlalchemy.engine.Result = await session.execute(query)
                completed_extraction_dict = {x.structure_id: x for x in query_result.scalars()}

                query = (
                    sqlalchemy.select(EveTables.ScheduledExtraction)
                    .where(EveTables.ScheduledExtraction.corporation_id == corporation_id)
                    .options(sqlalchemy.orm.selectinload(EveTables.ScheduledExtraction.structure))
                    .options(sqlalchemy.orm.selectinload(EveTables.ScheduledExtraction.corporation))
                    .options(sqlalchemy.orm.selectinload(EveTables.ScheduledExtraction.moon))
                )
                query_result: sqlalchemy.engine.Result = await session.execute(query)
                scheduled_extraction_dict = {x.structure_id: x for x in query_result.scalars()}

                migrated_structure_id_set = set()
                for structure_id, scheduled_obj in scheduled_extraction_dict.items():
                    scheduled_obj: EveTables.ScheduledExtraction

                    if now <= scheduled_obj.chunk_arrival_time:
                        continue

                    belt_lifetime_estimate: typing.Final = datetime.timedelta(days=2)
                    query = (
                        sqlalchemy.select(EveTables.StructureModifiers)
                        .where(EveTables.StructureModifiers.structure_id == structure_id)
                        .limit(1)
                    )
                    query_result: sqlalchemy.engine.Result = await session.execute(query)
                    obj = query_result.scalar_one_or_none()
                    if obj and isinstance(obj, EveTables.StructureModifiers):
                        belt_lifetime_estimate *= float(obj.belt_lifetime_modifier)

                    completed_obj: EveTables.CompletedExtraction = completed_extraction_dict.get(structure_id, None)
                    if completed_obj:
                        self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: delete({completed_obj})")
                        await session.delete(completed_obj)

                    new_completed_obj = EveTables.CompletedExtraction(
                        character_id=scheduled_obj.character_id,
                        corporation_id=scheduled_obj.corporation_id,
                        structure_id=scheduled_obj.structure_id,
                        moon_id=scheduled_obj.moon_id,
                        extraction_start_time=scheduled_obj.extraction_start_time,
                        chunk_arrival_time=scheduled_obj.chunk_arrival_time,
                        natural_decay_time=scheduled_obj.natural_decay_time,
                        belt_decay_time=scheduled_obj.chunk_arrival_time + belt_lifetime_estimate
                    )
                    self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: add({new_completed_obj})")
                    session.add(new_completed_obj)

                    self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {scheduled_obj} -> {new_completed_obj}")

                    migrated_structure_id_set.add(scheduled_obj.structure_id)

                if len(migrated_structure_id_set) > 0:
                    for structure_id in migrated_structure_id_set:
                        obj: EveTables.ScheduledExtraction = scheduled_extraction_dict.get(structure_id, None)
                        if obj:
                            self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: delete({obj})")
                            await session.delete(obj)

                await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

    @otel
    async def run_extractions(self, now: datetime.datetime, character_id: int, corporation_id: int, access_token: str) -> None:

        url = f"https://esi.evetech.net/latest/corporation/{corporation_id}/mining/extractions/"
        extractions: typing.Final = await self.get_pages(url, access_token)

        if extractions is None:
            self.logger.warning(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: corporation_id:{corporation_id} structures:{extractions}")
            return

        if len(extractions) > 0 and len(extractions) != len(list(filter(None, extractions))):
            self.logger.warning(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: corporation_id:{corporation_id} structures:{extractions}")
            return

        # Add the extractions to the query log
        try:
            async with await self.db.sessionmaker() as session, session.begin():
                session.add(EveTables.ExtractionQueryLog(corporation_id=corporation_id, character_id=character_id, json=extractions))
                await session.commit()
        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")


        extractions_obj_dict: typing.Final = dict()

        if len(extractions) > 0:

            extraction_moon_id_set: typing.Final = set()
            extraction_corporation_id_set: typing.Final = set()

            for x in extractions:
                edict = dict({
                    "character_id": character_id,
                    "corporation_id": corporation_id,
                })
                for k, v in x.items():
                    if k in ["chunk_arrival_time", "extraction_start_time", "natural_decay_time"]:
                        # v = dateutil.parser.parse(v).replace(tzinfo=zoneinfo.ZoneInfo("UTC"))
                        v = dateutil.parser.parse(v).replace(tzinfo=datetime.timezone.utc)
                    elif k in ["structure_id", "moon_id"]:
                        v = int(v)
                    else:
                        continue
                    edict[k] = v

                obj = EveTables.ScheduledExtraction(**edict)
                extractions_obj_dict[obj.structure_id] = obj
                extraction_moon_id_set.add(obj.moon_id)
                extraction_corporation_id_set.add(obj.corporation_id)

                await self.extraction_state.set(obj.structure_id, edict, now_exists=True)

            await self.backfill_moons(extraction_moon_id_set)
            await self.backfill_corporations(extraction_corporation_id_set)




        try:
            async with await self.db.sessionmaker() as session, session.begin():
                session: sqlalchemy.ext.asyncio.AsyncSession

                obj_update_set = set()
                obj_delete_set = set()
                obj_add_set = set()

                scheduled_extraction_dict: typing.Final[dict[int, EveTables.ScheduledExtraction]] = dict()
                query = (
                    sqlalchemy.select(EveTables.ScheduledExtraction)
                    .where(EveTables.ScheduledExtraction.corporation_id == corporation_id)
                    .options(sqlalchemy.orm.selectinload(EveTables.ScheduledExtraction.structure))
                    .options(sqlalchemy.orm.selectinload(EveTables.ScheduledExtraction.corporation))
                    .options(sqlalchemy.orm.selectinload(EveTables.ScheduledExtraction.moon))
                )
                query_result: sqlalchemy.engine.Result = await session.execute(query)
                for obj in query_result.scalars():
                    obj: EveTables.ScheduledExtraction
                    scheduled_extraction_dict[obj.structure_id] = obj

                for structure_id, obj in extractions_obj_dict.items():
                    existing_obj = scheduled_extraction_dict.get(structure_id, None)

                    if not existing_obj:
                        self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: add({obj})")
                        obj_add_set.add(obj)
                        continue

                    same_attributes = True
                    for attribute in [x for x in obj.__table__.columns.keys() if x not in ['timestamp']]:
                        if getattr(obj, attribute) != getattr(existing_obj, attribute):
                            self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {structure_id} {attribute} {getattr(existing_obj, attribute)} -> {getattr(obj, attribute)}")
                            setattr(existing_obj, attribute, getattr(obj, attribute))
                            same_attributes = False
                    if not same_attributes:
                        existing_obj.timestamp = now
                        obj_update_set.add(existing_obj)

                for structure_id, existing_obj in scheduled_extraction_dict.items():
                    if extractions_obj_dict.get(structure_id) is None:
                        self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: delete({existing_obj})")
                        obj_delete_set.add(existing_obj)

                if any([len(obj_add_set) > 0, len(obj_delete_set) > 0, len(obj_update_set) > 0]):
                    if len(obj_delete_set) > 0:
                        [await session.delete(obj) for obj in obj_delete_set]
                        await session.flush()
                    if len(obj_add_set) > 0:
                        session.add_all(obj_add_set)
                    await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")


    @otel
    async def run(self, client_session: collections.abc.MutableMapping) -> None:
        if not client_session.get(EveSSO.ESI_CHARACTER_HAS_STATION_MANAGER_ROLE, False):
            return

        character_id: typing.Final = int(client_session.get(EveSSO.ESI_CHARACTER_ID, 0))
        corporation_id: typing.Final = int(client_session.get(EveSSO.ESI_CORPORATION_ID, 0))
        access_token: typing.Final = client_session.get(EveSSO.ESI_ACCESS_TOKEN, '')

        now: typing.Final = datetime.datetime.now(tz=datetime.timezone.utc)

        if "esi-corporations.read_structures.v1" in client_session.get(EveSSO.ESI_ACCESS_TOKEN_SCOPES, []):
            await self.run_structures(now, character_id, corporation_id, access_token)

        if "esi-industry.read_corporation_mining.v1" in client_session.get(EveSSO.ESI_ACCESS_TOKEN_SCOPES, []):
            await self.roll_extractions(now, character_id, corporation_id, access_token)
            await self.run_extractions(now, character_id, corporation_id, access_token)


class EveStructurePollingTask(EveStructureTask):

    @otel
    async def get_available_periodic_credentials(self, now: datetime.datetime) -> dict[int, EveTables.PeriodicCredentials]:

        available_credentials_dict: typing.Final = dict()
        try:
            async with await self.db.sessionmaker() as session:

                query = (
                    sqlalchemy.select(EveTables.PeriodicCredentials)
                    .where(EveTables.PeriodicCredentials.is_permitted.is_(True))
                    .where(EveTables.PeriodicCredentials.access_token_exiry > now)
                    .order_by(sqlalchemy.asc(EveTables.PeriodicCredentials.access_token_exiry))
                )

                results: sqlalchemy.engine.Result = await session.execute(query)

                for obj in [x for x in results.scalars()]:
                    obj: EveTables.PeriodicCredentials

                    if not obj.is_station_manager_role:
                        continue

                    if not available_credentials_dict.get(obj.corporation_id) is None:
                        continue

                    available_credentials_dict[obj.corporation_id] = obj

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        # self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: available_credentials_dict: {sorted(available_credentials_dict.keys())}")
        return available_credentials_dict

    @otel
    async def get_refresh_times(self, corporation_id_list: list) -> dict[int, datetime.datetime]:

        corporation_refresh_history_dict: typing.Final = {x: datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc) for x in corporation_id_list}
        try:
            async with await self.db.sessionmaker() as session:

                query = (
                    sqlalchemy.select(EveTables.PeriodicTaskTimestamp)
                    .where(EveTables.PeriodicTaskTimestamp.corporation_id.in_(corporation_id_list))
                )

                results: sqlalchemy.engine.Result = await session.execute(query)

                for obj in [x for x in results.scalars()]:
                    obj: EveTables.PeriodicTaskTimestamp

                    corporation_refresh_history_dict[obj.corporation_id] = obj.timestamp

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        # self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: refresh_history_dict: {str(refresh_history_dict)}")
        return corporation_refresh_history_dict

    @otel
    async def set_refresh_times(self, corporation_id: int, character_id: int, timestamp: datetime.datetime) -> None:
        try:
            async with await self.db.sessionmaker() as session, session.begin():

                query = (
                    sqlalchemy.select(EveTables.PeriodicTaskTimestamp)
                    .where(EveTables.PeriodicTaskTimestamp.corporation_id == corporation_id)
                )

                results: sqlalchemy.engine.Result = await session.execute(query)

                obj_list = [x for x in results.scalars()]
                if len(obj_list) > 0:
                    for x in obj_list:
                        x: EveTables.PeriodicTaskTimestamp
                        x.timestamp = timestamp
                        x.character_id = character_id
                else:
                    session.add(EveTables.PeriodicTaskTimestamp(corporation_id=corporation_id, character_id=character_id))

                await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

    @otel
    async def run_once(self, client_session: collections.abc.MutableSet):

        refresh_buffer: typing.Final = datetime.timedelta(seconds=20)
        refresh_interval: typing.Final = datetime.timedelta(seconds=360) - refresh_buffer
        now: typing.Final = datetime.datetime.now(tz=datetime.timezone.utc)

        available_corporation_id_dict: typing.Final = await self.get_available_periodic_credentials(now)
        if len(available_corporation_id_dict.keys()) == 0:
            await asyncio.sleep(refresh_buffer.total_seconds())
            return

        corporation_refresh_history_dict: typing.Final = await self.get_refresh_times(available_corporation_id_dict.keys())
        oldest_corporation_id = sorted(corporation_refresh_history_dict.keys(), key=lambda x: corporation_refresh_history_dict[x])[0]
        oldest_corporation_timestamp = corporation_refresh_history_dict[oldest_corporation_id]
        # self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: oldest_corporation_id: {oldest_corporation_id}, oldest_timestamp: {oldest_timestamp}")

        if oldest_corporation_timestamp + refresh_interval > now:
            remaining_interval: typing.Final[datetime.timedelta] = oldest_corporation_timestamp + refresh_interval + refresh_buffer - now
            remaining_sleep_interval: typing.Final = min(refresh_interval.total_seconds(), remaining_interval.total_seconds())
            await asyncio.sleep(remaining_sleep_interval)
            return

        # Should make these asyncio tasks ...
        refresh_count = 0
        for corporation_id, oldest_corporation_timestamp in corporation_refresh_history_dict.items():

            if oldest_corporation_timestamp + refresh_interval > now:
                continue

            character_id: typing.Final = available_corporation_id_dict[corporation_id].character_id
            access_token: typing.Final = available_corporation_id_dict[corporation_id].access_token
            self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: updating corporation_id: {corporation_id} with character_id: {character_id}")

            # XXX: fixme
            # need structures first, because of the db relationships .. urgh
            await self.run_structures(now, character_id, corporation_id, access_token)
            await self.roll_extractions(now, character_id, corporation_id, access_token)
            await self.run_extractions(now, character_id, corporation_id, access_token)

            refresh_count += 1
            # refresh_wobble: typing.Final = datetime.timedelta(seconds=random.randrange(refresh_interval.total_seconds())) - refresh_interval / 2

            await self.set_refresh_times(corporation_id, character_id, now)

        # otel_add_event(inspect.currentframe().f_code.co_name, {"refresh_count": refresh_count})
        # self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: refresh_count {refresh_count}")
        # await asyncio.sleep(int(refresh_buffer.total_seconds()))

    async def run(self, client_session: collections.abc.MutableSet):
        tracer: typing.Final = opentelemetry.trace.get_tracer_provider().get_tracer(inspect.currentframe().f_code.co_name)
        while True:
            try:
                with tracer.start_as_current_span(inspect.currentframe().f_code.co_name):
                    await self.run_once(client_session)
            except Exception as ex:
                otel_add_exception(ex)
                self.logger.error(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")
