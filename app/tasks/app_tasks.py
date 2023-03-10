import asyncio
import collections
import collections.abc
import datetime
import inspect
import logging
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

from support.telemetry import otel, otel_add_exception

from .. import (AppDatabase, AppSSO, AppTables, AppConstants, MoonExtractionCompletedEvent,
                MoonExtractionScheduledEvent, StructureStateChangedEvent)
from .task import AppDatabaseTask


class AppCommonState:

    def __init__(self, db: AppDatabase, logger: logging.Logger | None = None) -> None:
        self.db: typing.Final = db
        self.logger: typing.Final = logger or logging.getLogger(self.__class__.__name__)
        self.name: typing.Final = self.__class__.__name__

    async def check_changes(self, structure_id: int, prior_exists: bool, now_exists: bool, prior_edict: dict, now_edict: dict) -> dict:
        result = dict()
        if not now_exists:
            if prior_exists:
                result = prior_edict | {'exists': now_exists}
        elif len(now_edict) > 0:
            for k, v in prior_edict.items():
                if k in ['character_id', 'corporation_id']:
                    continue
                if v != now_edict.get(k):
                    self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {structure_id} {k} changed from {v} to {now_edict.get(k)}")
                    result = prior_edict | now_edict | {'exists': now_exists}
                    break
        return result


class AppStructureState(AppCommonState):

    async def structure_ids(self, corporation_id: int) -> set[int]:
        try:
            async with await self.db.sessionmaker() as session:
                session: sqlalchemy.ext.asyncio.AsyncSession

                query = (
                    sqlalchemy.select(sqlalchemy.distinct(AppTables.StructureHistory.structure_id))
                    .where(
                        sqlalchemy.and_(
                            AppTables.StructureHistory.corporation_id == corporation_id,
                            AppTables.StructureHistory.exists == sqlalchemy.sql.expression.true(),
                            AppTables.StructureHistory.structure_id.not_in(
                                sqlalchemy.select(sqlalchemy.distinct(AppTables.StructureHistory.structure_id))
                                .where(
                                    sqlalchemy.and_(
                                        AppTables.StructureHistory.corporation_id == corporation_id,
                                        AppTables.StructureHistory.exists == sqlalchemy.sql.expression.false()
                                    )
                                )
                            )
                        )
                    )
                )

                return {x async for x in await session.stream_scalars(query)}

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        return set()

    async def get(self, structure_id: int) -> AppTables.StructureHistory | None:
        try:
            async with await self.db.sessionmaker() as session:
                session: sqlalchemy.ext.asyncio.AsyncSession

                query = (
                    sqlalchemy.select(AppTables.StructureHistory)
                    .where(
                        AppTables.StructureHistory.structure_id == structure_id
                    )
                    .order_by(sqlalchemy.desc(AppTables.StructureHistory.id))
                    .limit(1)
                )

                query_result: sqlalchemy.engine.Result = await session.execute(query)
                return query_result.scalar_one_or_none()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        return None

    async def set(self, structure_id: int, now_edict: dict, now_exists: bool, now_timestamp: datetime.datetime | None = None) -> bool:
        try:
            structure_changed = now_exists and now_edict
            structure_obj: typing.Final = await self.get(structure_id)
            if not structure_obj:
                self.logger.warning(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {structure_id} is {structure_obj}")

            if structure_obj:
                prior_edict: typing.Final = {x: getattr(structure_obj, x) for x in structure_obj.__table__.columns.keys() if x not in ['id', 'exists', 'timestamp']}
                change_edict = await self.check_changes(structure_id, structure_obj.exists, now_exists, prior_edict, now_edict)
                structure_changed = len(change_edict) > 0
                if structure_changed:
                    now_edict = change_edict

            if structure_changed and now_edict:
                now_edict |= {'exists': now_exists}
                if now_timestamp is not None:
                    now_edict |= {'timestamp': now_timestamp}

                async with await self.db.sessionmaker() as session, session.begin():
                    session: sqlalchemy.ext.asyncio.AsyncSession
                    session.add(AppTables.StructureHistory(**now_edict))
                    await session.commit()

                return True

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        return False


class AppExtractionState(AppCommonState):

    async def structure_ids(self, corporation_id: int) -> set[int]:
        try:
            async with await self.db.sessionmaker() as session:
                session: sqlalchemy.ext.asyncio.AsyncSession

                query = (
                    sqlalchemy.select(sqlalchemy.distinct(AppTables.ExtractionHistory.structure_id))
                    .where(
                        sqlalchemy.and_(
                            AppTables.ExtractionHistory.corporation_id == corporation_id,
                            AppTables.ExtractionHistory.exists == sqlalchemy.sql.expression.true(),
                            AppTables.ExtractionHistory.structure_id.not_in(
                                sqlalchemy.select(sqlalchemy.distinct(AppTables.ExtractionHistory.structure_id))
                                .where(
                                    sqlalchemy.and_(
                                        AppTables.ExtractionHistory.corporation_id == corporation_id,
                                        AppTables.ExtractionHistory.exists == sqlalchemy.sql.expression.false()
                                    )
                                )
                            )
                        )
                    )
                )

                return {x async for x in await session.stream_scalars(query)}

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        return set()

    async def get(self, structure_id: int) -> AppTables.ExtractionHistory | None:
        try:
            async with await self.db.sessionmaker() as session:
                session: sqlalchemy.ext.asyncio.AsyncSession

                query = (
                    sqlalchemy.select(AppTables.ExtractionHistory)
                    .where(
                        AppTables.ExtractionHistory.structure_id == structure_id
                    )
                    .order_by(sqlalchemy.desc(AppTables.ExtractionHistory.id))
                    .limit(1)
                )

                query_result: sqlalchemy.engine.Result = await session.execute(query)
                return query_result.scalar_one_or_none()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        return None

    async def set(self, structure_id: int, now_edict: dict, now_exists: bool, now_timestamp: datetime.datetime | None = None) -> bool:
        try:
            structure_changed = now_exists and now_edict
            structure_obj: typing.Final = await self.get(structure_id)
            if not structure_obj:
                self.logger.warning(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {structure_id} is {structure_obj}")

            if structure_obj:
                prior_edict: typing.Final = {x: getattr(structure_obj, x) for x in structure_obj.__table__.columns.keys() if x not in ['id', 'exists', 'timestamp']}
                change_edict = await self.check_changes(structure_id, structure_obj.exists, now_exists, prior_edict, now_edict)
                structure_changed = len(change_edict) > 0
                if structure_changed:
                    now_edict = change_edict

            if structure_changed and now_edict:
                now_edict |= {'exists': now_exists}
                if now_timestamp is not None:
                    now_edict |= {'timestamp': now_timestamp}

                async with await self.db.sessionmaker() as session:
                    session: sqlalchemy.ext.asyncio.AsyncSession

                    session.begin()
                    session.add(AppTables.ExtractionHistory(**now_edict))
                    await session.commit()

                return True

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        return False


class AppStructureTask(AppDatabaseTask):

    @otel
    def __init__(self, client_session: collections.abc.MutableMapping, db: AppDatabase, outbound: asyncio.Queue, logger: logging.Logger | None = None) -> None:
        super().__init__(client_session, db, outbound, logger)
        self.structure_state: typing.Final = AppStructureState(db, logger)
        self.extraction_state: typing.Final = AppExtractionState(db, logger)

    @otel
    async def run_structures(self, now: datetime.datetime, character_id: int, corporation_id: int, access_token: str) -> None:

        prior_structure_id_set: typing.Final = await self.structure_state.structure_ids(corporation_id)

        url: typing.Final = f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/corporations/{corporation_id}/structures/"
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
                session.add(AppTables.StructurQueryLog(corporation_id=corporation_id, character_id=character_id, json=structures))
                await session.commit()
        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        structure_obj_dict: typing.Final = dict()
        await self.backfill_corporations({corporation_id})

        for x in structures:
            edict: typing.Final[dict[str, int | bool | datetime.datetime]] = dict({
                "character_id": character_id,
            })
            for k, v in x.items():
                if k in ["fuel_expires", "state_timer_end", "state_timer_start", "unanchors_at"]:
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

            obj = AppTables.Structure(**edict)
            # self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {obj}")

            structure_obj_dict[obj.structure_id] = obj

            await self.backfill_types({obj.type_id})
            await self.backfill_corporations({obj.corporation_id})

            structure_changed = await self.structure_state.set(obj.structure_id, edict, now_exists=True)
            if self.outbound and structure_changed and isinstance(obj, AppTables.Structure):
                await self.outbound.put(StructureStateChangedEvent(
                    structure_id=obj.structure_id,
                    corporation_id=corporation_id,
                    system_id=obj.system_id,
                    exists=True,
                    state=obj.state,
                    state_timer_start=obj.state_timer_start,
                    state_timer_end=obj.state_timer_end
                ))

        # Remove deleted
        for structure_id in set(prior_structure_id_set) - set(structure_obj_dict.keys()):
            previous_state: typing.Final = await self.structure_state.get(structure_id)
            structure_changed = await self.structure_state.set(structure_id, dict(), now_exists=False)
            if self.outbound and structure_changed and isinstance(previous_state, AppTables.StructureHistory):
                await self.outbound.put(StructureStateChangedEvent(
                    structure_id=structure_id,
                    corporation_id=corporation_id,
                    system_id=previous_state.system_id,
                    exists=False,
                    state=previous_state.state,
                    state_timer_start=previous_state.state_timer_start,
                    state_timer_end=previous_state.state_timer_end
                ))

        # Ugly hack to remove structures that no longer exist. If I was better with the ORM I would not have to do this ..
        try:
            async with await self.db.sessionmaker() as session:

                query_list: typing.Final = list([
                    sqlalchemy.delete(AppTables.CompletedExtraction)
                    .where(
                        sqlalchemy.and_(
                            AppTables.CompletedExtraction.corporation_id == corporation_id,
                            AppTables.CompletedExtraction.structure_id.not_in(structure_obj_dict.keys())
                        )
                    ),

                    sqlalchemy.delete(AppTables.ScheduledExtraction)
                    .where(
                        sqlalchemy.and_(
                            AppTables.ScheduledExtraction.corporation_id == corporation_id,
                            AppTables.ScheduledExtraction.structure_id.not_in(structure_obj_dict.keys())
                        )
                    ),

                    sqlalchemy.delete(AppTables.Structure)
                    .where(
                        sqlalchemy.and_(
                            AppTables.Structure.corporation_id == corporation_id,
                            AppTables.Structure.structure_id.not_in(structure_obj_dict.keys())
                        )
                    ),
                ])

                session.begin()
                for query in query_list:
                    await session.execute(query)
                await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        # Stop here if there are no structures
        if not len(structure_obj_dict) > 0:
            return

        try:
            async with await self.db.sessionmaker() as session:
                session: sqlalchemy.ext.asyncio.AsyncSession

                query = (
                    sqlalchemy.select(AppTables.Structure)
                    .where(AppTables.Structure.corporation_id == corporation_id)
                )

                session.begin()

                existing_structure_obj_dict: typing.Final = {x.structure_id: x async for x in await session.stream_scalars(query)}

                for structure_id, obj in structure_obj_dict.items():
                    existing_obj: AppTables.Structure | None = existing_structure_obj_dict.get(structure_id)

                    if not existing_obj:
                        self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {obj} added")
                        session.add(obj)
                        continue

                    same_attributes = True
                    for attribute in [x for x in obj.__table__.columns.keys() if x not in ['timestamp', 'character_id']]:
                        if getattr(obj, attribute) != getattr(existing_obj, attribute):
                            self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {structure_id} {attribute} {getattr(existing_obj, attribute)} -> {getattr(obj, attribute)}")
                            setattr(existing_obj, attribute, getattr(obj, attribute))
                            same_attributes = False

                    if same_attributes:
                        continue

                    existing_obj.timestamp = now
                    self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {existing_obj} updated")
                    session.add(existing_obj)

                await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

    @otel
    async def roll_extractions(self, now: datetime.datetime, character_id: int, corporation_id: int, access_token: str) -> None:

        # Migrated scheduled extractions to completed extractions
        try:
            async with await self.db.sessionmaker() as session:
                session: sqlalchemy.ext.asyncio.AsyncSession

                session.begin()

                query = (
                    sqlalchemy.select(AppTables.ScheduledExtraction)
                    .where(AppTables.ScheduledExtraction.corporation_id == corporation_id)
                )

                scheduled_extraction_dict = {x.structure_id: x async for x in await session.stream_scalars(query)}

                query = (
                    sqlalchemy.select(AppTables.CompletedExtraction)
                    .where(AppTables.CompletedExtraction.corporation_id == corporation_id)
                )

                completed_extraction_dict = {x.structure_id: x async for x in await session.stream_scalars(query)}

                delete_completed_id_set: typing.Final = set()
                add_completed_obj_set: typing.Final = set()

                # migrated_structure_id_set = set()
                for structure_id, scheduled_obj in scheduled_extraction_dict.items():
                    scheduled_obj: AppTables.ScheduledExtraction

                    if now <= scheduled_obj.chunk_arrival_time:
                        continue

                    completed_obj: AppTables.CompletedExtraction | None = completed_extraction_dict.get(structure_id)
                    if completed_obj:
                        if completed_obj.chunk_arrival_time == scheduled_obj.chunk_arrival_time:
                            self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {scheduled_obj} already migrated")
                            continue

                    belt_lifetime_estimate = datetime.timedelta(days=2)
                    query = (
                        sqlalchemy.select(AppTables.StructureModifiers)
                        .where(AppTables.StructureModifiers.structure_id == structure_id)
                        .limit(1)
                    )
                    query_result: sqlalchemy.engine.Result = await session.execute(query)
                    obj = query_result.scalar_one_or_none()
                    if obj and isinstance(obj, AppTables.StructureModifiers):
                        belt_lifetime_estimate *= float(obj.belt_lifetime_modifier)

                    new_completed_obj = AppTables.CompletedExtraction(
                        character_id=scheduled_obj.character_id,
                        corporation_id=scheduled_obj.corporation_id,
                        structure_id=scheduled_obj.structure_id,
                        moon_id=scheduled_obj.moon_id,
                        extraction_start_time=scheduled_obj.extraction_start_time,
                        chunk_arrival_time=scheduled_obj.chunk_arrival_time,
                        natural_decay_time=scheduled_obj.natural_decay_time,
                        belt_decay_time=scheduled_obj.chunk_arrival_time + belt_lifetime_estimate + (scheduled_obj.natural_decay_time - scheduled_obj.chunk_arrival_time) / 2
                    )

                    if self.outbound and isinstance(completed_obj, AppTables.CompletedExtraction):
                        await self.outbound.put(MoonExtractionCompletedEvent(
                            structure_id=new_completed_obj.structure_id,
                            corporation_id=new_completed_obj.corporation_id,
                            moon_id=new_completed_obj.moon_id,
                            extraction_start_time=new_completed_obj.extraction_start_time,
                            chunk_arrival_time=new_completed_obj.chunk_arrival_time,
                            belt_decay_time=new_completed_obj.belt_decay_time
                        ))

                    if completed_obj:
                        self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {completed_obj} removed")
                        delete_completed_id_set.add(structure_id)

                    if new_completed_obj:
                        self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {new_completed_obj} added")
                        add_completed_obj_set.add(new_completed_obj)

                    self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {scheduled_obj} -> {new_completed_obj}")

                if len(delete_completed_id_set) > 0:
                    query = (
                        sqlalchemy.delete(AppTables.CompletedExtraction)
                        .where(AppTables.CompletedExtraction.structure_id.in_(delete_completed_id_set))
                    )
                    await session.execute(query)

                if len(add_completed_obj_set) > 0:
                    session.add_all(add_completed_obj_set)

                await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

    @otel
    async def run_extractions(self, now: datetime.datetime, character_id: int, corporation_id: int, access_token: str) -> None:

        url = f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/corporation/{corporation_id}/mining/extractions/"
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
                session.add(AppTables.ExtractionQueryLog(corporation_id=corporation_id, character_id=character_id, json=extractions))
                await session.commit()
        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        extractions_obj_dict: typing.Final = dict()

        if len(extractions) > 0:

            for x in extractions:
                edict: typing.Final[dict[str, int | bool | datetime.datetime]] = dict({
                    "character_id": character_id,
                    "corporation_id": corporation_id,
                })
                for k, v in x.items():
                    if k in ["chunk_arrival_time", "extraction_start_time", "natural_decay_time"]:
                        v = dateutil.parser.parse(v).replace(tzinfo=datetime.timezone.utc)
                    elif k in ["structure_id", "moon_id"]:
                        v = int(v)
                    else:
                        continue
                    edict[k] = v

                obj = AppTables.ScheduledExtraction(**edict)
                # self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {obj}")

                extractions_obj_dict[obj.structure_id] = obj

                await self.backfill_moons({obj.moon_id})
                await self.backfill_corporations({obj.corporation_id})

                structure_changed = await self.extraction_state.set(obj.structure_id, edict, now_exists=True)
                if self.outbound and structure_changed and isinstance(obj, AppTables.ScheduledExtraction):
                    await self.outbound.put(MoonExtractionScheduledEvent(
                        structure_id=obj.structure_id,
                        corporation_id=obj.corporation_id,
                        moon_id=obj.moon_id,
                        extraction_start_time=obj.extraction_start_time,
                        chunk_arrival_time=obj.chunk_arrival_time,
                    ))

        try:
            async with await self.db.sessionmaker() as session:
                session: sqlalchemy.ext.asyncio.AsyncSession

                session.begin()

                query = (
                    sqlalchemy.select(AppTables.ScheduledExtraction)
                    .where(AppTables.ScheduledExtraction.corporation_id == corporation_id)
                )
                scheduled_extraction_dict: typing.Final = {x.structure_id: x async for x in await session.stream_scalars(query)}

                deleted_structure_id_set: typing.Final = set()
                for structure_id, existing_obj in scheduled_extraction_dict.items():
                    if extractions_obj_dict.get(structure_id) is None:
                        self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {existing_obj} deleted")
                        await session.delete(existing_obj)
                        deleted_structure_id_set.add(structure_id)

                for structure_id in deleted_structure_id_set:
                    scheduled_extraction_dict.pop(structure_id)

                for structure_id, obj in extractions_obj_dict.items():
                    existing_obj: AppTables.ScheduledExtraction | None = scheduled_extraction_dict.get(structure_id)

                    if not existing_obj:
                        # if obj.chunk_arrival_time < now:
                        #     self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: NOT ADDING {obj}")
                        #     continue
                        self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {obj} added")
                        session.add(obj)
                        continue

                    same_attributes = True
                    for attribute in [x for x in obj.__table__.columns.keys() if x not in ['timestamp', 'character_id']]:
                        if getattr(obj, attribute) != getattr(existing_obj, attribute):
                            self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {structure_id} {attribute} {getattr(existing_obj, attribute)} -> {getattr(obj, attribute)}")
                            setattr(existing_obj, attribute, getattr(obj, attribute))
                            same_attributes = False

                    if same_attributes:
                        continue

                    existing_obj.timestamp = now
                    self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {existing_obj} updated")
                    session.add(existing_obj)

                await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

    @otel
    async def run(self, client_session: collections.abc.MutableMapping) -> None:
        if not client_session.get(AppSSO.ESI_CHARACTER_IS_STATION_MANAGER_ROLE, False):
            return

        character_id: typing.Final = int(client_session.get(AppSSO.ESI_CHARACTER_ID, 0))
        corporation_id: typing.Final = int(client_session.get(AppSSO.ESI_CORPORATION_ID, 0))
        access_token: typing.Final = client_session.get(AppSSO.ESI_ACCESS_TOKEN, '')

        now: typing.Final = datetime.datetime.now(tz=datetime.timezone.utc)

        if "esi-corporations.read_structures.v1" in client_session.get(AppSSO.ESI_SCOPES, []):
            await self.run_structures(now, character_id, corporation_id, access_token)

        if "esi-industry.read_corporation_mining.v1" in client_session.get(AppSSO.ESI_SCOPES, []):
            await self.roll_extractions(now, character_id, corporation_id, access_token)
            await self.run_extractions(now, character_id, corporation_id, access_token)


class AppStructurePollingTask(AppStructureTask):

    # STRUCTURE_REFRESH_INTERVAL_SECONDS: typing.Final = 300
    STRUCTURE_REFRESH_INTERVAL_SECONDS: typing.Final = 360
    # STRUCTURE_REFRESH_INTERVAL_SECONDS: typing.Final = 450

    @otel
    async def get_available_periodic_credentials(self, now: datetime.datetime) -> dict[int, AppTables.PeriodicCredentials]:

        available_credentials_dict: typing.Final = dict()
        try:
            async with await self.db.sessionmaker() as session:
                session: sqlalchemy.ext.asyncio.AsyncSession

                query = (
                    sqlalchemy.select(AppTables.PeriodicCredentials)
                    .where(AppTables.PeriodicCredentials.is_enabled.is_(True))
                    .where(AppTables.PeriodicCredentials.access_token_expiry > now)
                    .order_by(sqlalchemy.asc(AppTables.PeriodicCredentials.access_token_expiry))
                )

                async for obj in await session.stream_scalars(query):
                    obj: AppTables.PeriodicCredentials

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
                session: sqlalchemy.ext.asyncio.AsyncSession

                query = (
                    sqlalchemy.select(AppTables.PeriodicTaskTimestamp)
                    .where(AppTables.PeriodicTaskTimestamp.corporation_id.in_(corporation_id_list))
                )

                async for obj in await session.stream_scalars(query):
                    obj: AppTables.PeriodicTaskTimestamp

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
                session: sqlalchemy.ext.asyncio.AsyncSession

                query = (
                    sqlalchemy.select(AppTables.PeriodicTaskTimestamp)
                    .where(AppTables.PeriodicTaskTimestamp.corporation_id == corporation_id)
                )

                obj_list = [x async for x in await session.stream_scalars(query)]
                if len(obj_list) > 0:
                    for x in obj_list:
                        x: AppTables.PeriodicTaskTimestamp
                        x.timestamp = timestamp
                        x.character_id = character_id
                        session.add(x)
                else:
                    session.add(AppTables.PeriodicTaskTimestamp(corporation_id=corporation_id, character_id=character_id))

                await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

    @otel
    async def run_once(self, client_session: collections.abc.MutableSet):

        refresh_interval: typing.Final = datetime.timedelta(seconds=self.STRUCTURE_REFRESH_INTERVAL_SECONDS)
        now: typing.Final = datetime.datetime.now(tz=datetime.timezone.utc)

        available_corporation_id_dict: typing.Final = await self.get_available_periodic_credentials(now)
        if len(available_corporation_id_dict.keys()) == 0:
            await asyncio.sleep(refresh_interval.total_seconds())
            return

        corporation_refresh_history_dict: typing.Final[dict] = await self.get_refresh_times(available_corporation_id_dict.keys())
        oldest_corporation_id = sorted(corporation_refresh_history_dict.keys(), key=lambda x: corporation_refresh_history_dict[x])[0]
        oldest_corporation_timestamp = corporation_refresh_history_dict[oldest_corporation_id]
        # self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: oldest_corporation_id: {oldest_corporation_id}, oldest_timestamp: {oldest_timestamp}")

        if oldest_corporation_timestamp + refresh_interval > now:
            remaining_interval: datetime.timedelta = (oldest_corporation_timestamp + refresh_interval) - (now)
            # remaining_sleep_interval = min(refresh_interval.total_seconds(), remaining_interval.total_seconds())
            await asyncio.sleep(remaining_interval.total_seconds())
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

    async def run(self, client_session: collections.abc.MutableSet):
        tracer: typing.Final = opentelemetry.trace.get_tracer_provider().get_tracer(inspect.currentframe().f_code.co_name)
        while True:
            try:
                with tracer.start_as_current_span(inspect.currentframe().f_code.co_name):
                    await self.run_once(client_session)
            except Exception as ex:
                otel_add_exception(ex)
                self.logger.error(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")
