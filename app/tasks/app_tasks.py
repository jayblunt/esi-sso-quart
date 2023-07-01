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

from .. import (AppConstants, AppDatabase, AppESI, AppSSO, AppTables,
                MoonExtractionCompletedEvent, MoonExtractionScheduledEvent,
                StructureStateChangedEvent)
from .task import AppDatabaseTask


class AppCommonState:

    def __init__(self, db: AppDatabase, logger: logging.Logger | None = None) -> None:
        self.db: typing.Final = db
        self.logger: typing.Final = logger or logging.getLogger(self.__class__.__name__)
        self.name: typing.Final = self.__class__.__name__

    async def query_scalar_set(self, query: sqlalchemy.sql.Select) -> set():
        try:
            async with await self.db.sessionmaker() as session:
                session: sqlalchemy.ext.asyncio.AsyncSession
                return {x async for x in await session.stream_scalars(query)}

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        return set()

    async def changes(self, structure_id: int, prior_exists: bool, now_exists: bool, prior_edict: dict, now_edict: dict) -> dict:
        result = dict()
        if not now_exists:
            if prior_exists:
                result = prior_edict | {'exists': now_exists}
        elif len(now_edict) > 0:
            nchanges = 0
            for k, v in prior_edict.items():
                if k in ['character_id', 'corporation_id']:
                    continue
                if v != now_edict.get(k):
                    self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {structure_id} {k} changed from {v} to {now_edict.get(k)}")
                    nchanges += 1
            if nchanges > 0:
                result = now_edict | {'exists': now_exists}
        return result


class AppStructureState(AppCommonState):

    async def structure_ids(self, corporation_id: int) -> set[int]:

        query = (
            sqlalchemy.select(sqlalchemy.distinct(AppTables.StructureHistory.structure_id))
            .where(
                sqlalchemy.and_(
                    AppTables.StructureHistory.corporation_id == corporation_id,
                    AppTables.StructureHistory.exists == sqlalchemy.sql.expression.true(),
                    AppTables.StructureHistory.structure_id.not_in(
                        sqlalchemy.select(sqlalchemy.distinct(AppTables.StructureHistory.structure_id))
                        .where(AppTables.StructureHistory.exists == sqlalchemy.sql.expression.false())
                    )
                )
            )
        )

        return await self.query_scalar_set(query)

    async def get(self, structure_id: int) -> AppTables.StructureHistory | None:
        try:
            async with await self.db.sessionmaker() as session:
                session: sqlalchemy.ext.asyncio.AsyncSession

                query = (
                    sqlalchemy.select(AppTables.StructureHistory)
                    .where(AppTables.StructureHistory.structure_id == structure_id)
                    .order_by(sqlalchemy.desc(AppTables.StructureHistory.id))
                    .limit(1)
                )

                query_result: sqlalchemy.engine.Result = await session.execute(query)
                return query_result.scalar_one_or_none()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        return None

    async def set(self, structure_id: int, now_edict: dict, now_exists: bool) -> bool:
        try:
            want_set = len(now_edict) > 0 and now_exists

            structure_obj: typing.Final = await self.get(structure_id)
            if structure_obj:
                prior_edict: typing.Final = {x: getattr(structure_obj, x) for x in structure_obj.__table__.columns.keys() if x not in ['id', 'exists', 'timestamp']}
                change_edict = await self.changes(structure_id, structure_obj.exists, now_exists, prior_edict, now_edict)
                want_set = bool(len(change_edict) > 0)
                if want_set:
                    now_edict = change_edict

            if want_set:
                self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {structure_id} want_set={want_set}")

                now_edict |= {'exists': now_exists}

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

        query = (
            sqlalchemy.select(sqlalchemy.distinct(AppTables.ScheduledExtraction.structure_id))
            .where(
                sqlalchemy.and_(
                    AppTables.ScheduledExtraction.corporation_id == corporation_id,
                    AppTables.ScheduledExtraction.structure_id.not_in(
                        sqlalchemy.select(sqlalchemy.distinct(AppTables.StructureHistory.structure_id))
                        .where(AppTables.StructureHistory.exists == sqlalchemy.sql.expression.false())
                    )
                )
            )
        )
        return await self.query_scalar_set(query)

    async def get(self, structure_id: int) -> AppTables.ExtractionHistory | None:
        try:
            async with await self.db.sessionmaker() as session:
                session: sqlalchemy.ext.asyncio.AsyncSession

                query = (
                    sqlalchemy.select(AppTables.ExtractionHistory)
                    .where(AppTables.ExtractionHistory.structure_id == structure_id)
                    .order_by(sqlalchemy.desc(AppTables.ExtractionHistory.id))
                    .limit(1)
                )

                query_result: sqlalchemy.engine.Result = await session.execute(query)
                return query_result.scalar_one_or_none()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        return None

    async def add(self, structure_id: int, new_edict: dict, new_exists: bool) -> bool:
        try:
            want_add = len(new_edict) > 0 and new_exists

            if want_add:
                extraction_obj: typing.Final = await self.get(structure_id)
                if extraction_obj:
                    old_edict = {x: getattr(extraction_obj, x) for x in extraction_obj.__table__.columns.keys() if x not in ['id', 'exists', 'timestamp']}
                    changes_edict = await self.changes(structure_id, extraction_obj.exists, new_exists, old_edict, new_edict)
                    want_add = want_add and bool(len(changes_edict) > 0)

            if want_add:
                new_edict |= {'exists': new_exists}
                self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {structure_id} want_add={want_add}, new_exists={new_exists}, new_edict={len(new_edict)}")
                async with await self.db.sessionmaker() as session:
                    session: sqlalchemy.ext.asyncio.AsyncSession

                    session.begin()
                    session.add(AppTables.ExtractionHistory(**new_edict))
                    await session.commit()

            return want_add

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
    async def get_pages(self, url: str, access_token: str) -> list[dict] | None:

        pages: typing.Final = await AppESI.get_pages(url, access_token, request_params=self.request_params)

        if pages is None:
            self.logger.warning(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: url:{url}: pages:{pages}")
            return None

        if len(pages) > 0 and len(pages) != len(list(filter(None, pages))):
            self.logger.warning(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: url:{url}: pages:{pages}")
            return None

        return pages

    @otel
    async def run_structures(self, now: datetime.datetime, character_id: int, corporation_id: int, access_token: str) -> None:

        url: typing.Final = f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/corporations/{corporation_id}/structures/"
        structures: typing.Final = await self.get_pages(url, access_token)

        if structures is None:
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

        previous_structure_id_set: typing.Final = await self.structure_state.structure_ids(corporation_id)
        current_structure_obj_dict: typing.Final = dict()

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
            self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {obj}")

            current_structure_obj_dict[obj.structure_id] = obj

            await self.backfill_types({obj.type_id})
            await self.backfill_corporations({obj.corporation_id})

            structure_changed = await self.structure_state.set(obj.structure_id, edict, now_exists=True)
            if structure_changed and self.outbound and isinstance(obj, AppTables.Structure):
                await self.outbound.put(StructureStateChangedEvent(
                    structure_id=obj.structure_id,
                    corporation_id=corporation_id,
                    system_id=obj.system_id,
                    exists=True,
                    state=obj.state,
                    state_timer_start=obj.state_timer_start,
                    state_timer_end=obj.state_timer_end,
                    fuel_expires=obj.fuel_expires,
                ))

        # Remove deleted
        deleted_structure_id_set: typing.Final = set(previous_structure_id_set) - set(current_structure_obj_dict.keys())
        for structure_id in deleted_structure_id_set:
            obj = await self.structure_state.get(structure_id)
            structure_changed = await self.structure_state.set(structure_id, dict(), now_exists=False)
            if structure_changed and self.outbound and isinstance(obj, AppTables.StructureHistory):
                await self.outbound.put(StructureStateChangedEvent(
                    structure_id=structure_id,
                    corporation_id=corporation_id,
                    system_id=obj.system_id,
                    exists=False,
                    state=obj.state,
                    state_timer_start=obj.state_timer_start,
                    state_timer_end=obj.state_timer_end,
                    fuel_expires=obj.fuel_expires,
                ))

        # Ugly hack to remove structures that no longer exist. If I was better with the ORM I would not have to do this ..
        try:
            async with await self.db.sessionmaker() as session:

                query_list: typing.Final = list([
                    sqlalchemy.delete(AppTables.CompletedExtraction)
                    .where(
                        sqlalchemy.and_(
                            AppTables.CompletedExtraction.corporation_id == corporation_id,
                            AppTables.CompletedExtraction.structure_id.not_in(current_structure_obj_dict.keys())
                        )
                    ),

                    sqlalchemy.delete(AppTables.ScheduledExtraction)
                    .where(
                        sqlalchemy.and_(
                            AppTables.ScheduledExtraction.corporation_id == corporation_id,
                            AppTables.ScheduledExtraction.structure_id.not_in(current_structure_obj_dict.keys())
                        )
                    ),

                    sqlalchemy.delete(AppTables.Structure)
                    .where(
                        sqlalchemy.and_(
                            AppTables.Structure.corporation_id == corporation_id,
                            AppTables.Structure.structure_id.not_in(current_structure_obj_dict.keys())
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
        if not len(current_structure_obj_dict) > 0:
            return

        # Now update the AppTables.Structure table
        try:
            async with await self.db.sessionmaker() as session:
                session: sqlalchemy.ext.asyncio.AsyncSession

                query = (
                    sqlalchemy.select(AppTables.Structure)
                    .where(AppTables.Structure.corporation_id == corporation_id)
                )

                session.begin()

                existing_structure_obj_dict: typing.Final = {x.structure_id: x async for x in await session.stream_scalars(query)}

                for structure_id, obj in current_structure_obj_dict.items():
                    existing_obj: AppTables.Structure = existing_structure_obj_dict.get(structure_id)

                    if not existing_obj:
                        self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {obj} added")
                        session.add(obj)
                        continue

                    obj_nchanges = 0
                    for attribute in [x for x in obj.__table__.columns.keys() if x not in ['timestamp', 'character_id']]:
                        if getattr(obj, attribute) != getattr(existing_obj, attribute):
                            self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {structure_id} {attribute} {getattr(existing_obj, attribute)} -> {getattr(obj, attribute)}")
                            setattr(existing_obj, attribute, getattr(obj, attribute))
                            obj_nchanges += 1

                    if obj_nchanges > 0:
                        existing_obj.timestamp = now
                        self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {existing_obj} updated")
                        session.add(existing_obj)

                await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

    @otel
    async def roll_extractions(self, now: datetime.datetime, character_id: int, corporation_id: int, changed_structure_id_set: set) -> None:

        try:
            async with await self.db.sessionmaker() as session:
                session: sqlalchemy.ext.asyncio.AsyncSession

                session.begin()

                query = (
                    sqlalchemy.select(AppTables.ScheduledExtraction)
                    .where(
                        sqlalchemy.and_(
                            AppTables.ScheduledExtraction.corporation_id == corporation_id,
                            AppTables.ScheduledExtraction.structure_id.in_(changed_structure_id_set)
                        )
                    )
                )

                scheduled_extraction_dict: typing.Final[dict[int, AppTables.ScheduledExtraction]] = {x.structure_id: x async for x in await session.stream_scalars(query)}

                for obj in [x for x in scheduled_extraction_dict.values()]:
                    if now < obj.chunk_arrival_time:
                        self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {obj} cancelled")
                        await session.delete(obj)
                        scheduled_extraction_dict.pop(obj.structure_id)

                query = (
                    sqlalchemy.select(AppTables.CompletedExtraction)
                    .where(
                        sqlalchemy.and_(
                            AppTables.CompletedExtraction.corporation_id == corporation_id,
                            AppTables.CompletedExtraction.structure_id.in_(changed_structure_id_set)
                        )
                    )
                )

                completed_extraction_dict: typing.Final[dict[int, AppTables.CompletedExtraction]] = {x.structure_id: x async for x in await session.stream_scalars(query)}

                # migrated_structure_id_set = set()
                for scheduled_structure_id, scheduled_obj in scheduled_extraction_dict.items():

                    await session.delete(scheduled_obj)
                    self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {scheduled_obj} remmoved")

                    previous_completed_obj = completed_extraction_dict.get(scheduled_structure_id)
                    if previous_completed_obj:

                        if previous_completed_obj.chunk_arrival_time == scheduled_obj.chunk_arrival_time:
                            self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {scheduled_obj} is {previous_completed_obj}")
                            continue

                        self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {previous_completed_obj} removed")
                        await session.delete(previous_completed_obj)

                    belt_lifetime_estimate = datetime.timedelta(days=2)
                    query = (
                        sqlalchemy.select(AppTables.StructureModifiers)
                        .where(AppTables.StructureModifiers.structure_id == scheduled_structure_id)
                        .limit(1)
                    )
                    query_result: sqlalchemy.engine.Result = await session.execute(query)
                    obj = query_result.scalar_one_or_none()
                    if obj and isinstance(obj, AppTables.StructureModifiers):
                        belt_lifetime_estimate *= float(obj.belt_lifetime_modifier)

                    actual_decay_time: typing.Final = min(now.replace(microsecond=0), scheduled_obj.natural_decay_time)

                    new_completed_obj = AppTables.CompletedExtraction(
                        character_id=scheduled_obj.character_id,
                        corporation_id=scheduled_obj.corporation_id,
                        structure_id=scheduled_obj.structure_id,
                        moon_id=scheduled_obj.moon_id,
                        extraction_start_time=scheduled_obj.extraction_start_time,
                        chunk_arrival_time=scheduled_obj.chunk_arrival_time,
                        natural_decay_time=scheduled_obj.natural_decay_time,
                        belt_decay_time=actual_decay_time + belt_lifetime_estimate
                    )


                    session.add(new_completed_obj)
                    self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {new_completed_obj} added")

                    if self.outbound and isinstance(new_completed_obj, AppTables.CompletedExtraction):
                        await self.outbound.put(MoonExtractionCompletedEvent(
                            structure_id=new_completed_obj.structure_id,
                            corporation_id=new_completed_obj.corporation_id,
                            moon_id=new_completed_obj.moon_id,
                            extraction_start_time=new_completed_obj.extraction_start_time,
                            chunk_arrival_time=new_completed_obj.chunk_arrival_time,
                            belt_decay_time=new_completed_obj.belt_decay_time
                        ))

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

        # Add the extractions to the query log
        try:
            async with await self.db.sessionmaker() as session, session.begin():
                session.add(AppTables.ExtractionQueryLog(corporation_id=corporation_id, character_id=character_id, json=extractions))
                await session.commit()
        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        previous_structure_id_set: typing.Final = await self.extraction_state.structure_ids(corporation_id)
        changed_structure_id_set: typing.Final = set()
        current_extractions_obj_dict: typing.Final = dict()

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
            self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {obj}")

            current_extractions_obj_dict[obj.structure_id] = obj

            await self.backfill_moons({obj.moon_id})
            await self.backfill_corporations({obj.corporation_id})

            extraction_changed = await self.extraction_state.add(obj.structure_id, edict, new_exists=True)
            if extraction_changed:
                self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: ROLL CHANGED {obj.structure_id} 1")
                changed_structure_id_set.add(obj.structure_id)
            elif now > obj.natural_decay_time:
                self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: ROLL DECAYED {obj.structure_id} 1")
                changed_structure_id_set.add(obj.structure_id)

        deleted_extraction_id_set: typing.Final = set(previous_structure_id_set) - set(current_extractions_obj_dict.keys())
        for structure_id in deleted_extraction_id_set:
            self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: ROLL DELETED {structure_id} 1")
            changed_structure_id_set.add(structure_id)

        for structure_id in changed_structure_id_set:
            obj = await self.extraction_state.get(structure_id)
            if self.outbound and isinstance(obj, AppTables.ScheduledExtraction):
                await self.outbound.put(MoonExtractionScheduledEvent(
                    structure_id=obj.structure_id,
                    corporation_id=obj.corporation_id,
                    moon_id=obj.moon_id,
                    extraction_start_time=obj.extraction_start_time,
                    chunk_arrival_time=obj.chunk_arrival_time,
                ))

        if len(changed_structure_id_set) > 0:
            await self.roll_extractions(now, character_id, corporation_id, changed_structure_id_set)


        # Stop here if there are no structures
        if not len(current_extractions_obj_dict) > 0:
            return


        # Now update the AppTables.Structure table
        try:
            async with await self.db.sessionmaker() as session:
                session: sqlalchemy.ext.asyncio.AsyncSession

                query = (
                    sqlalchemy.select(AppTables.ScheduledExtraction)
                    .where(AppTables.ScheduledExtraction.corporation_id == corporation_id)
                )

                session.begin()

                existing_extraction_obj_dict: typing.Final = {x.structure_id: x async for x in await session.stream_scalars(query)}

                for structure_id, obj in current_extractions_obj_dict.items():
                    existing_obj: AppTables.ScheduledExtraction = existing_extraction_obj_dict.get(structure_id)

                    if not existing_obj:
                        self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {obj} added")
                        session.add(obj)
                        continue

                    obj_nchanges = 0
                    for attribute in [x for x in obj.__table__.columns.keys() if x not in ['timestamp', 'character_id']]:
                        if getattr(obj, attribute) != getattr(existing_obj, attribute):
                            self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {structure_id} {attribute} {getattr(existing_obj, attribute)} -> {getattr(obj, attribute)}")
                            setattr(existing_obj, attribute, getattr(obj, attribute))
                            obj_nchanges += 1

                    if obj_nchanges > 0:
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
            await self.run_extractions(now, character_id, corporation_id, access_token)


class AppStructurePollingTask(AppStructureTask):

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

        refresh_interval: typing.Final = datetime.timedelta(seconds=AppConstants.STRUCTURE_REFRESH_INTERVAL_SECONDS)
        now: typing.Final = datetime.datetime.now(tz=datetime.timezone.utc)

        available_corporation_id_dict: typing.Final = await self.get_available_periodic_credentials(now)
        if len(available_corporation_id_dict.keys()) == 0:
            self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {len(available_corporation_id_dict.keys())=}")
            await asyncio.sleep(refresh_interval.total_seconds())
            return

        corporation_refresh_history_dict: typing.Final[dict] = await self.get_refresh_times(available_corporation_id_dict.keys())
        oldest_corporation_id = sorted(corporation_refresh_history_dict.keys(), key=lambda x: corporation_refresh_history_dict[x])[0]
        oldest_corporation_timestamp = corporation_refresh_history_dict[oldest_corporation_id]
        # self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: oldest_corporation_id: {oldest_corporation_id}, oldest_timestamp: {oldest_timestamp}")

        if oldest_corporation_timestamp + refresh_interval > now:
            remaining_interval: datetime.timedelta = (oldest_corporation_timestamp + refresh_interval) - (now)
            # remaining_sleep_interval = min(refresh_interval.total_seconds(), remaining_interval.total_seconds())
            self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {oldest_corporation_id=}, {oldest_corporation_timestamp=}, {remaining_interval=}")
            await asyncio.sleep(remaining_interval.total_seconds())
            return

        # Should make these asyncio tasks ...
        # refresh_count = 0
        for corporation_id, oldest_corporation_timestamp in corporation_refresh_history_dict.items():

            if oldest_corporation_timestamp + refresh_interval > now:
                continue

            character_id: typing.Final = available_corporation_id_dict[corporation_id].character_id
            access_token: typing.Final = available_corporation_id_dict[corporation_id].access_token
            self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: updating corporation_id: {corporation_id} with character_id: {character_id}")

            # XXX: fixme
            # need structures first, because of the db relationships .. urgh
            await self.run_structures(now, character_id, corporation_id, access_token)
            await self.run_extractions(now, character_id, corporation_id, access_token)

            # refresh_count += 1
            # refresh_wobble: typing.Final = datetime.timedelta(seconds=random.randrange(refresh_interval.total_seconds())) - refresh_interval / 2

            await self.set_refresh_times(corporation_id, character_id, now)

        self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: updates complete")

    async def run(self, client_session: collections.abc.MutableSet):
        tracer: typing.Final = opentelemetry.trace.get_tracer_provider().get_tracer(inspect.currentframe().f_code.co_name)
        while True:
            try:
                with tracer.start_as_current_span(inspect.currentframe().f_code.co_name):
                    await self.run_once(client_session)
            except Exception as ex:
                otel_add_exception(ex)
                self.logger.error(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")
