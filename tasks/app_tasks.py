import asyncio
import collections
import collections.abc
import datetime
import inspect
import logging
import random
import typing
import zoneinfo

import aiohttp
import aiohttp.client_exceptions
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
from telemetry import otel, otel_add_error, otel_add_exception

from .task import EveTask


class EveCommonState:


    def __init__(self, db: EveDatabase, logger: logging.Logger = logging.getLogger()) -> None:
        self.db: typing.Final = db
        self.logger: typing.Final = logger
        self.name: typing.Final = self.__class__.__name__


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

                # # Remove the exists=False structures
                # query = (
                #     sqlalchemy.select(sqlalchemy.distinct(EveTables.StructureHistory.structure_id))
                #     .where(
                #         sqlalchemy.and_(
                #             EveTables.StructureHistory.corporation_id == corporation_id,
                #             EveTables.StructureHistory.exists == sqlalchemy.sql.expression.false()
                #         )
                #     )
                # )
                # result: sqlalchemy.engine.Result = await session.execute(query)
                # for x in result.scalars():
                #     if x in corporation_structure_ids:
                #         corporation_structure_ids.discard(x)

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


    async def set(self, structure_id: int, edict: dict, exists: bool) -> None:
        try:
            structure_changed = exists and edict
            structure_obj = await self.get(structure_id)
            if structure_obj:
                structure_changed = False
                if not exists:

                    if structure_obj.exists:
                        structure_changed = True
                        edict = {x: getattr(structure_obj, x) for x in structure_obj.__table__.columns.keys() if x not in ['id', 'exists', 'timestamp']}

                else:

                    if edict:
                        for k, v in edict.items():
                            if hasattr(structure_obj, k):
                                if getattr(structure_obj, k) != v:
                                    structure_changed = True
                                    break

            if structure_changed and edict:
                structure_obj = EveTables.StructureHistory(exists=exists, **edict)
                async with await self.db.sessionmaker() as session, session.begin():
                    session.add(structure_obj)
                    await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")


class EveSExtractionState(EveCommonState):


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

                # # Remove the exists=False structures
                # query = (
                #     sqlalchemy.select(sqlalchemy.distinct(EveTables.ExtractionHistory.structure_id))
                #     .where(
                #         sqlalchemy.and_(
                #             EveTables.ExtractionHistory.corporation_id == corporation_id,
                #             EveTables.ExtractionHistory.exists == sqlalchemy.sql.expression.false()
                #         )
                #     )
                # )
                # result: sqlalchemy.engine.Result = await session.execute(query)
                # for x in result.scalars():
                #     if x in corporation_structure_ids:
                #         corporation_structure_ids.discard(x)

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


    async def set(self, structure_id: int, edict: dict, exists: bool) -> None:
        try:
            structure_changed = exists and edict
            structure_obj = await self.get(structure_id)
            if structure_obj:
                structure_changed = False
                if not exists:

                    if structure_obj.exists:
                        structure_changed = True
                        edict = {x: getattr(structure_obj, x) for x in structure_obj.__table__.columns.keys() if x not in ['id', 'exists', 'timestamp']}

                else:

                    if edict:
                        for k, v in edict.items():
                            if hasattr(structure_obj, k):
                                if getattr(structure_obj, k) != v:
                                    structure_changed = True
                                    break

            if structure_changed and edict:
                structure_obj = EveTables.ExtractionHistory(exists=exists, **edict)
                async with await self.db.sessionmaker() as session, session.begin():
                    session.add(structure_obj)
                    await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")


class EveStructureTask(EveTask):

    @otel
    def __init__(self, client_session: collections.abc.MutableMapping, db: EveDatabase, logger: logging.Logger = logging.getLogger()):
        super().__init__(client_session, db, logger)
        self.structure_state: typing.Final = EveStructureState(db, logger)
        self.extraction_state: typing.Final = EveSExtractionState(db, logger)

    @otel
    async def _get_url(self, url: str, http_session: aiohttp.ClientSession) -> dict | None:
        attempts_remaining = self.ERROR_RETRY_COUNT
        while attempts_remaining > 0:
            async with await http_session.get(url, params=self.common_params) as response:
                if response.status in [200]:
                    return dict(await response.json())
                else:
                    attempts_remaining -= 1
                    otel_add_error(f"{response.url} -> {response.status}")
                    self.logger.warning("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"{response.url} -> {response.status}"))
                    await asyncio.sleep(self.ERROR_SLEEP_TIME)
        self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {url}")
        return None

    @otel
    async def _get_type(self, type_id: int, http_session: aiohttp.ClientSession) -> None | EveTables.UniverseType:
        url: typing.Final = f"https://esi.evetech.net/latest/universe/types/{type_id}/"
        rdict: typing.Final = await self._get_url(url, http_session)
        if rdict is not None:
            edict: typing.Final = dict()
            for k, v in rdict.items():
                if k in ["name"]:
                    edict[k] = v
                elif k in ["type_id", "group_id", "market_group_id"]:
                    edict[k] = int(v)
                else:
                    continue
            return EveTables.UniverseType(**edict)
        return None

    @otel
    async def _get_corporation(self, corporation_id: int, http_session: aiohttp.ClientSession) -> None | EveTables.Corporation:
        url: typing.Final = f"https://esi.evetech.net/latest/corporations/{corporation_id}/"
        rdict: typing.Final = await self._get_url(url, http_session)
        if rdict is not None:
            edict: typing.Final = dict({
                "corporation_id": corporation_id
            })
            for k, v in rdict.items():
                if k in ["alliance_id"]:
                    edict[k] = int(v)
                elif k not in ["name", "ticker"]:
                    continue
                edict[k] = v
            return EveTables.Corporation(**edict)
        return None

    @otel
    async def _get_moon(self, moon_id: int, http_session: aiohttp.ClientSession) -> None | EveTables.UniverseMoon:
        url: typing.Final = f"https://esi.evetech.net/latest/universe/moons/{moon_id}/"
        rdict: typing.Final = await self._get_url(url, http_session)
        if rdict is not None:
            edict: typing.Final = dict()
            for k, v in rdict.items():
                if k in ["name"]:
                    edict[k] = v
                elif k in ["moon_id", "system_id"]:
                    edict[k] = int(v)
                else:
                    continue
            return EveTables.UniverseMoon(**edict)
        return None

    @otel
    async def backfill_types(self, type_id_set: set) -> None:
        try:
            async with await self.db.sessionmaker() as session, session.begin():

                existing_type_query = sqlalchemy.select(EveTables.UniverseType)
                existing_type_query_result: typing.Final[sqlalchemy.engine.Result] = await session.execute(existing_type_query)
                existing_type_id_set: typing.Final = {x.type_id for x in existing_type_query_result.scalars()}

                type_obj_set: typing.Final = set()
                async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=self.LIMIT_PER_HOST)) as http_session:
                    type_task_list: typing.Final = list()
                    for id in type_id_set - existing_type_id_set:
                        type_task_list.append(asyncio.ensure_future(self._get_type(id, http_session)))

                    if len(type_task_list) > 0:
                        result_list = await asyncio.gather(*type_task_list)
                        for obj in [obj for obj in result_list if obj]:
                            type_obj_set.add(obj)

                if len(type_obj_set) > 0:
                    session.add_all(type_obj_set)
                    await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

    @otel
    async def backfill_moons(self, moon_id_set: set) -> None:
        try:
            async with await self.db.sessionmaker() as session, session.begin():

                existing_moon_query = sqlalchemy.select(EveTables.UniverseMoon)
                existing_moon_query_result: typing.Final[sqlalchemy.engine.Result] = await session.execute(existing_moon_query)
                existing_moon_id_set: typing.Final = {x.moon_id for x in existing_moon_query_result.scalars()}

                moon_obj_set: typing.Final = set()
                async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=self.LIMIT_PER_HOST)) as http_session:
                    task_list: typing.Final = list()
                    for id in moon_id_set - existing_moon_id_set:
                        task_list.append(asyncio.ensure_future(self._get_moon(id, http_session)))

                    if len(task_list) > 0:
                        result_list = await asyncio.gather(*task_list)
                        for obj in [obj for obj in result_list if obj]:
                            moon_obj_set.add(obj)

                if len(moon_obj_set) > 0:
                    session.add_all(moon_obj_set)
                    await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

    @otel
    async def backfill_corporations(self, corporation_id_set: set) -> None:
        try:
            async with await self.db.sessionmaker() as session, session.begin():

                existing_corporation_query = sqlalchemy.select(EveTables.Corporation)
                existing_corporation_query_result: typing.Final[sqlalchemy.engine.Result] = await session.execute(existing_corporation_query)
                existing_corporation_id_set: typing.Final = {x.corporation_id for x in existing_corporation_query_result.scalars()}

                corporation_obj_set: typing.Final = set()
                async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=self.LIMIT_PER_HOST)) as http_session:
                    corporation_task_list: typing.Final = list()
                    for id in corporation_id_set - existing_corporation_id_set:
                        corporation_task_list.append(asyncio.ensure_future(self._get_corporation(id, http_session)))

                    if len(corporation_task_list) > 0:
                        result_list = await asyncio.gather(*corporation_task_list)
                        for obj in [obj for obj in result_list if obj]:
                            corporation_obj_set.add(obj)

                if len(corporation_obj_set) > 0:
                    session.add_all(corporation_obj_set)
                    await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")


    @otel
    async def run_structures(self, now: datetime.datetime, character_id: int, corporation_id: int, access_token: str):

        prior_structure_id_set: typing.Final = await self.structure_state.structure_ids(corporation_id)

        url = f"https://esi.evetech.net/latest/corporations/{corporation_id}/structures/"
        structures: typing.Final = await self.get_pages(url, access_token)

        # Add the structures to the query log
        try:
            async with await self.db.sessionmaker() as session, session.begin():
                session.add(EveTables.StructurQueryLog(corporation_id=corporation_id, character_id=character_id, json=structures))
                await session.commit()
        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        if len(structures) == 0:
            for structure_id in prior_structure_id_set:
                await self.structure_state.set(structure_id, None, exists=False)
            self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: corporation_id: {corporation_id}, structures: {structures}")
            return


        backfill_type_id_set: typing.Final = set()
        backfill_corporation_id_set: typing.Final = set({corporation_id})

        structure_id_set: typing.Final = set()
        structure_obj_set: typing.Final = set()

        for x in structures:
            edict: typing.Final = dict({
                "character_id": character_id,
            })
            for k, v in x.items():
                if k in ["fuel_expires", "state_timer_end", "state_timer_start", "unanchors_at"]:
                    v = dateutil.parser.parse(v).replace(tzinfo=zoneinfo.ZoneInfo("UTC"))
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
            structure_obj_set.add(obj)
            backfill_type_id_set.add(obj.type_id)
            backfill_corporation_id_set.add(obj.corporation_id)
            structure_id_set.add(obj.structure_id)

            await self.structure_state.set(obj.structure_id, edict, exists=True)

        # Remove deleted
        for structure_id in set(prior_structure_id_set) - set(structure_id_set):
            await self.structure_state.set(structure_id, None, exists=False)

        # self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: corporation_id_set: {sorted(list(corporation_id_set))}")
        # self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: structure_id_set: {sorted(list(structure_id_set))}")

        # Add missing types
        if len(backfill_type_id_set) > 0:
            await self.backfill_types(backfill_type_id_set)

        # Add missing corporations
        if len(backfill_corporation_id_set) > 0:
            await self.backfill_corporations(backfill_corporation_id_set)

        # Ugly hack to remove structures that no longer exist. If I was better with the ORM I would not have to do this ..
        try:
            async with await self.db.sessionmaker() as session, session.begin():

                completed_extraction_query: typing.Final = sqlalchemy.select(EveTables.CompletedExtraction).where(
                    EveTables.CompletedExtraction.corporation_id == corporation_id,
                    EveTables.CompletedExtraction.structure_id.not_in(structure_id_set)
                )
                completed_extraction_query_result: typing.Final[sqlalchemy.engine.Result] = await session.execute(completed_extraction_query)
                completed_extraction_obj_list: typing.Final[list[EveTables.CompletedExtraction]] = [x for x in completed_extraction_query_result.scalars()]

                if len(completed_extraction_obj_list) > 0:
                    # self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: completed_extraction_obj_list: {sorted(list(map(lambda x: x.structure_id, completed_extraction_obj_list)))}")
                    [await session.delete(x) for x in completed_extraction_obj_list]
                    await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"{inspect.currentframe().f_code.co_name}: {ex}")

        try:
            async with await self.db.sessionmaker() as session, session.begin():

                scheduled_extraction_query: typing.Final = sqlalchemy.select(EveTables.ScheduledExtraction).where(
                    EveTables.ScheduledExtraction.corporation_id == corporation_id,
                    EveTables.ScheduledExtraction.structure_id.not_in(structure_id_set)
                )
                scheduled_extraction_query_result: typing.Final[sqlalchemy.engine.Result] = await session.execute(scheduled_extraction_query)
                scheduled_extraction_obj_list: typing.Final[list[EveTables.ScheduledExtraction]] = [x for x in scheduled_extraction_query_result.scalars()]

                if len(scheduled_extraction_obj_list) > 0:
                    # self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: scheduled_extraction_obj_list: {sorted(list(map(lambda x: x.structure_id, scheduled_extraction_obj_list)))}")
                    [await session.delete(x) for x in scheduled_extraction_obj_list]
                    await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"{inspect.currentframe().f_code.co_name}: {ex}")

        try:
            async with await self.db.sessionmaker() as session, session.begin():

                all_structures_query: typing.Final = sqlalchemy.select(EveTables.Structure).where(
                    EveTables.Structure.corporation_id == corporation_id
                )
                all_structures_query_result: typing.Final[sqlalchemy.engine.Result] = await session.execute(all_structures_query)
                existing_obj_list: typing.Final[list[EveTables.Structure]] = [x for x in all_structures_query_result.scalars()]

                if len(existing_obj_list) > 0:
                    # self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: existing_obj_list: {sorted(list(map(lambda x: x.structure_id, existing_obj_list)))}")
                    [await session.delete(x) for x in existing_obj_list]

                if len(structure_obj_set) > 0:
                    session.add_all(structure_obj_set)

                await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"{inspect.currentframe().f_code.co_name}: {ex}")

    @otel
    async def run_extractions(self, now: datetime.datetime, character_id: int, corporation_id: int, access_token: str):

        # prior_structure_id_set: typing.Final = await self.extraction_state.structure_ids(corporation_id)

        url = f"https://esi.evetech.net/latest/corporation/{corporation_id}/mining/extractions/"
        extractions: typing.Final = await self.get_pages(url, access_token)

        # Add the extractions to the query log
        try:
            async with await self.db.sessionmaker() as session, session.begin():
                session.add(EveTables.ExtractionQueryLog(corporation_id=corporation_id, character_id=character_id, json=extractions))
                await session.commit()
        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        if len(extractions) == 0:
            # for structure_id in prior_structure_id_set:
            #     await self.extraction_state.set(structure_id, None, exists=False)
            self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: corporation_id: {corporation_id}, extractions: {extractions}")
            return


        moon_id_set: typing.Final = set()
        structure_id_set: typing.Final = set()
        corporation_id_set: typing.Final = set()
        fresh_extractions_obj_dict: typing.Final = dict()

        for x in extractions:
            edict = dict({
                "character_id": character_id,
                "corporation_id": corporation_id,
            })
            for k, v in x.items():
                if k in ["chunk_arrival_time", "extraction_start_time", "natural_decay_time"]:
                    v = dateutil.parser.parse(v).replace(tzinfo=zoneinfo.ZoneInfo("UTC"))
                elif k in ["structure_id", "moon_id"]:
                    v = int(v)
                else:
                    continue
                edict[k] = v

            obj = EveTables.ScheduledExtraction(**edict)
            fresh_extractions_obj_dict[obj.structure_id] = obj
            moon_id_set.add(obj.moon_id)
            structure_id_set.add(obj.structure_id)
            corporation_id_set.add(obj.corporation_id)

            await self.extraction_state.set(obj.structure_id, edict, exists=True)

        # # Remove deleted
        # for structure_id in set(prior_structure_id_set) - set(structure_id_set):
        #     await self.extraction_state.set(structure_id, None, exists=False)

        # Add missing moons
        await self.backfill_moons(moon_id_set)

        # Add missing corporations
        await self.backfill_corporations(corporation_id_set)

        # Update Scheduled and Completed
        if len(fresh_extractions_obj_dict.keys()) > 0:
            try:
                async with await self.db.sessionmaker() as session, session.begin():

                    completed_extractions_query: typing.Final = sqlalchemy.select(EveTables.CompletedExtraction).where(EveTables.CompletedExtraction.corporation_id == corporation_id)
                    completed_extractions_query_result: typing.Final[sqlalchemy.engine.Result] = await session.execute(completed_extractions_query)
                    completed_extractions_dict: typing.Final = {x.structure_id: x for x in completed_extractions_query_result.scalars()}

                    existing_extractions_query: typing.Final = sqlalchemy.select(EveTables.ScheduledExtraction).where(EveTables.ScheduledExtraction.corporation_id == corporation_id)
                    existing_extractions_query_result: typing.Final[sqlalchemy.engine.Result] = await session.execute(existing_extractions_query)
                    existing_extractions_obj_dict: typing.Final = {x.structure_id: x for x in existing_extractions_query_result.scalars()}

                    for structure_id in set(fresh_extractions_obj_dict.keys()).intersection(set(existing_extractions_obj_dict.keys())):

                        existing_extraction: EveTables.ScheduledExtraction = existing_extractions_obj_dict[structure_id]
                        if now >= existing_extraction.chunk_arrival_time:

                            # Remove the old / existing completed extraction
                            if structure_id in completed_extractions_dict.keys():
                                await session.delete(completed_extractions_dict[structure_id])

                            belt_lifetime_estimate: typing.Final = datetime.timedelta(days=2)

                            modifier_query: typing.Final = sqlalchemy.select(EveTables.StructureModifiers).where(EveTables.StructureModifiers.structure_id == structure_id).limit(1)
                            modifier_query_result: typing.Final[sqlalchemy.engine.Result] = await session.execute(modifier_query)
                            modifier_obj_dct: typing.Final[dict[int, EveTables.StructureModifiers]] = {x.structure_id: x for x in modifier_query_result.scalars()}

                            modifier_obj = modifier_obj_dct.get(structure_id)
                            if modifier_obj is not None:
                                belt_lifetime_estimate *= float(modifier_obj.belt_lifetime_modifier)

                            completed_extraction = EveTables.CompletedExtraction(
                                character_id=existing_extraction.character_id,
                                corporation_id=existing_extraction.corporation_id,
                                structure_id=existing_extraction.structure_id,
                                moon_id=existing_extraction.moon_id,
                                extraction_start_time=existing_extraction.extraction_start_time,
                                chunk_arrival_time=existing_extraction.chunk_arrival_time,
                                natural_decay_time=existing_extraction.natural_decay_time,
                                belt_decay_time=existing_extraction.chunk_arrival_time + belt_lifetime_estimate)

                            session.add(completed_extraction)

                    # We wipe and re-insert to handle the case where a structure goes away between taaks
                    if len(existing_extractions_obj_dict.keys()) > 0:
                        [await session.delete(x) for x in existing_extractions_obj_dict.values()]

                    session.add_all(fresh_extractions_obj_dict.values())

                    await session.commit()

            except Exception as ex:
                otel_add_exception(ex)
                self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

    @otel
    async def run(self, client_session: collections.abc.MutableMapping):
        if not client_session.get(EveSSO.ESI_CHARACTER_HAS_STATION_MANAGER_ROLE, False):
            return

        character_id: typing.Final = int(client_session.get(EveSSO.ESI_CHARACTER_ID, 0))
        corporation_id: typing.Final = int(client_session.get(EveSSO.ESI_CORPORATION_ID, 0))
        access_token: typing.Final = client_session.get(EveSSO.ESI_ACCESS_TOKEN, '')

        now: typing.Final = datetime.datetime.now(tz=datetime.timezone.utc)

        if "esi-corporations.read_structures.v1" in client_session.get(EveSSO.ESI_ACCESS_TOKEN_SCOPES, []):
            await self.run_structures(now, character_id, corporation_id, access_token)

        if "esi-industry.read_corporation_mining.v1" in client_session.get(EveSSO.ESI_ACCESS_TOKEN_SCOPES, []):
            await self.run_extractions(now, character_id, corporation_id, access_token)


class EveStructurePollingTask(EveStructureTask):

    @otel
    async def run_once(self, client_session: collections.abc.MutableSet):
        # refresh_buffer: typing.Final = datetime.timedelta(seconds=30)
        refresh_buffer: typing.Final = datetime.timedelta(seconds=20)
        refresh_interval: typing.Final = datetime.timedelta(seconds=300) - refresh_buffer

        now: typing.Final = datetime.datetime.now(tz=datetime.timezone.utc)

        available_corporation_id_dict: typing.Final[dict[int, EveTables.PeriodicCredentials]] = dict()
        try:
            async with await self.db.sessionmaker() as session:

                query = (
                    sqlalchemy.select(EveTables.PeriodicCredentials)
                    .where(EveTables.PeriodicCredentials.is_permitted.is_(True))
                    .where(EveTables.PeriodicCredentials.access_token_exiry > now)
                    .order_by(sqlalchemy.asc(EveTables.PeriodicCredentials.access_token_exiry))
                )
                results: sqlalchemy.engine.Result = await session.execute(query)
                rl: typing.Final = [x for x in results.scalars()]

                for obj in rl:
                    if isinstance(obj, EveTables.PeriodicCredentials):
                        if not obj.is_station_manager_role:
                            return

                        if not available_corporation_id_dict.get(obj.corporation_id) is None:
                            return

                        available_corporation_id_dict[obj.corporation_id] = obj

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"{inspect.currentframe().f_code.co_name}: {ex}")

        # self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: available_corporation_id_dict: {sorted(available_corporation_id_dict.keys())}")

        # If there are no corporations that can be refreshed, sleep and return
        if len(available_corporation_id_dict.keys()) == 0:
            await asyncio.sleep(refresh_buffer.total_seconds())
            return

        # Throw this in for now, need something cleaner.
        refresh_history_dict: typing.Final = {x: datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc) for x in available_corporation_id_dict.keys()}
        try:
            async with await self.db.sessionmaker() as session:

                query = (
                    sqlalchemy.select(EveTables.PeriodicTaskTimestamp)
                    .where(EveTables.PeriodicTaskTimestamp.corporation_id.in_(available_corporation_id_dict.keys()))
                )
                results: sqlalchemy.engine.Result = await session.execute(query)
                obj_list = [x for x in results.scalars()]

                if len(obj_list) > 0:
                    for x in obj_list:
                        if isinstance(x, EveTables.PeriodicTaskTimestamp):
                            refresh_history_dict[x.corporation_id] = x.timestamp

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"{inspect.currentframe().f_code.co_name}: {ex}")

        # self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: refresh_history_dict: {str(refresh_history_dict)}")

        oldest_corporation_id = sorted(refresh_history_dict.keys(), key=lambda x: refresh_history_dict[x])[0]
        oldest_timestamp = refresh_history_dict[oldest_corporation_id]

        # self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: oldest_corporation_id: {oldest_corporation_id}, oldest_timestamp: {oldest_timestamp}")

        if oldest_timestamp + refresh_interval > now:
            remaining_interval: typing.Final[datetime.timedelta] = oldest_timestamp + refresh_interval + refresh_buffer - now
            # otel_add_event(inspect.currentframe().f_code.co_name, {"remaining_interval": remaining_interval.total_seconds(), "refresh_corporation_id": oldest_corporation_id})
            remaining_sleep_interval: typing.Final = min(refresh_interval.total_seconds(), remaining_interval.total_seconds())
            # self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: remaining_interval: {remaining_interval.total_seconds()}, remaining_sleep_interval: {remaining_sleep_interval}")
            await asyncio.sleep(remaining_sleep_interval)
            return

        # Should make these asyncio tasks ...
        refresh_count = 0
        for corporation_id, oldest_timestamp in refresh_history_dict.items():
            if oldest_timestamp + refresh_interval > now:
                continue

            character_id: typing.Final = available_corporation_id_dict[corporation_id].character_id
            access_token: typing.Final = available_corporation_id_dict[corporation_id].access_token

            # otel_add_event(inspect.currentframe().f_code.co_name, {"character_id": character_id, "corporation_id": corporation_id})
            self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: updating corporation_id: {corporation_id} with character_id: {character_id}")

            # XXX: fixme
            # need structures first, because of the db relationships .. urgh
            await self.run_structures(now, character_id, corporation_id, access_token)
            await self.run_extractions(now, character_id, corporation_id, access_token)

            refresh_count += 1
            refresh_wobble: typing.Final = datetime.timedelta(seconds=random.randrange(refresh_interval.total_seconds())) - refresh_interval / 2
            # self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: refresh_wobble: {refresh_wobble.total_seconds()}")

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
                            if isinstance(x, EveTables.PeriodicTaskTimestamp):
                                x.timestamp = now + refresh_wobble
                                x.character_id = character_id
                    else:
                        session.add(EveTables.PeriodicTaskTimestamp(corporation_id=corporation_id, character_id=character_id))
                    await session.commit()

            except Exception as ex:
                otel_add_exception(ex)
                self.logger.error(f"{inspect.currentframe().f_code.co_name}: {ex}")

        # otel_add_event(inspect.currentframe().f_code.co_name, {"refresh_count": refresh_count})
        # self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: refresh_count {refresh_count}")
        # await asyncio.sleep(int(refresh_buffer.total_seconds()))

    async def run(self, client_session: collections.abc.MutableSet):
        tracer: typing.Final = opentelemetry.trace.get_tracer_provider().get_tracer(inspect.currentframe().f_code.co_name)
        while True:
            with tracer.start_as_current_span(inspect.currentframe().f_code.co_name):
                await self.run_once(client_session)
