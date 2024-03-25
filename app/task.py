import abc
import asyncio
import collections.abc
import datetime
import http
import inspect
import logging
import os
import traceback
import typing

import aiohttp
import aiohttp.client_exceptions
import dateutil.parser
import sqlalchemy
import sqlalchemy.engine
import sqlalchemy.exc
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql

from app import AppConstants, AppDatabase, AppESI, AppTables
from support.telemetry import otel, otel_add_exception


class AppTask(metaclass=abc.ABCMeta):

    CONFIGDIR: typing.Final = "CONFIGDIR"
    ENVIRONMENT: typing.Final = "ENVIRONMENT"

    @otel
    def __init__(self, client_session: collections.abc.MutableMapping, esi: AppESI, db: AppDatabase, eventqueue: asyncio.Queue, logger: logging.Logger | None = None):
        self.esi: typing.Final = esi
        self.db: typing.Final = db
        self.eventqueue: typing.Final = eventqueue
        self.logger: typing.Final = logger
        self.name: typing.Final = self.__class__.__name__
        self.configdir: typing.Final = os.path.abspath(client_session.get(self.CONFIGDIR, "."))

        self.task: asyncio.Task | None = None
        if client_session.get(self.name, False):
            self.task = None
        else:
            client_session[self.name] = True
            self.task = asyncio.create_task(self.manage_task(client_session), name=self.__class__.__name__)

    async def manage_task(self, client_session: collections.abc.MutableMapping):
        try:
            await self.run(client_session)
        except asyncio.CancelledError as ex:
            otel_add_exception(ex)
            self.logger.error(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex=}")
        client_session[self.name] = False

    @abc.abstractmethod
    async def run(self, client_session: collections.abc.MutableMapping):
        pass

    @property
    def request_params(self) -> dict:
        return {
            "datasource": "tranquility",
            "language": "en",
        }


class AppDatabaseTask(AppTask):

    @otel
    async def _get_type(self, type_id: int, http_session: aiohttp.ClientSession) -> None | AppTables.UniverseType:
        url: typing.Final = f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/universe/types/{type_id}/"
        esi_result = await self.esi.get(http_session, url, request_params=self.request_params)
        if esi_result.status in [http.HTTPStatus.OK, http.HTTPStatus.NOT_MODIFIED] and esi_result.data is not None:
            edict: typing.Final = dict()
            for k, v in esi_result.data.items():
                if k in ["name"]:
                    edict[k] = v
                elif k in ["mass", "volume"]:
                    edict[k] = float(v)
                elif k in ["group_id", "icon_id", "market_group_id", "type_id"]:
                    edict[k] = int(v)
                else:
                    continue
            return AppTables.UniverseType(**edict)
        return None

    @otel
    async def _get_character(self, character_id: int, http_session: aiohttp.ClientSession) -> None | AppTables.Corporation:
        url: typing.Final = f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/characters/{character_id}/"
        esi_result = await self.esi.get(http_session, url, request_params=self.request_params)
        if esi_result.status in [http.HTTPStatus.OK, http.HTTPStatus.NOT_MODIFIED] and esi_result.data is not None:
            edict: typing.Final = dict({
                "character_id": character_id
            })
            for k, v in esi_result.data.items():
                if k in ["alliance_id", "corporation_id"]:
                    edict[k] = int(v)
                elif k in ["birthday"]:
                    edict[k] = dateutil.parser.parse(v).replace(tzinfo=datetime.UTC)
                elif k not in ["name"]:
                    continue
                edict[k] = v

            conversions: typing.Final = {
                'birthday': lambda x: dateutil.parser.parse(str(x)).replace(tzinfo=datetime.UTC)
            }
            for k, v in conversions.items():
                if k in edict.keys():
                    edict[k] = v(edict[k])

            return AppTables.Character(**edict)
        return None

    @otel
    async def _get_corporation(self, corporation_id: int, http_session: aiohttp.ClientSession) -> None | AppTables.Corporation:
        url: typing.Final = f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/corporations/{corporation_id}/"
        esi_result = await self.esi.get(http_session, url, request_params=self.request_params)
        if esi_result.status in [http.HTTPStatus.OK, http.HTTPStatus.NOT_MODIFIED] and esi_result.data is not None:
            edict: typing.Final = dict({
                "corporation_id": corporation_id
            })
            for k, v in esi_result.data.items():
                if k in ["alliance_id"]:
                    edict[k] = int(v)
                elif k not in ["name", "ticker"]:
                    continue
                edict[k] = v
            return AppTables.Corporation(**edict)
        return None

    @otel
    async def _get_moon(self, moon_id: int, http_session: aiohttp.ClientSession) -> None | AppTables.UniverseMoon:
        url: typing.Final = f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/universe/moons/{moon_id}/"
        esi_result = await self.esi.get(http_session, url, request_params=self.request_params)
        if esi_result.status in [http.HTTPStatus.OK, http.HTTPStatus.NOT_MODIFIED] and esi_result.data is not None:
            edict: typing.Final = dict()
            for k, v in esi_result.data.items():
                if k in ["name"]:
                    edict[k] = v
                elif k in ["moon_id", "system_id"]:
                    edict[k] = int(v)
                else:
                    continue
            return AppTables.UniverseMoon(**edict)
        return None

    @otel
    async def backfill_types(self, type_id_set: set) -> None:
        if not len(type_id_set) > 0:
            return

        try:
            async with await self.db.sessionmaker() as session:
                session: sqlalchemy.ext.asyncio.AsyncSession

                existing_type_id_set: typing.Final = set()

                if len(type_id_set) == 1:
                    type_id = list(type_id_set)[0]
                    query = (
                        sqlalchemy.select(AppTables.UniverseType.type_id)
                        .where(AppTables.UniverseType.type_id == type_id)
                        .limit(1)
                    )
                    query_result: sqlalchemy.engine.Result = await session.execute(query)
                    if query_result.scalar_one_or_none() is not None:
                        existing_type_id_set.add(type_id)
                else:
                    query = sqlalchemy.select(AppTables.UniverseType.type_id)
                    async for x in await session.stream_scalars(query):
                        existing_type_id_set.add(x)

                missing_type_id_set = type_id_set - existing_type_id_set
                if len(missing_type_id_set) > 0:
                    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=AppConstants.ESI_LIMIT_PER_HOST)) as http_session:
                        type_task_list: typing.Final = list()
                        for id in missing_type_id_set:
                            type_task_list.append(self._get_type(id, http_session))

                        if len(type_task_list) > 0:
                            task_result_list = await asyncio.gather(*type_task_list)
                            session.begin()
                            session.add_all([obj for obj in task_result_list if obj])
                            await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex=}")

    @otel
    async def backfill_moons(self, moon_id_set: set) -> None:
        if not len(moon_id_set) > 0:
            return

        try:
            async with await self.db.sessionmaker() as session:
                session: sqlalchemy.ext.asyncio.AsyncSession

                existing_moon_id_set: typing.Final = set()

                if len(moon_id_set) == 1:
                    moon_id = list(moon_id_set)[0]
                    query = (
                        sqlalchemy.select(AppTables.UniverseMoon.moon_id)
                        .where(AppTables.UniverseMoon.moon_id == moon_id)
                        .limit(1)
                    )
                    query_result: sqlalchemy.engine.Result = await session.execute(query)
                    if query_result.scalar_one_or_none() is not None:
                        existing_moon_id_set.add(moon_id)
                else:
                    query = sqlalchemy.select(AppTables.UniverseMoon.moon_id)
                    async for x in await session.stream_scalars(query):
                        existing_moon_id_set.add(x)

                missing_moon_id_set = moon_id_set - existing_moon_id_set
                if len(missing_moon_id_set) > 0:
                    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=AppConstants.ESI_LIMIT_PER_HOST)) as http_session:
                        task_list: typing.Final = list()
                        for id in missing_moon_id_set:
                            task_list.append(self._get_moon(id, http_session))

                        if len(task_list) > 0:
                            task_result_list = await asyncio.gather(*task_list)
                            session.begin()
                            session.add_all([obj for obj in task_result_list if obj])
                            await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex=}")

    @otel
    async def backfill_characters(self, character_id_set: set) -> None:
        if not len(character_id_set) > 0:
            return

        try:
            async with await self.db.sessionmaker() as session:
                session: sqlalchemy.ext.asyncio.AsyncSession

                existing_character_id_set: typing.Final = set()

                if len(character_id_set) == 1:
                    character_id = list(character_id_set)[0]
                    query = (
                        sqlalchemy.select(AppTables.Character.character_id)
                        .where(AppTables.Character.character_id == character_id)
                        .limit(1)
                    )
                    query_result: sqlalchemy.engine.Result = await session.execute(query)
                    if query_result.scalar_one_or_none() is not None:
                        existing_character_id_set.add(character_id)
                else:
                    query = sqlalchemy.select(AppTables.Character.character_id)
                    async for x in await session.stream_scalars(query):
                        existing_character_id_set.add(x)

                missing_character_id_set = character_id_set - existing_character_id_set
                if len(missing_character_id_set) > 0:
                    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=AppConstants.ESI_LIMIT_PER_HOST)) as http_session:
                        character_task_list: typing.Final = list()
                        for id in character_id_set - existing_character_id_set:
                            character_task_list.append(self._get_character(id, http_session))

                        if len(character_task_list) > 0:
                            task_result_list = await asyncio.gather(*character_task_list)
                            session.begin()
                            session.add_all([obj for obj in task_result_list if obj])
                            await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex=}")
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {traceback.format_exc()=}")

    @otel
    async def backfill_corporations(self, corporation_id_set: set) -> None:
        if not len(corporation_id_set) > 0:
            return

        try:
            async with await self.db.sessionmaker() as session:
                session: sqlalchemy.ext.asyncio.AsyncSession

                existing_corporation_id_set: typing.Final = set()

                if len(corporation_id_set) == 1:
                    corporation_id = list(corporation_id_set)[0]
                    query = (
                        sqlalchemy.select(AppTables.Corporation.corporation_id)
                        .where(AppTables.Corporation.corporation_id == corporation_id)
                        .limit(1)
                    )
                    query_result: sqlalchemy.engine.Result = await session.execute(query)
                    if query_result.scalar_one_or_none() is not None:
                        existing_corporation_id_set.add(corporation_id)
                else:
                    query = sqlalchemy.select(AppTables.Corporation.corporation_id)
                    async for x in await session.stream_scalars(query):
                        existing_corporation_id_set.add(x)

                missing_corporation_id_set = corporation_id_set - existing_corporation_id_set
                if len(missing_corporation_id_set) > 0:
                    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=AppConstants.ESI_LIMIT_PER_HOST)) as http_session:
                        corporation_task_list: typing.Final = list()
                        for id in corporation_id_set - existing_corporation_id_set:
                            corporation_task_list.append(self._get_corporation(id, http_session))

                        if len(corporation_task_list) > 0:
                            task_result_list = await asyncio.gather(*corporation_task_list)
                            session.begin()
                            session.add_all([obj for obj in task_result_list if obj])
                            await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex=}")
