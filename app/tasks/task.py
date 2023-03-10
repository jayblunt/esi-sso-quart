import abc
import asyncio
import collections.abc
import inspect
import logging
import os
import typing

import aiohttp
import aiohttp.client_exceptions
import sqlalchemy
import sqlalchemy.engine
import sqlalchemy.exc
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql

from support.telemetry import otel, otel_add_error, otel_add_exception

from .. import AppConstants, AppDatabase, AppESI, AppTables


class AppTask(metaclass=abc.ABCMeta):

    CONFIGDIR: typing.Final = "CONFIGDIR"
    ENVIRONMENT: typing.Final = "ENVIRONMENT"

    @otel
    def __init__(self, client_session: collections.abc.MutableMapping, db: AppDatabase, outbound: asyncio.Queue, logger: logging.Logger | None = None):
        self.db: typing.Final = db
        self.outbound: typing.Final = outbound
        self.logger: typing.Final = logger or logging.getLogger(self.__class__.__name__)
        self.name: typing.Final = self.__class__.__name__
        self.configdir: typing.Final = os.path.abspath(client_session.get(self.CONFIGDIR, "."))

        self.task: asyncio.Task | None = None
        if client_session.get(self.name, False):
            self.task = None
        else:
            client_session[self.name] = True
            self.task = asyncio.create_task(self.manage_task(client_session), name=self.__class__.__name__)

    async def manage_task(self, client_session: collections.abc.MutableMapping):
        await self.run(client_session)
        client_session[self.name] = False

    @abc.abstractmethod
    async def run(self, client_session: collections.abc.MutableMapping):
        pass

    @property
    def common_params(self) -> dict:
        return {
            "datasource": "tranquility",
            "language": "en",
        }

    @otel
    async def get_pages(self, url: str, access_token: str) -> list | None:

        session_headers: typing.Final = dict()
        if len(access_token) > 0:
            session_headers["Authorization"] = f"Bearer {access_token}"

        async with aiohttp.ClientSession(headers=session_headers) as http_session:
            maxpageno: int = 0
            results = None

            attempts_remaining = AppConstants.ESI_ERROR_RETRY_COUNT
            while attempts_remaining > 0:
                async with await http_session.get(url, params=self.common_params) as response:
                    if response.status in [200]:
                        maxpageno = int(response.headers.get('X-Pages', 1))
                        results = list()
                        results.extend(await response.json())
                        break
                    else:
                        attempts_remaining -= 1
                        otel_add_error(f"{response.url} -> {response.status}")
                        self.logger.warning("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"{response.url} -> {response.status}"))
                        if response.status in [400, 403]:
                            attempts_remaining = 0
                        if attempts_remaining > 0:
                            await asyncio.sleep(AppConstants.ESI_ERROR_SLEEP_TIME * AppConstants.ESI_ERROR_SLEEP_MODIFIERS.get(response.status, 1))

            if results is not None:
                pages = list(range(2, 1 + int(maxpageno)))

                task_list: typing.Final = [AppESI.get_url(http_session, url, self.common_params | {"page": x}) for x in pages]
                if len(task_list) > 0:
                    results.extend(sum(await asyncio.gather(*task_list), []))

            return results

        return None


class AppDatabaseTask(AppTask):


    @otel
    async def _get_type(self, type_id: int, http_session: aiohttp.ClientSession) -> None | AppTables.UniverseType:
        url: typing.Final = f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/universe/types/{type_id}/"
        rdict: typing.Final = await AppESI.get_url(http_session, url, self.common_params)
        if rdict is not None:
            edict: typing.Final = dict()
            for k, v in rdict.items():
                if k in ["name"]:
                    edict[k] = v
                elif k in ["type_id", "group_id", "market_group_id"]:
                    edict[k] = int(v)
                else:
                    continue
            return AppTables.UniverseType(**edict)
        return None


    @otel
    async def _get_corporation(self, corporation_id: int, http_session: aiohttp.ClientSession) -> None | AppTables.Corporation:
        url: typing.Final = f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/corporations/{corporation_id}/"
        rdict: typing.Final = await AppESI.get_url(http_session, url, self.common_params)
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
            return AppTables.Corporation(**edict)
        return None


    @otel
    async def _get_moon(self, moon_id: int, http_session: aiohttp.ClientSession) -> None | AppTables.UniverseMoon:
        url: typing.Final = f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/universe/moons/{moon_id}/"
        rdict: typing.Final = await AppESI.get_url(http_session, url, self.common_params)
        if rdict is not None:
            edict: typing.Final = dict()
            for k, v in rdict.items():
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
                            session.begin()
                            result_list = await asyncio.gather(*type_task_list)
                            for obj in [obj for obj in result_list if obj]:
                                session.add(obj)
                            await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")


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
                            session.begin()
                            result_list = await asyncio.gather(*task_list)
                            for obj in [obj for obj in result_list if obj]:
                                session.add(obj)
                            await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")


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
                            session.begin()
                            result_list = await asyncio.gather(*corporation_task_list)
                            for obj in [obj for obj in result_list if obj]:
                                session.add(obj)
                            await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")
