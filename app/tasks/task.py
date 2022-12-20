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

from .. import AppDatabase, AppTables


class AppTask(metaclass=abc.ABCMeta):

    LIMIT_PER_HOST: typing.Final = 13
    ERROR_SLEEP_TIME: typing.Final = 7
    ERROR_RETRY_COUNT: typing.Final = 11

    # ESI throws off a 504 at daily restart, so let's double the retry
    # waiting period for those.
    ERROR_SLEEP_MODIFIERS: typing.Final = {
        504: 2
    }

    CONFIGDIR: typing.Final = "CONFIGDIR"

    @otel
    def __init__(self, client_session: collections.abc.MutableMapping, db: AppDatabase, outbound: asyncio.Queue, logger: logging.Logger | None = None):
        self.db: typing.Final = db
        self.outbound: typing.Final = outbound
        self.logger: typing.Final = logger or logging.getLogger(self.__class__.__name__)
        self.name: typing.Final = self.__class__.__name__
        self.configdir: typing.Final = os.path.abspath(client_session.get(self.CONFIGDIR, "."))

        if client_session.get(self.name, False):
            self.task: asyncio.Task = None
        else:
            client_session[self.name] = True
            self.task: asyncio.Task = asyncio.create_task(self.manage_task(client_session), name=self.__class__.__name__)

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

            attempts_remaining = self.ERROR_RETRY_COUNT
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
                        if response.status in [403]:
                            break
                        if attempts_remaining > 0:
                            await asyncio.sleep(self.ERROR_SLEEP_TIME * self.ERROR_SLEEP_MODIFIERS.get(response.status, 1))

            if results is not None:
                pages = list(range(2, 1 + int(maxpageno)))

                task_list: typing.Final = [self._get_url(http_session, url, {"page": x}) for x in pages]
                if len(task_list) > 0:
                    results.extend(sum(await asyncio.gather(*task_list), []))

            return results

        return None

    @otel
    async def _get_url(self, http_session: aiohttp.ClientSession, url: str, request_params: dict | None = None) -> list | None:

        request_params = request_params or dict()
        attempts_remaining = self.ERROR_RETRY_COUNT
        # self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {url} {str(request_params)}")
        while attempts_remaining > 0:
            async with await http_session.get(url, params=self.common_params | request_params) as response:
                if response.status in [200]:
                    return await response.json()
                else:
                    attempts_remaining -= 1
                    otel_add_error(f"{response.url} -> {response.status}")
                    self.logger.warning(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {response.url} -> {response.status}")
                    if response.status in [403]:
                        return None
                    if attempts_remaining > 0:
                        await asyncio.sleep(self.ERROR_SLEEP_TIME * self.ERROR_SLEEP_MODIFIERS.get(response.status, 1))

        return None





class AppDatabaseTask(AppTask):


    @otel
    async def _get_type(self, type_id: int, http_session: aiohttp.ClientSession) -> None | AppTables.UniverseType:
        url: typing.Final = f"https://esi.evetech.net/latest/universe/types/{type_id}/"
        rdict: typing.Final = await self._get_url(http_session, url)
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
        url: typing.Final = f"https://esi.evetech.net/latest/corporations/{corporation_id}/"
        rdict: typing.Final = await self._get_url(http_session, url)
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
        url: typing.Final = f"https://esi.evetech.net/latest/universe/moons/{moon_id}/"
        rdict: typing.Final = await self._get_url(http_session, url)
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

                query = sqlalchemy.select(AppTables.UniverseType.type_id)
                async for x in await session.stream_scalars(query):
                    existing_type_id_set.add(x)

                missing_type_id_set = type_id_set - existing_type_id_set
                if len(missing_type_id_set) > 0:
                    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=self.LIMIT_PER_HOST)) as http_session:
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

                query = sqlalchemy.select(AppTables.UniverseMoon.moon_id)
                async for x in await session.stream_scalars(query):
                    existing_moon_id_set.add(x)

                missing_moon_id_set = moon_id_set - existing_moon_id_set
                if len(missing_moon_id_set) > 0:
                    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=self.LIMIT_PER_HOST)) as http_session:
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

                query = sqlalchemy.select(AppTables.Corporation.corporation_id)
                async for x in await session.stream_scalars(query):
                    existing_corporation_id_set.add(x)

                missing_corporation_id_set = corporation_id_set - existing_corporation_id_set
                if len(missing_corporation_id_set) > 0:
                    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=self.LIMIT_PER_HOST)) as http_session:
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
