import abc
import asyncio
import collections
import collections.abc
import inspect
import typing

import aiohttp
import aiohttp.client_exceptions
import sqlalchemy
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql

from support.telemetry import otel, otel_add_error, otel_add_exception

from .. import AppConstants, AppESI, AppSSO, AppTables
from .task import AppTask


class ESIBackfillTask(AppTask, metaclass=abc.ABCMeta):

    @property
    @abc.abstractmethod
    def object_class(self) -> typing.Any:
        pass

    def object_valid(self, obj: typing.Any) -> bool:
        return isinstance(obj, self.object_class)

    @abc.abstractmethod
    def object_id(self, obj: typing.Any) -> int:
        pass

    @abc.abstractmethod
    def index_url(self) -> str:
        pass

    @abc.abstractmethod
    def item_url(self, id: int) -> str:
        pass

    @abc.abstractmethod
    def item_dict(self, id: int, input: dict) -> dict:
        pass

    @otel
    async def _get_item_dict(self, id: int, http_session: aiohttp.ClientSession) -> dict | None:
        url: typing.Final = self.item_url(id)
        attempts_remaining = AppConstants.ESI_ERROR_RETRY_COUNT
        while attempts_remaining > 0:
            async with await http_session.get(url, params=self.request_params) as response:
                if response.status in [200]:
                    edict: typing.Final = self.item_dict(id, await response.json())
                    if len(edict) > 0:
                        return edict
                else:
                    attempts_remaining -= 1
                    otel_add_error(f"{response.url} -> {response.status}")
                    self.logger.info("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"{response.url} -> {response.status}"))
                    if response.status in [400, 403]:
                        attempts_remaining = 0
                    if attempts_remaining > 0:
                        await asyncio.sleep(AppConstants.ESI_ERROR_SLEEP_TIME * AppConstants.ESI_ERROR_SLEEP_MODIFIERS.get(response.status, 1))

        self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {id} -> {None}")
        return None

    @otel
    async def run(self, client_session: collections.abc.MutableMapping):

        self.logger.info(f"> {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")

        url: typing.Final = self.index_url()
        access_token: typing.Final = client_session.get(AppSSO.ESI_ACCESS_TOKEN, '')
        obj_id_set: typing.Final = set(await AppESI.get_pages(url, access_token, request_params=self.request_params))

        # cache_dict: typing.Final = dict()
        # cache_filename: typing.Final = os.path.join(self.configdir, f"{self.__class__.__name__}.json")
        # if os.path.exists(cache_filename):
        #     with open(cache_filename) as ifp:
        #         cache_dict |= {self.object_id(x): x for x in [self.object_class(**edict) for edict in json.load(ifp)]}

        existing_obj_id_set: typing.Final = set()

        try:
            async with await self.db.sessionmaker() as session:
                session: sqlalchemy.ext.asyncio.AsyncSession

                query = sqlalchemy.select(self.object_class)
                async for obj in await session.stream_scalars(query):
                    existing_obj_id_set.add(self.object_id(obj))

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        missing_obj_id_set = obj_id_set - existing_obj_id_set
        if len(missing_obj_id_set) > 0:

            add_edict_list: typing.Final = list()
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=AppConstants.ESI_LIMIT_PER_HOST)) as http_session:
                task_list: typing.Final = list()
                for obj_id in missing_obj_id_set:
                    self.logger.info("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"{str(self.object_class.__name__)}({obj_id})"))
                    task_list.append(self._get_item_dict(obj_id, http_session))

                if len(task_list) > 0:
                    add_edict_list.extend([x for x in await asyncio.gather(*task_list) if x])

            if len(add_edict_list) > 0:
                try:
                    async with await self.db.sessionmaker() as session:
                        session.begin()
                        for edict in add_edict_list:
                            if any([edict is None, len(edict) == 0]):
                                continue
                            session.add(self.object_class(**edict))
                        await session.commit()
                except Exception as ex:
                    otel_add_exception(ex)
                    self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        # async with await self.db.sessionmaker() as session, session.begin():
        #     existing_query = sqlalchemy.select(self.object_class)
        #     existing_query_result = await session.execute(existing_query)
        #     existing_obj_list: typing.Final = [{x: getattr(result, x) for x in result.__table__.columns.keys()} for result in existing_query_result.scalars()]
        #     if len(existing_obj_list) > 0:
        #         with open(cache_filename, "w") as ofp:
        #             json.dump(existing_obj_list, ofp, indent=4)

        self.logger.info(f"< {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")


class ESIUniverseRegionsBackfillTask(ESIBackfillTask):

    @property
    def object_class(self) -> typing.Any:
        return AppTables.UniverseRegion

    def object_id(self, obj: typing.Any) -> int:
        if isinstance(obj, self.object_class):
            return obj.region_id
        return 0

    def index_url(self) -> str:
        return f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/universe/regions/"

    def item_url(self, id: int) -> str:
        url: typing.Final = f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/universe/regions/{id}/"
        # print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {url}")
        return url

    def item_dict(self, id: int, input: dict) -> dict:
        edict: typing.Final = dict({
            "region_id": id
        })
        for k, v in input.items():
            if k in ["name"]:
                edict[k] = v
            elif k in ["region_id"]:
                edict[k] = int(v)
            else:
                continue
        # print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {edict}")
        return edict


class ESIUniverseConstellationsBackfillTask(ESIBackfillTask):

    @property
    def object_class(self) -> typing.Any:
        return AppTables.UniverseConstellation

    def object_id(self, obj: typing.Any) -> int:
        if isinstance(obj, self.object_class):
            return obj.constellation_id
        return 0

    def index_url(self) -> str:
        return f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/universe/constellations/"

    def item_url(self, id: int) -> str:
        url: typing.Final = f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/universe/constellations/{id}/"
        # print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {url}")
        return url

    def item_dict(self, id: int, input: dict) -> dict:
        edict: typing.Final = dict({
            "constellation_id": id
        })
        for k, v in input.items():
            if k in ["name"]:
                edict[k] = v
            elif k in ["region_id", "constellation_id"]:
                edict[k] = int(v)
            else:
                continue
        # print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {edict}")
        return edict


class ESIUniverseSystemsBackfillTask(ESIBackfillTask):

    @property
    def object_class(self) -> typing.Any:
        return AppTables.UniverseSystem

    def object_id(self, obj: typing.Any) -> int:
        if isinstance(obj, self.object_class):
            return obj.system_id
        return 0

    def index_url(self) -> str:
        return f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/universe/systems/"

    def item_url(self, id: int) -> str:
        url: typing.Final = f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/universe/systems/{id}/"
        # print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {url}")
        return url

    def item_dict(self, id: int, input: dict) -> dict:
        edict: typing.Final = dict({
            "system_id": id
        })
        for k, v in input.items():
            if k in ["name"]:
                edict[k] = v
            elif k in ["constellation_id", "system_id"]:
                edict[k] = int(v)
            else:
                continue
        # print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {edict}")
        return edict


class ESIAllianceBackfillTask(ESIBackfillTask):

    @property
    def object_class(self) -> typing.Any:
        return AppTables.Alliance

    def object_id(self, obj: typing.Any) -> int:
        if isinstance(obj, self.object_class):
            return obj.alliance_id
        return 0

    def index_url(self) -> str:
        return f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/alliances/"

    def item_url(self, id: int) -> str:
        url: typing.Final = f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/alliances/{id}/"
        # print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {url}")
        return url

    def item_dict(self, id: int, input: dict) -> dict:
        edict: typing.Final = dict({
            "alliance_id": id
        })
        for k, v in input.items():
            if k not in ["name", "ticker"]:
                continue
            edict[k] = v
        # print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {edict}")
        return edict
