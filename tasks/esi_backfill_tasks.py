import abc
import asyncio
import collections
import collections.abc
import http
import inspect
import typing

import aiohttp
import aiohttp.client_exceptions
import sqlalchemy
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql
import opentelemetry.trace

from app import AppConstants, AppTables, AppTask
from support.telemetry import otel, otel_add_error, otel_add_exception


class ESIBackfillTask(AppTask, metaclass=abc.ABCMeta):

    @property
    @abc.abstractmethod
    def object_class(self) -> type:
        ...

    def object_valid(self, obj: object) -> bool:
        return isinstance(obj, self.object_class)

    @abc.abstractmethod
    def object_id(self, obj: type) -> int:
        ...

    @abc.abstractmethod
    def index_url(self) -> str:
        ...

    @abc.abstractmethod
    def item_url(self, id: int) -> str:
        ...

    @abc.abstractmethod
    def item_dict(self, id: int, input: dict) -> dict:
        ...

    @otel
    async def _get_item_dict(self, id: int, http_session: aiohttp.ClientSession) -> dict:
        url: typing.Final = self.item_url(id)
        attempts_remaining = AppConstants.ESI_ERROR_RETRY_COUNT
        while attempts_remaining > 0:
            try:
                async with await http_session.get(url, params=self.request_params) as response:
                    if response.status in [http.HTTPStatus.OK]:
                        edict: typing.Final = self.item_dict(id, await response.json())
                        if len(edict) > 0:
                            return edict
                    else:
                        attempts_remaining -= 1
                        otel_add_error(f"{response.url} -> {response.status}")
                        self.logger.info("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name, f"{response.url} -> {response.status}"))
                        if response.status in [http.HTTPStatus.BAD_REQUEST, http.HTTPStatus.FORBIDDEN]:
                            attempts_remaining = 0
                        if attempts_remaining > 0:
                            await asyncio.sleep(AppConstants.ESI_ERROR_SLEEP_TIME * AppConstants.ESI_ERROR_SLEEP_MODIFIERS.get(response.status, 1))

            except aiohttp.client_exceptions.ClientConnectionError as ex:
                attempts_remaining -= 1
                otel_add_error(f"{url=} -> {ex=}")
                self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {url=}, {ex=}")
                if attempts_remaining > 0:
                    await asyncio.sleep(AppConstants.ESI_ERROR_SLEEP_TIME)

            except Exception as ex:
                otel_add_error(f"{url=} -> {ex=}")
                self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {url=}, {ex=}")
                break

        self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {id} -> {None}")
        return None

    async def run_once(self, client_session: collections.abc.MutableMapping, /):
        self.logger.info(f"> {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")

        url: typing.Final = self.index_url()

        esi_result: typing.Final = await self.esi.pages(url, '', request_params=self.request_params)
        if esi_result.status == http.HTTPStatus.NOT_MODIFIED:
            self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {esi_result.status=}")
            return

        obj_id_set: typing.Final = set()
        if esi_result.status == http.HTTPStatus.OK and esi_result.data is not None:
            obj_id_set.update(set(esi_result.data))

        existing_obj_id_set: typing.Final = set()

        try:
            async with await self.db.sessionmaker() as session:
                session: sqlalchemy.ext.asyncio.AsyncSession

                query = sqlalchemy.select(self.object_class)
                async for obj in await session.stream_scalars(query):
                    existing_obj_id_set.add(self.object_id(obj))

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex=}")

        missing_obj_id_set = obj_id_set - existing_obj_id_set
        if len(missing_obj_id_set) > 0:

            add_edict_list: typing.Final = list()

            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=AppConstants.ESI_LIMIT_PER_HOST)) as http_session:
                task_list: typing.Final = list()
                for obj_id in missing_obj_id_set:
                    self.logger.info("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name, f"{str(self.object_class.__name__)}({obj_id})"))
                    task_list.append(self._get_item_dict(obj_id, http_session))

                if len(task_list) > 0:
                    task_result_list = await asyncio.gather(*task_list)
                    add_edict_list.extend([x for x in filter(None, task_result_list)])

            if len(add_edict_list) > 0:
                add_obj_list = list()
                for edict in add_edict_list:
                    await asyncio.sleep(0)

                    if any([edict is None, len(edict) == 0]):
                        continue
                    add_obj_list.append(self.object_class(**edict))
                if len(add_obj_list) > 0:
                    try:
                        async with await self.db.sessionmaker() as session:
                            session.begin()
                            session.add_all(add_obj_list)
                            await session.commit()
                    except Exception as ex:
                        otel_add_exception(ex)
                        self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex=}")

        self.logger.info(f"< {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")

    async def run(self, client_session: collections.abc.MutableMapping, /):
        tracer = opentelemetry.trace.get_tracer_provider().get_tracer(__name__)
        with tracer.start_as_current_span(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}"):
            await self.run_once(client_session)


class ESIUniverseRegionsBackfillTask(ESIBackfillTask):

    @property
    def object_class(self) -> type:
        return AppTables.UniverseRegion

    def object_id(self, obj: AppTables.UniverseRegion) -> int:
        if isinstance(obj, self.object_class):
            return obj.region_id
        return 0

    def index_url(self) -> str:
        return f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/universe/regions/"

    def item_url(self, id: int) -> str:
        return f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/universe/regions/{id}/"

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
        return edict


class ESIUniverseConstellationsBackfillTask(ESIBackfillTask):

    @property
    def object_class(self) -> type:
        return AppTables.UniverseConstellation

    def object_id(self, obj: AppTables.UniverseConstellation) -> int:
        if isinstance(obj, self.object_class):
            return obj.constellation_id
        return 0

    def index_url(self) -> str:
        return f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/universe/constellations/"

    def item_url(self, id: int) -> str:
        return f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/universe/constellations/{id}/"

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
        return edict


class ESIUniverseSystemsBackfillTask(ESIBackfillTask):

    @property
    def object_class(self) -> type:
        return AppTables.UniverseSystem

    def object_id(self, obj: AppTables.UniverseSystem) -> int:
        if isinstance(obj, self.object_class):
            return obj.system_id
        return 0

    def index_url(self) -> str:
        return f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/universe/systems/"

    def item_url(self, id: int) -> str:
        return f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/universe/systems/{id}/"

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
        return edict


class ESIAllianceBackfillTask(ESIBackfillTask):

    @property
    def object_class(self) -> type:
        return AppTables.Alliance

    def object_id(self, obj: AppTables.Alliance) -> int:
        if isinstance(obj, self.object_class):
            return obj.alliance_id
        return 0

    def index_url(self) -> str:
        return f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/alliances/"

    def item_url(self, id: int) -> str:
        return f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/alliances/{id}/"

    def item_dict(self, id: int, input: dict) -> dict:
        edict: typing.Final = dict({
            "alliance_id": id
        })
        for k, v in input.items():
            if k not in ["name", "ticker"]:
                continue
            edict[k] = v
        return edict


class ESINPCorporationBackfillTask(ESIBackfillTask):

    @property
    def object_class(self) -> type:
        return AppTables.Corporation

    def object_id(self, obj: AppTables.Corporation) -> int:
        if isinstance(obj, self.object_class):
            return obj.corporation_id
        return 0

    def index_url(self) -> str:
        return f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/corporations/npccorps/"

    def item_url(self, id: int) -> str:
        return f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/corporations/{id}/"

    def item_dict(self, id: int, input: dict) -> dict:
        edict: typing.Final = dict({
            "corporation_id": id
        })
        for k, v in input.items():
            if k not in ["name", "ticker"]:
                continue
            edict[k] = v
        return edict
