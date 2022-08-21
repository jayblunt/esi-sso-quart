import abc
import asyncio
import collections
import collections.abc
import inspect
import json
import os
from typing import Any, Final

import aiohttp
import aiohttp.client_exceptions
import sqlalchemy
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql
from db import EveTables
from sso import EveSSO

from .task import EveTask


class EveBackfillTask(EveTask, metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def object_class(self) -> Any:
        pass

    @abc.abstractmethod
    def object_valid(self, obj: Any) -> bool:
        pass

    @abc.abstractmethod
    def object_id(self, obj: Any) -> int:
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

    async def _get_page(self, id: int, http_session: aiohttp.ClientSession) -> Any:
        obj_class: Final = self.object_class()
        url: Final = self.item_url(id)
        attempts_remaining = self.ERROR_RETRY_COUNT
        while attempts_remaining > 0:
            async with http_session.get(url, params=self.common_params) as response:
                if response.status in [200]:
                    edict: Final = self.item_dict(id, await response.json())
                    if len(edict) > 0:
                        return obj_class(**edict)
                else:
                    attempts_remaining -= 1
                    self.logger.info("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"{response.url} -> {response.status}"))
                    await asyncio.sleep(self.ERROR_SLEEP_TIME)
        self.logger.error("- {}.{}: {} -> {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  id, None))
        return None

    async def run(self, client_session: collections.abc.MutableMapping):

        self.logger.info(f"> {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")

        obj_class: Final = self.object_class()

        url = self.index_url()
        access_token: Final = client_session.get(EveSSO.ESI_ACCESS_TOKEN, '')
        id_set: Final = set(await self.get_pages(url, access_token))

        cache_dict: Final = dict()
        cache_filename: Final = os.path.join(self.configdir, f"{self.__class__.__name__}.json")
        self.logger.info("- {}.{}: {} = {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  "cache_filename", cache_filename))
        if os.path.exists(cache_filename):
            with open(cache_filename) as ifp:
                cache_dict |= {self.object_id(x): x for x in [obj_class(**edict) for edict in json.load(ifp)]}

        self.logger.info("- {}.{}: {} = {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  "cache_dict", cache_dict))

        async with await self.db.sessionmaker() as db, db.begin():

            existing_query = sqlalchemy.select(obj_class)
            existing_query_result = await db.execute(existing_query)
            existing_obj_set: Final = {result for result in existing_query_result.scalars()}
            existing_id_set: Final = {self.object_id(x) for x in existing_obj_set}

            obj_set: Final = set()
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=self.LIMIT_PER_HOST)) as http_session:
                task_list: Final = list()
                for id in id_set - existing_id_set:
                    cache_obj = cache_dict.get(id, None)
                    if cache_obj is None:
                        task_list.append(asyncio.ensure_future(self._get_page(id, http_session)))
                    else:
                        obj_set.add(cache_obj)

                if len(task_list) > 0:
                    result_list = await asyncio.gather(*task_list)
                    self.logger.info("- {}.{}: {} = {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  "len(result_list)", len(result_list)))
                    for obj in result_list:
                        if obj is None:
                            continue
                        obj_set.add(obj)

            if len(obj_set) > 0:
                db.add_all(obj_set)
                await db.commit()

        async with await self.db.sessionmaker() as db, db.begin():
            existing_query = sqlalchemy.select(obj_class)
            existing_query_result = await db.execute(existing_query)
            existing_obj_list: Final = [{x: getattr(result, x) for x in result.__table__.columns.keys()} for result in existing_query_result.scalars()]
            if len(existing_obj_list) > 0:
                with open(cache_filename, "w") as ofp:
                    json.dump(existing_obj_list, ofp, indent=4)

        self.logger.info(f"< {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")


class EveUniverseRegionsTask(EveBackfillTask):

    def object_class(self) -> Any:
        return EveTables.UniverseRegion

    def object_valid(self, obj: Any) -> bool:
        return isinstance(obj, EveTables.UniverseRegion)

    def object_id(self, obj: Any) -> int:
        if isinstance(obj, EveTables.UniverseRegion):
            return obj.region_id
        return 0

    def index_url(self) -> str:
        return "https://esi.evetech.net/latest/universe/regions/"

    def item_url(self, id: int) -> str:
        url: Final = f"https://esi.evetech.net/latest/universe/regions/{id}/"
        # print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {url}")
        return url

    def item_dict(self, id: int, input: dict) -> dict:
        edict: Final = dict({
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


class EveUniverseConstellationsTask(EveBackfillTask):

    def object_class(self) -> Any:
        return EveTables.UniverseConstellation

    def object_valid(self, obj: Any) -> bool:
        return isinstance(obj, EveTables.UniverseConstellation)

    def object_id(self, obj: Any) -> int:
        if isinstance(obj, EveTables.UniverseConstellation):
            return obj.constellation_id
        return 0

    def index_url(self) -> str:
        return "https://esi.evetech.net/latest/universe/constellations/"

    def item_url(self, id: int) -> str:
        url: Final = f"https://esi.evetech.net/latest/universe/constellations/{id}/"
        # print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {url}")
        return url

    def item_dict(self, id: int, input: dict) -> dict:
        edict: Final = dict({
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


class EveUniverseSystemsTask(EveBackfillTask):

    def object_class(self) -> Any:
        return EveTables.UniverseSystem

    def object_valid(self, obj: Any) -> bool:
        return isinstance(obj, EveTables.UniverseSystem)

    def object_id(self, obj: Any) -> int:
        if isinstance(obj, EveTables.UniverseSystem):
            return obj.system_id
        return 0

    def index_url(self) -> str:
        return "https://esi.evetech.net/latest/universe/systems/"

    def item_url(self, id: int) -> str:
        url: Final = f"https://esi.evetech.net/latest/universe/systems/{id}/"
        # print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {url}")
        return url

    def item_dict(self, id: int, input: dict) -> dict:
        edict: Final = dict({
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


class EveAllianceTask(EveBackfillTask):

    def object_class(self) -> Any:
        return EveTables.Alliance

    def object_valid(self, obj: Any) -> bool:
        return isinstance(obj, EveTables.Alliance)

    def object_id(self, obj: Any) -> int:
        if isinstance(obj, EveTables.Alliance):
            return obj.alliance_id
        return 0

    def index_url(self) -> str:
        return "https://esi.evetech.net/latest/alliances/"

    def item_url(self, id: int) -> str:
        url: Final = f"https://esi.evetech.net/latest/alliances/{id}/"
        # print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {url}")
        return url

    def item_dict(self, id: int, input: dict) -> dict:
        edict: Final = dict({
            "alliance_id": id
        })
        for k, v in input.items():
            if k not in ["name", "ticker"]:
                continue
            edict[k] = v
        # print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {edict}")
        return edict
