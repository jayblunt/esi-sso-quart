import inspect
import os
from typing import Final

import aiohttp
import aiohttp.client_exceptions
import quart.json
import sqlalchemy
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql
from db import EveTables

from .task import EveTask


class EveUniverseRegionsTask(EveTask):

    async def run(self):

        common_params: Final = {
            "datasource": "tranquility",
            "language": "en"
        }

        self.logger.info("> {}.{}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name))

        url = "https://esi.evetech.net/latest/universe/regions/"
        region_id_set: Final = set(await self.get_pages(url))

        cache_obj_set: Final = set()
        cache_filename: Final = os.path.join(self.configdir, f"{self.__class__.__name__}.json")
        if os.path.exists(cache_filename):
            with open(cache_filename) as ifp:
                cache_obj_set |= {EveTables.UniverseRegion(**edict) for edict in quart.json.load(ifp)}

        async with await self.db.sessionmaker() as session:

            existing_obj_set: Final = set()
            async with session.begin():
                existing_query = sqlalchemy.select(EveTables.UniverseRegion)
                existing_query_result = await session.execute(existing_query)
                existing_obj_set |= {result for result in existing_query_result.scalars()}
            existing_region_id_set: Final = {x.region_id for x in existing_obj_set}

            # if len(region_id_set - existing_region_id_set) > 0:
            #     load_id_set: Final = region_id_set - existing_region_id_set
            #     load_obj_set: Final = set()
            #     for x in cache_obj_set:
            #         obj_id = x.region_id
            #         if obj_id in load_id_set:
            #             load_obj_set.add(x)
            #             existing_region_id_set.add(obj_id)

            #     if len(load_obj_set) > 0:
            #         session.add_all(load_obj_set)
            #         await session.flush()

            obj_set: Final = set()
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=self.LIMIT_PER_HOST)) as client_session:
                for region_id in region_id_set - existing_region_id_set:
                    url = f"https://esi.evetech.net/latest/universe/regions/{region_id}/"
                    async with client_session.get(url, params=common_params) as response:
                        self.logger.info("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"{response.url} -> {response.status}"))
                        # print(f"{response.url} -> {response.status}")
                        if response.status in [200]:
                            edict: Final = dict()
                            for k, v in dict(await response.json()).items():
                                if k in ["name"]:
                                    edict[k] = v
                                elif k in ["region_id"]:
                                    edict[k] = int(v)
                                else:
                                    continue
                            obj: Final = EveTables.UniverseRegion(**edict)
                            obj_set.add(obj)

            if len(obj_set) > 0:
                session.add_all(obj_set)
                await session.commit()

        async with await self.db.sessionmaker() as session:
            existing_query = sqlalchemy.select(EveTables.UniverseRegion)
            existing_query_result = await session.execute(existing_query)
            existing_obj_list: Final = [{x: getattr(result, x) for x in result.__table__.columns.keys()} for result in existing_query_result.scalars()]
            if len(existing_obj_list) > 0:
                with open(cache_filename, "w") as ofp:
                    quart.json.dump(existing_obj_list, ofp, indent=4)

        self.logger.info("< {}.{}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name))


class EveUniverseConstellationsTask(EveTask):

    async def run(self):

        common_params: Final = {
            "datasource": "tranquility",
            "language": "en"
        }

        self.logger.info("> {}.{}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name))

        url = "https://esi.evetech.net/latest/universe/constellations/"
        constellation_id_set: Final = set(await self.get_pages(url))

        cache_obj_set: Final = set()
        cache_filename: Final = os.path.join(self.configdir, f"{self.__class__.__name__}.json")
        if os.path.exists(cache_filename):
            with open(cache_filename) as ifp:
                cache_obj_set |= {EveTables.UniverseConstellation(**edict) for edict in quart.json.load(ifp)}

        async with await self.db.sessionmaker() as session:

            existing_obj_set: Final = set()
            async with session.begin():
                existing_query = sqlalchemy.select(EveTables.UniverseConstellation)
                existing_query_result = await session.execute(existing_query)
                existing_obj_set |= {result for result in existing_query_result.scalars()}
            existing_constellation_id_set: Final = {x.constellation_id for x in existing_obj_set}

            # if len(constellation_id_set - existing_constellation_id_set) > 0:
            #     load_id_set: Final = constellation_id_set - existing_constellation_id_set
            #     load_obj_set: Final = set()
            #     for x in cache_obj_set:
            #         obj_id = x.constellation_id
            #         if obj_id in load_id_set:
            #             load_obj_set.add(x)
            #             existing_constellation_id_set.add(obj_id)

            #     if len(load_obj_set) > 0:
            #         session.add_all(load_obj_set)
            #         await session.flush()

            obj_set: Final = set()
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=self.LIMIT_PER_HOST)) as client_session:
                for constellation_id in constellation_id_set - existing_constellation_id_set:
                    url = f"https://esi.evetech.net/latest/universe/constellations/{constellation_id}/"
                    async with client_session.get(url, params=common_params) as response:
                        self.logger.info("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"{response.url} -> {response.status}"))
                        # print(f"{response.url} -> {response.status}")
                        if response.status in [200]:
                            edict: Final = dict()
                            for k, v in dict(await response.json()).items():
                                if k in ["name"]:
                                    edict[k] = v
                                elif k in ["region_id", "constellation_id"]:
                                    edict[k] = int(v)
                                else:
                                    continue
                            obj: Final = EveTables.UniverseConstellation(**edict)
                            obj_set.add(obj)

            if len(obj_set) > 0:
                session.add_all(obj_set)
                await session.commit()

        async with await self.db.sessionmaker() as session:
            existing_query = sqlalchemy.select(EveTables.UniverseConstellation)
            existing_query_result = await session.execute(existing_query)
            existing_obj_list: Final = [{x: getattr(result, x) for x in result.__table__.columns.keys()} for result in existing_query_result.scalars()]
            if len(existing_obj_list) > 0:
                with open(cache_filename, "w") as ofp:
                    quart.json.dump(existing_obj_list, ofp, indent=4)

        self.logger.info("< {}.{}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name))


class EveUniverseSystemsTask(EveTask):

    async def run(self):

        common_params: Final = {
            "datasource": "tranquility",
            "language": "en"
        }

        self.logger.info("> {}.{}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name))

        url = "https://esi.evetech.net/latest/universe/systems/"
        system_id_set: Final = set(await self.get_pages(url))

        cache_obj_set: Final = set()
        cache_filename: Final = os.path.join(self.configdir, f"{self.__class__.__name__}.json")
        if os.path.exists(cache_filename):
            with open(cache_filename) as ifp:
                cache_obj_set |= {EveTables.UniverseSystem(**edict) for edict in quart.json.load(ifp)}

        async with await self.db.sessionmaker() as session:

            existing_obj_set: Final = set()
            async with session.begin():
                existing_query = sqlalchemy.select(EveTables.UniverseSystem)
                existing_query_result = await session.execute(existing_query)
                existing_obj_set |= {result for result in existing_query_result.scalars()}
            existing_system_id_set: Final = {x.system_id for x in existing_obj_set}

            # if len(system_id_set - existing_system_id_set) > 0:
            #     load_id_set: Final = system_id_set - existing_system_id_set
            #     load_obj_set: Final = set()
            #     for x in cache_obj_set:
            #         obj_id = x.system_id
            #         if obj_id in load_id_set:
            #             load_obj_set.add(x)
            #             existing_system_id_set.add(obj_id)

            #     if len(load_obj_set) > 0:
            #         session.add_all(load_obj_set)
            #         await session.flush()

            obj_set: Final = set()
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=self.LIMIT_PER_HOST)) as client_session:
                for system_id in system_id_set - existing_system_id_set:
                    url = f"https://esi.evetech.net/latest/universe/systems/{system_id}/"
                    async with client_session.get(url, params=common_params) as response:
                        self.logger.info("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"{response.url} -> {response.status}"))
                        # print(f"{response.url} -> {response.status}")
                        if response.status in [200]:
                            edict: Final = dict()
                            for k, v in dict(await response.json()).items():
                                if k in ["name"]:
                                    edict[k] = v
                                elif k in ["constellation_id", "system_id"]:
                                    edict[k] = int(v)
                                else:
                                    continue
                            obj: Final = EveTables.UniverseSystem(**edict)
                            obj_set.add(obj)

            if len(obj_set) > 0:
                session.add_all(obj_set)
                await session.commit()

        async with await self.db.sessionmaker() as session:
            existing_query = sqlalchemy.select(EveTables.UniverseSystem)
            existing_query_result = await session.execute(existing_query)
            existing_obj_list: Final = [{x: getattr(result, x) for x in result.__table__.columns.keys()} for result in existing_query_result.scalars()]
            if len(existing_obj_list) > 0:
                with open(cache_filename, "w") as ofp:
                    quart.json.dump(existing_obj_list, ofp, indent=4)

        self.logger.info("< {}.{}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name))
