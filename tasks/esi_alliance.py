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


class EveAllianceTask(EveTask):

    async def run(self):

        self.logger.info("> {}.{}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name))

        url = "https://esi.evetech.net/latest/alliances/"
        alliance_id_set: Final = set(await self.get_pages(url))

        cache_obj_set: Final = set()
        cache_filename: Final = os.path.join(self.configdir, f"{self.__class__.__name__}.json")
        if os.path.exists(cache_filename):
            with open(cache_filename) as ifp:
                cache_obj_set |= {EveTables.Alliance(**edict) for edict in quart.json.load(ifp)}

        async with await self.db.sessionmaker() as session:
            async with session.begin():

                existing_query = sqlalchemy.select(EveTables.Alliance)
                existing_query_result = await session.execute(existing_query)
                existing_obj_set: Final = {result for result in existing_query_result.scalars()}
                existing_alliance_id_set: Final = {x.alliance_id for x in existing_obj_set}

                # if len(alliance_id_set - existing_alliance_id_set) > 0:
                #     load_id_set: Final = alliance_id_set - existing_alliance_id_set
                #     load_obj_set: Final = set()
                #     for x in cache_obj_set:
                #         obj_id = x.alliance_id
                #         if obj_id in load_id_set:
                #             load_obj_set.add(x)
                #             existing_alliance_id_set.add(obj_id)

                #     if len(load_obj_set) > 0:
                #         session.add_all(load_obj_set)
                #         await session.flush()

                obj_set: Final = set()
                async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=self.LIMIT_PER_HOST)) as client_session:
                    for alliance_id in alliance_id_set - existing_alliance_id_set:
                        url = f"https://esi.evetech.net/latest/alliances/{alliance_id}/"
                        async with client_session.get(url, params=self.common_params) as response:
                            self.logger.info("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"{response.url} -> {response.status}"))
                            print(f"{response.url} -> {response.status}")
                            if response.status in [200]:
                                edict: Final = dict({
                                    "alliance_id": alliance_id
                                })
                                for k, v in dict(await response.json()).items():
                                    if k not in ["name", "ticker"]:
                                        continue
                                    edict[k] = v

                                obj: Final = EveTables.Alliance(**edict)
                                obj_set.add(obj)

                if len(obj_set) > 0:
                    session.add_all(obj_set)
                    await session.commit()

        async with await self.db.sessionmaker() as session:
            existing_query = sqlalchemy.select(EveTables.Alliance)
            existing_query_result = await session.execute(existing_query)
            existing_obj_list: Final = [{x: getattr(result, x) for x in result.__table__.columns.keys()} for result in existing_query_result.scalars()]
            with open(cache_filename, "w") as ofp:
                quart.json.dump(existing_obj_list, ofp, indent=4)

        self.logger.info("< {}.{}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name))
