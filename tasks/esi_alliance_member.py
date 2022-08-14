from typing import Final

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


class EveEsiAlliancMemberTask(EveTask):

    async def run(self):

        corporation_id_set: Final = set()

        alliance_id: Final = int(self.session.get(EveSSO.ESI_ALLIANCE_ID, 0))
        if alliance_id in [99002329]:
            corporation_id_set.add(1000169)

        if alliance_id > 0:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=self.LIMIT_PER_HOST)) as client_session:
                url = f"https://esi.evetech.net/latest/alliances/{alliance_id}/corporations/"
                async with client_session.get(url, params=self.common_params) as response:
                    # print(f"{response.url} -> {response.status}")
                    if response.status in [200]:
                        for corporation_id in list(await response.json()):
                            corporation_id_set.add(int(corporation_id))

        if alliance_id > 0 and len(corporation_id_set) > 0:

            async with await self.db.sessionmaker() as session:

                async with session.begin():
                    alliance_corporations_query: Final = sqlalchemy.select(EveTables.AllianceCorporation).where(EveTables.AllianceCorporation.alliance_id == alliance_id)
                    alliance_corporations_query_result = await session.execute(alliance_corporations_query)
                    existing_obj_set: Final = {result for result in alliance_corporations_query_result.scalars()}

                    obj_set = set()
                    for corporation_id in corporation_id_set:
                        obj = EveTables.AllianceCorporation(alliance_id=alliance_id, corporation_id=corporation_id)
                        obj_set.add(obj)

                    if len(existing_obj_set) > 0:
                        [await session.delete(x) for x in existing_obj_set]

                    if len(obj_set) > 0:
                        session.add_all(obj_set)

                    if any([len(existing_obj_set) > 0, len(obj_set) > 0]):
                        await session.commit()

        if len(corporation_id_set) > 0:

            async with await self.db.sessionmaker() as session:

                async with session.begin():
                    existing_corporations_query: Final = sqlalchemy.select(EveTables.Corporation)
                    existing_corporations_query_result = await session.execute(existing_corporations_query)
                    existing_corporation_set: Final = {result for result in existing_corporations_query_result.scalars()}

                    existing_corporation_id_set: Final = {x.corporation_id for x in existing_corporation_set}

                    obj_set = set()

                    # print(f"existing_corporation_id_set: {existing_corporation_id_set}")
                    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=self.LIMIT_PER_HOST)) as client_session:

                        for corporation_id in corporation_id_set - existing_corporation_id_set:
                            # print(f"corporation_id: {corporation_id}")

                            url = f"https://esi.evetech.net/latest/corporations/{corporation_id}/"
                            async with client_session.get(url, params=self.common_params) as response:
                                # print(f"{response.url} -> {response.status}")
                                if response.status in [200]:
                                    edict: Final = dict({
                                        "corporation_id": corporation_id
                                    })

                                    for k, v in dict(await response.json()).items():
                                        if k in ["alliance_id"]:
                                            v = int(v)
                                        elif k not in ["name", "ticker"]:
                                            continue
                                        edict[k] = v

                                    obj = EveTables.Corporation(**edict)
                                    obj_set.add(obj)

                    if len(obj_set) > 0:
                        print(obj_set)
                        session.add_all(obj_set)
                        await session.commit()
