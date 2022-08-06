import contextlib
from typing import Final, MutableSet

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


class EveAlliancMemberTask(EveTask):

    async def run(self):

        session_headers = {
            # "Authorization": f"Bearer {self.session.get(EveSSO.ESI_ACCESS_TOKEN, '')}"
        }

        common_params = {
            "datasource": "tranquility"
        }

        corporation_id_set: Final[MutableSet[int]] = set()

        async with aiohttp.ClientSession(headers=session_headers) as client_session:

            alliance_id: Final = int(self.session.get(EveSSO.ESI_ALLIANCE_ID, 0))
            if alliance_id > 0:

                url = f"https://esi.evetech.net/latest/alliances/{alliance_id}/corporations/"
                with contextlib.suppress(aiohttp.client_exceptions.ClientResponseError):
                    async with client_session.get(url, params=common_params) as response:
                        print(f"{response.url} -> {response.status}")
                        if response.status in [200]:
                            for corporation_id in list(await response.json()):
                                corporation_id_set.add(int(corporation_id))

        if alliance_id > 0 and len(corporation_id_set) > 0:

            async_session = sqlalchemy.orm.sessionmaker(await self.db.engine, expire_on_commit=False, class_=sqlalchemy.ext.asyncio.AsyncSession)

            async with async_session() as session:

                # Save current alliance membership
                async with session.begin():
                    all_extractions_set: Final = dict()
                    all_extractions_stmt: Final = sqlalchemy.select(EveTables.AllianceMember).where(EveTables.AllianceMember.alliance_id == alliance_id)
                    for results in await session.execute(all_extractions_stmt):
                        all_extractions_set.add(results[0])

                    current_insertions: Final = list()
                    current_deletions: Final = list(all_extractions_set)
                    for corporation_id in corporation_id_set:
                        ie = EveTables.AllianceMember(alliance_id=alliance_id, corporation_id=corporation_id)
                        current_insertions.append(ie)

                    if len(current_deletions):
                        [await session.delete(x) for x in current_deletions]

                    if len(current_insertions):
                        session.add_all(current_insertions)

                    await session.commit()

            # print(f"corporation_list: {corporation_id_set}")
            # async with aiohttp.ClientSession(headers=session_headers) as client_session:

            #     for corporation_id in corporation_id_set:

            #         url = f"https://esi.evetech.net/latest/corporations/{corporation_id}"
            #         with contextlib.suppress(aiohttp.client_exceptions.ClientResponseError):
            #             async with client_session.get(url, params=common_params) as response:
            #                 print(f"{response.url} -> {response.status}")
            #                 if response.status in [200]:
            #                     results = dict(await response.json())
            #                     print(
            #                         f"{corporation_id}: {quart.json.dumps(results, ensure_ascii=True, indent=4)}")
