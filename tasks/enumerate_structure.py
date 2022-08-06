import asyncio
import contextlib
import datetime
import json
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


class EveEnumerateStructureTask(EveTask):

    async def run(self):

        session_headers = {
            "Authorization": f"Bearer {self.session.get(EveSSO.ESI_ACCESS_TOKEN)}"
        }

        common_params = {
            "datasource": "tranquility"
        }

        corporation_id: Final = self.session.get(
            EveSSO.ESI_CORPORATEION_ID, '')

        required_scopes: Final = {
            "esi-corporations.read_structures.v1", "esi-industry.read_corporation_mining.v1"}

        structure_id_set: Final[MutableSet[str]] = set()

        if all([self.session.get(EveSSO.ESI_CHARACTER_STATION_MANAGER_ROLE, False), len(required_scopes.intersection(set(self.session.get(EveSSO.ESI_TOKEN_SCOPES, [])))) == len(required_scopes)]):

            async_session = sqlalchemy.orm.sessionmaker(await self.db.engine, expire_on_commit=False, class_=sqlalchemy.ext.asyncio.AsyncSession)

            url = f"https://esi.evetech.net/latest/corporations/{corporation_id}/structures/"

            structures: Final = await self.get_pages(url)
            # print(json.dumps(structures, ensure_ascii=True, indent=4))
            async with async_session() as session:
                async with session.begin():
                    insertions = [EveTables.Structure(structure_id=i.get("structure_id"), json=i) for i in structures]
                    session.add_all(insertions)
                    await session.commit()

            
            url = f"https://esi.evetech.net/latest/corporation/{corporation_id}/mining/extractions/"
            extractions: Final = await self.get_pages(url)
            # print(json.dumps(extractions, ensure_ascii=True, indent=4))
            async with async_session() as session:
                async with session.begin():
                    insertions = [EveTables.Extraction(structure_id=i.get("structure_id"), json=i) for i in extractions]
                    session.add_all(insertions)
                    await session.commit()
