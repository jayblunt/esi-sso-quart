import zoneinfo
from typing import Final

import dateutil.parser
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

        corporation_id: Final = int(self.session.get(
            EveSSO.ESI_CORPORATEION_ID, 0))

        required_scopes: Final = {
            "esi-corporations.read_structures.v1", "esi-industry.read_corporation_mining.v1"}

        if all([self.session.get(EveSSO.ESI_CHARACTER_STATION_MANAGER_ROLE, False), len(required_scopes.intersection(set(self.session.get(EveSSO.ESI_TOKEN_SCOPES, [])))) == len(required_scopes)]):

            async_session = sqlalchemy.orm.sessionmaker(await self.db.engine, expire_on_commit=False, class_=sqlalchemy.ext.asyncio.AsyncSession)

            url = f"https://esi.evetech.net/latest/corporations/{corporation_id}/structures/"
            structures: Final = await self.get_pages(url)
            # print(json.dumps(structures, ensure_ascii=True, indent=4))

            url = f"https://esi.evetech.net/latest/corporation/{corporation_id}/mining/extractions/"
            extractions: Final = await self.get_pages(url)
            # print(json.dumps(extractions, ensure_ascii=True, indent=4))

            async with async_session() as session:

                # Save history
                async with session.begin():
                    structure_insertions = [EveTables.StructureHistory(character_id=int(self.session.get(
                        EveSSO.ESI_CHARACTER_ID, 0)), structure_id=int(x.get("structure_id", 0)), json=x) for x in structures]

                    extraction_insertions = [EveTables.ExtractionHistory(character_id=int(self.session.get(
                        EveSSO.ESI_CHARACTER_ID, 0)), structure_id=int(x.get("structure_id", 0)), json=x) for x in extractions]
                    session.add_all(structure_insertions + extraction_insertions)
                    await session.commit()

                # Save current extractions
                async with session.begin():
                    current_insertions = list()
                    current_deletions = list()
                    for x in extractions:
                        d = dict()
                        for k, v in x.items():
                            if k in ["chunk_arrival_time", "extraction_start_time", "natural_decay_time"]:
                                v = dateutil.parser.parse(v).replace(tzinfo=zoneinfo.ZoneInfo("UTC"))
                            else:
                                v = int(v)
                            d[k] = v
                        e = EveTables.Extraction(**d)
                        if await session.get(EveTables.Extraction, {"structure_id": e.structure_id, "moon_id": e.moon_id}) is not None:
                            current_deletions.append(e)
                        current_insertions.append(e)
                    if len(current_deletions):
                        [await session.delete(x) for x in current_deletions]
                    if len(current_insertions):
                        session.add_all(current_insertions)
                    await session.commit()
