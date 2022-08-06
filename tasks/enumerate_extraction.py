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


class EveEnumerateExtractionTask(EveTask):

    async def run(self):

        corporation_id: Final = int(self.session.get(
            EveSSO.ESI_CORPORATION_ID, 0))

        required_scopes: Final = {
            "esi-industry.read_corporation_mining.v1"}

        if all([self.session.get(EveSSO.ESI_CHARACTER_STATION_MANAGER_ROLE, False), len(required_scopes.intersection(set(self.session.get(EveSSO.ESI_TOKEN_SCOPES, [])))) == len(required_scopes)]):

            async_session = sqlalchemy.orm.sessionmaker(await self.db.engine, expire_on_commit=False, class_=sqlalchemy.ext.asyncio.AsyncSession)

            url = f"https://esi.evetech.net/latest/corporation/{corporation_id}/mining/extractions/"
            extractions: Final = await self.get_pages(url)
            # print(json.dumps(extractions, ensure_ascii=True, indent=4))
            if len(extractions) == 0:
                return

            async with async_session() as session:

                # Save history
                async with session.begin():
                    history_insertions: Final = [EveTables.ExtractionHistory(character_id=int(self.session.get(
                        EveSSO.ESI_CHARACTER_ID, 0)), structure_id=int(x.get("structure_id", 0)), json=x) for x in extractions]
                    session.add_all(history_insertions)
                    await session.commit()

                # Save current extractions
                async with session.begin():
                    all_extractions_set: Final = set()
                    all_extractions_stmt: Final = sqlalchemy.select(EveTables.Extraction).where(EveTables.Extraction.corporation_id == corporation_id)
                    for results in await session.execute(all_extractions_stmt):
                        all_extractions_set.add(results[0])

                    current_insertions: Final = list()
                    current_deletions: Final = list(all_extractions_set)
                    for x in extractions:
                        edict: Final = dict({
                            "character_id": int(self.session.get(EveSSO.ESI_CHARACTER_ID, 0)),
                            "corporation_id": int(self.session.get(EveSSO.ESI_CORPORATION_ID, 0)),
                        })
                        for k, v in x.items():
                            if k in ["chunk_arrival_time", "extraction_start_time", "natural_decay_time"]:
                                v = dateutil.parser.parse(v).replace(tzinfo=zoneinfo.ZoneInfo("UTC"))
                            elif k in ["structure_id", "moon_id"]:
                                v = int(v)
                            else:
                                continue
                            edict[k] = v
                        ie: Final = EveTables.Extraction(**edict)
                        current_insertions.append(ie)

                    if len(current_deletions):
                        [await session.delete(x) for x in current_deletions]

                    if len(current_insertions):
                        session.add_all(current_insertions)

                    await session.commit()
