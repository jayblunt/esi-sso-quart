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
            EveSSO.ESI_CORPORATION_ID, 0))

        required_scopes: Final = {
            "esi-corporations.read_structures.v1"}

        if all([self.session.get(EveSSO.ESI_CHARACTER_STATION_MANAGER_ROLE, False), len(required_scopes.intersection(set(self.session.get(EveSSO.ESI_TOKEN_SCOPES, [])))) == len(required_scopes)]):

            async_session = sqlalchemy.orm.sessionmaker(await self.db.engine, expire_on_commit=False, class_=sqlalchemy.ext.asyncio.AsyncSession)

            url = f"https://esi.evetech.net/latest/corporations/{corporation_id}/structures/"
            structures: Final = await self.get_pages(url)
            # print(json.dumps(structures, ensure_ascii=True, indent=4))
            if len(structures) == 0:
                return

            async with async_session() as session:

                # Save history
                async with session.begin():
                    history_insertions: Final = [EveTables.StructureHistory(character_id=int(self.session.get(
                        EveSSO.ESI_CHARACTER_ID, 0)), structure_id=int(x.get("structure_id", 0)), json=x) for x in structures]
                    session.add_all(history_insertions)
                    await session.commit()

                # Save current extractions
                async with session.begin():
                    all_structures_set: Final = dict()
                    all_structures_stmt: Final = sqlalchemy.select(EveTables.Structure).where(EveTables.Structure.corporation_id == corporation_id)
                    for results in await session.execute(all_structures_stmt):
                        all_structures_set.add(results[0])

                    current_insertions: Final = list()
                    current_deletions: Final = list(all_structures_set)
                    for x in structures:
                        edict: Final = dict()
                        for k, v in x.items():
                            if k in ["fuel_expires", "state_timer_end", "state_timer_start", "unanchors_at"]:
                                v = dateutil.parser.parse(v).replace(tzinfo=zoneinfo.ZoneInfo("UTC"))
                            elif k in ["corporation_id", "structure_id", "system_id", "type_id"]:
                                v = int(v)
                            elif k in ["services"]:
                                v = list(filter(lambda x: x.get('name', '') == "Moon Drilling", v))
                                edict["has_moon_drill"] = bool(len(v) > 0)
                                continue
                            elif k not in ["name", "state"]:
                                continue
                            edict[k] = v
                        ie: Final = EveTables.Structure(**edict)
                        current_insertions.append(ie)

                    if len(current_deletions):
                        [await session.delete(x) for x in current_deletions]

                    if len(current_insertions):
                        session.add_all(current_insertions)

                    await session.commit()
