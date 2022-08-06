from binhex import FInfo
import dataclasses
import json
import os
from typing import Final

import sqlalchemy
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql
from db import EveTables

from .task import EveTask


@dataclasses.dataclass(frozen=True)
class MoonYieldData:
    type_id: int
    system_id: int
    planet_id: int
    moon_id: int
    yield_percent: float


class EveMoonYieldTask(EveTask):

    async def run(self):

        moon_data_list: Final = list()
        moon_data_filename: Final = os.path.abspath(os.path.join(self.session.get("BASEDIR", "."), "static", "moon_data.json"))
        if os.path.exists(moon_data_filename):
            with open(moon_data_filename) as ifp:
                [moon_data_list.append(MoonYieldData(**x)) for x in json.load(ifp)]
        print(moon_data_list)

        if len(moon_data_list) > 0:

            async_session = sqlalchemy.orm.sessionmaker(await self.db.engine, expire_on_commit=False, class_=sqlalchemy.ext.asyncio.AsyncSession)

            async with async_session() as session:

                async with session.begin():
                    all_deletions: Final = set()
                    all_additions: Final = set()

                    for md in moon_data_list:
                        all_moons_stmt: Final = sqlalchemy.select(EveTables.MoonYield).where(EveTables.MoonYield.moon_id == md.moon_id)
                        for results in await session.execute(all_moons_stmt):
                            all_deletions.add(results[0])

                        all_additions.add(EveTables.MoonYield(type_id=md.type_id, system_id=md.system_id, planet_id=md.planet_id, moon_id=md.moon_id, yield_percent=md.yield_percent))

                    if len(all_deletions):
                        [await session.delete(x) for x in all_deletions]

                    if len(all_additions):
                        session.add_all(all_additions)

                    await session.commit()
