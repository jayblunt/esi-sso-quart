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
                [moon_data_list.append(MoonYieldData(**edict)) for edict in json.load(ifp)]

        if len(moon_data_list) > 0:

            async with await self.db.sessionmaker() as session:

                async with session.begin():
                    existing_obj_set: Final = set()
                    moon_yield_query: Final = sqlalchemy.select(EveTables.MoonYield)
                    moon_yield_query_result = await session.execute(moon_yield_query)
                    existing_obj_set |= {result for result in moon_yield_query_result.scalars()}

                    obj_set: Final = set()
                    for md in moon_data_list:
                        obj = EveTables.MoonYield(type_id=md.type_id, system_id=md.system_id, planet_id=md.planet_id, moon_id=md.moon_id, yield_percent=md.yield_percent)
                        obj_set.add(obj)

                    if len(existing_obj_set) > 0:
                        [await session.delete(x) for x in existing_obj_set]

                    if len(obj_set) > 0:
                        session.add_all(obj_set)

                    if any([len(existing_obj_set) > 0, len(obj_set) > 0]):
                        await session.commit()
