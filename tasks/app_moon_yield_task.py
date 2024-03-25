import asyncio
import collections
import collections.abc
import inspect
import json
import logging
import os
import typing

import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql

from app import AppDatabaseTask, AppTables
from support.telemetry import otel, otel_add_exception


class MoonYieldData(typing.NamedTuple):
    type_id: int
    system_id: int
    planet_id: int
    moon_id: int
    yield_percent: float


class AppMoonYieldTask(AppDatabaseTask):

    @otel
    async def run(self, client_session: collections.abc.MutableSet):

        self.logger.info(f"> {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")

        moon_data_filename: typing.Final = os.path.abspath(os.path.join(self.configdir, "moon_data.json"))

        @otel
        def read_moon_data(logger: logging.Logger, filename: str) -> list:

            moon_data_list: typing.Final = list()

            try:
                if not os.path.exists(moon_data_filename):
                    logger.error(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: moon_data_filename:{moon_data_filename}")

                if os.path.exists(moon_data_filename):
                    with open(moon_data_filename) as ifp:
                        [moon_data_list.append(MoonYieldData(**edict)) for edict in json.load(ifp)]

            except Exception as ex:
                otel_add_exception(ex)
                logger.error(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex=}")

            return moon_data_list

        moon_data_list: typing.Final = await asyncio.to_thread(read_moon_data, self.logger, moon_data_filename)

        if len(moon_data_list) == 0:
            self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {moon_data_list=}")
            self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {moon_data_filename=}")

        type_id_set: typing.Final = set()

        bootstrap = {
            35835, 35825, 35832, 35826, 35836, 35833, 45496, 45499, 45501, 45513, 45495,
            45500, 45494, 45490, 45492, 45504, 45511, 45493, 45491, 45497, 45498, 45510,
            45512, 46280, 46282, 46292, 46288, 46284, 46302, 46296, 46281, 46297, 46293,
            46298, 46316, 46308, 46318, 46283, 46300, 46301, 46303}
        for x in bootstrap:
            type_id_set.add(x)

        if len(moon_data_list) > 0:

            existing_id_set: typing.Final = set(map(lambda x: x.moon_id, moon_data_list))
            if len(existing_id_set) > 0:
                try:
                    async with await self.db.sessionmaker() as session, session.begin():
                        session: sqlalchemy.ext.asyncio.AsyncSession

                        query = (
                            sqlalchemy.delete(AppTables.MoonYield)
                            .where(AppTables.MoonYield.moon_id.in_(existing_id_set))
                        )
                        await session.execute(query)
                        await session.commit()

                except Exception as ex:
                    otel_add_exception(ex)
                    self.logger.error(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex=}")

            try:
                async with await self.db.sessionmaker() as session, session.begin():

                    obj_set: typing.Final = set()
                    for md in moon_data_list:
                        md: MoonYieldData
                        obj = AppTables.MoonYield(type_id=md.type_id, system_id=md.system_id, planet_id=md.planet_id, moon_id=md.moon_id, yield_percent=md.yield_percent)
                        obj_set.add(obj)
                        type_id_set.add(obj.type_id)

                    if len(obj_set) > 0:
                        session.add_all(obj_set)
                        await session.commit()

            except Exception as ex:
                otel_add_exception(ex)
                self.logger.error(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex=}")

        if len(type_id_set) > 0:
            await self.backfill_types(type_id_set)

        self.logger.info(f"< {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
