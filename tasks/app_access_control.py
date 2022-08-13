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
from db import EveTables, EveAccessType

from .task import EveTask


class EveAccessControlTask(EveTask):

    async def run(self):

        self.logger.info("> {}.{}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name))

        acl_bootstrap_set: Final = {
            EveTables.AccessControls(type=EveAccessType.ALLIANCE, id=99002329, permit=True),
            EveTables.AccessControls(type=EveAccessType.CORPORATION, id=1000169, permit=True),
            EveTables.AccessControls(type=EveAccessType.CORPORATION, id=98508146, permit=True),
            EveTables.AccessControls(type=EveAccessType.CORPORATION, id=98629865, permit=True),
        }

        async with await self.db.sessionmaker() as session:
            existing_acl_set: Final = set()
            async with session.begin():
                existing_query = sqlalchemy.select(EveTables.AccessControls)
                existing_query_result = await session.execute(existing_query)
                existing_acl_set |= {result for result in existing_query_result.scalars()}

            if len(existing_acl_set) == 0:
                for acl in acl_bootstrap_set:
                    async with session.begin():
                        session.add(acl)
                        await session.commit()

        self.logger.info("< {}.{}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name))
