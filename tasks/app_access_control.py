import inspect
from typing import Final

import sqlalchemy
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql
from db import EveAccessType, EveTables

from .task import EveTask


class EveAccessControlTask(EveTask):

    async def run(self):

        self.logger.info(f"> {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")

        acl_bootstrap_set: Final = {
            EveTables.AccessControls(type=EveAccessType.ALLIANCE, id=99002329, permit=True),
            EveTables.AccessControls(type=EveAccessType.CORPORATION, id=1000169, permit=True),
            EveTables.AccessControls(type=EveAccessType.CORPORATION, id=98508146, permit=True),
            EveTables.AccessControls(type=EveAccessType.CORPORATION, id=98629865, permit=True),
        }

        async with await self.db.sessionmaker() as db, db.begin():
            existing_acl_set: Final = set()
            existing_query = sqlalchemy.select(EveTables.AccessControls)
            existing_query_result = await db.execute(existing_query)
            existing_acl_set |= {result for result in existing_query_result.scalars()}

            if len(existing_acl_set) == 0:
                db.add_all(acl_bootstrap_set)
                await db.commit()

        self.logger.info(f"< {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
