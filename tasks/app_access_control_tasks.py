import collections.abc
import inspect
import typing

import sqlalchemy
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql

from db import EveAccessType, EveTables
from telemetry import otel, otel_add_exception

from .task import EveTask


class EveAccessControlTask(EveTask):

    @otel
    async def run(self, client_session: collections.abc.MutableMapping):

        self.logger.info(f"> {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")

        acl_bootstrap_set: typing.Final = {
            EveTables.AccessControls(type=EveAccessType.ALLIANCE, id=99002329, permit=True),
            EveTables.AccessControls(type=EveAccessType.CORPORATION, id=1000169, permit=True),
            EveTables.AccessControls(type=EveAccessType.CORPORATION, id=98508146, permit=True),
            EveTables.AccessControls(type=EveAccessType.CORPORATION, id=98629865, permit=True),
        }

        try:
            async with await self.db.sessionmaker() as session, session.begin():

                existing_acl_set: typing.Final = set()
                query = sqlalchemy.select(EveTables.AccessControls)
                query_result: sqlalchemy.engine.Result = await session.execute(query)
                existing_acl_set |= {x for x in query_result.scalars()}

                if len(existing_acl_set) == 0:
                    session.add_all(acl_bootstrap_set)
                    await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        self.logger.info(f"< {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
