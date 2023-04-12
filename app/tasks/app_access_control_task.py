import collections.abc
import inspect
import typing

import sqlalchemy
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql

from support.telemetry import otel, otel_add_exception

from .. import AppAccessType, AppTables
from .task import AppTask


class AppAccessControlTask(AppTask):

    @otel
    async def run(self, client_session: collections.abc.MutableMapping):

        self.logger.info(f"> {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")

        acl_bootstrap_set: typing.Final = {
            AppTables.AccessControls(type=AppAccessType.ALLIANCE, id=99002329, permit=True, trust=True),
            AppTables.AccessControls(type=AppAccessType.CORPORATION, id=1000169, permit=True, trust=True),
            AppTables.AccessControls(type=AppAccessType.CORPORATION, id=98508146, permit=True, trust=True),
            AppTables.AccessControls(type=AppAccessType.CORPORATION, id=98629865, permit=True, trust=True),
        }

        try:
            async with await self.db.sessionmaker() as session, session.begin():
                session: sqlalchemy.ext.asyncio.AsyncSession

                existing_acl_set: typing.Final = set()
                query = sqlalchemy.select(AppTables.AccessControls)
                query_result: sqlalchemy.engine.Result = await session.execute(query)
                [existing_acl_set.add(x) for x in query_result.scalars()]

                if len(existing_acl_set) == 0:
                    session.add_all(acl_bootstrap_set)

                await session.commit()

        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        self.logger.info(f"< {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
