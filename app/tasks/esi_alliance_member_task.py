import collections.abc
import inspect
import typing

import aiohttp
import aiohttp.client_exceptions
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql

from support.telemetry import otel, otel_add_exception

from .. import AppConstants, AppSSO, AppTables, AppESI
from .task import AppTask


class ESIAlliancMemberTask(AppTask):

    @otel
    async def run(self, client_session: collections.abc.MutableMapping):

        corporation_id_set: typing.Final = set()

        alliance_id: typing.Final = int(client_session.get(AppSSO.ESI_ALLIANCE_ID, 0))

        # XXX: Add CAS to CAStabouts ...
        if alliance_id in [99002329]:
            corporation_id_set.add(1000169)

        if alliance_id > 0:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=AppConstants.ESI_LIMIT_PER_HOST)) as http_session:
                url = f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/alliances/{alliance_id}/corporations/"
                response = await AppESI.get_url(http_session, url, self.request_params)
                if response is not None:
                    for corporation_id in list(response):
                        corporation_id_set.add(int(corporation_id))

        if alliance_id > 0 and len(corporation_id_set) > 0:

            try:
                async with await self.db.sessionmaker() as session:
                    session: sqlalchemy.ext.asyncio.AsyncSession

                    session.begin()

                    query = (
                        sqlalchemy.delete(AppTables.AllianceCorporation)
                        .where(AppTables.AllianceCorporation.alliance_id == alliance_id)
                    )
                    await session.execute(query)

                    obj_set = set()
                    for corporation_id in corporation_id_set:
                        session.add(AppTables.AllianceCorporation(alliance_id=alliance_id, corporation_id=corporation_id))

                    await session.commit()

            except Exception as ex:
                otel_add_exception(ex)
                self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        if len(corporation_id_set) > 0:

            try:
                async with await self.db.sessionmaker() as session, session.begin():

                    existing_corporation_id_set: typing.Final = set()

                    query = sqlalchemy.select(AppTables.Corporation)
                    async for obj in await session.stream_scalars(query):
                        obj: AppTables.Corporation
                        existing_corporation_id_set.add(obj)

                    obj_set = set()

                    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=AppConstants.ESI_LIMIT_PER_HOST)) as http_session:

                        for corporation_id in corporation_id_set - existing_corporation_id_set:

                            url = f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/corporations/{corporation_id}/"
                            response = await AppESI.get_url(http_session, url, self.request_params)
                            if response is not None:
                                edict: typing.Final = dict({
                                    "corporation_id": corporation_id
                                })

                                for k, v in dict(response).items():
                                    if k in ["alliance_id"]:
                                        v = int(v)
                                    elif k not in ["name", "ticker"]:
                                        continue
                                    edict[k] = v

                                obj = AppTables.Corporation(**edict)
                                obj_set.add(obj)

                    if len(obj_set) > 0:
                        session.add_all(obj_set)
                        await session.commit()

            except Exception as ex:
                otel_add_exception(ex)
                self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")
