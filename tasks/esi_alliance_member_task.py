import asyncio
import collections.abc
import http
import inspect
import typing

import aiohttp
import aiohttp.client_exceptions
import opentelemetry.trace
import sqlalchemy
import sqlalchemy.exc
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql

from app import AppConstants, AppESIResult, AppTables, AppDatabaseTask, AppAccessType
from support.telemetry import otel, otel_add_exception


class ESIAlliancMemberTask(AppDatabaseTask):

    @otel
    async def run_once(self, alliance_id: int, /):

        corporation_id_set: typing.Final = set()

        # XXX: Add CAS to CAStabouts ...
        if alliance_id in [99002329]:
            corporation_id_set.add(1000169)

        if alliance_id > 0:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=AppConstants.ESI_LIMIT_PER_HOST)) as http_session:
                url = f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/alliances/{alliance_id}/corporations/"
                esi_result = await self.esi.get(http_session, url, request_params=self.request_params)
                if esi_result.status in [http.HTTPStatus.OK, http.HTTPStatus.NOT_MODIFIED] and esi_result.data is not None:
                    for corporation_id in list(esi_result.data):
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
                self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex=}")

        if len(corporation_id_set) > 0:
            await self.backfill_corporations(corporation_id_set)

    async def run(self, client_session: collections.abc.MutableMapping, /):
        tracer = opentelemetry.trace.get_tracer_provider().get_tracer(__name__)
        alliance_id_set: typing.Final[set[int]] = set()
        with tracer.start_as_current_span(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}"):
            try:
                async with await self.db.sessionmaker() as session:
                    session: sqlalchemy.ext.asyncio.AsyncSession

                    query = (
                        sqlalchemy.select(AppTables.AccessControls)
                        .where(AppTables.AccessControls.type == AppAccessType.ALLIANCE)
                    )

                    async for obj, in await session.stream(query):
                        obj: AppTables.AccessControls
                        alliance_id_set.add(obj.id)

            except sqlalchemy.exc.SQLAlchemyError as ex:
                otel_add_exception(ex)
                self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex=}")

            for alliance_id in alliance_id_set:
                await self.run_once(alliance_id)
