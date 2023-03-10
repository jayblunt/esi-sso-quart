import asyncio
import collections
import collections.abc
import inspect
import logging
import os
import typing

import quart

from support.telemetry import otel

from .. import (AppDatabase, AppTables, AppStructureEvent, MoonExtractionCompletedEvent,
                MoonExtractionScheduledEvent, AppFunctions)
from .task import AppDatabaseTask


class AppStructureNotificationTask(AppDatabaseTask):


    @otel
    def __init__(self, client_session: collections.abc.MutableMapping, db: AppDatabase, outbound: asyncio.Queue, logger: logging.Logger | None = None) -> None:
        super().__init__(client_session, db, outbound, logger)


    async def run(self, client_session: collections.abc.MutableSet):
        # tracer: typing.Final = opentelemetry.trace.get_tracer_provider().get_tracer(inspect.currentframe().f_code.co_name)
        while True:
            msg: AppStructureEvent = await self.outbound.get()
            self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {msg}")
