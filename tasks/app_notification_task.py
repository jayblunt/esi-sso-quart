import asyncio
import collections
import collections.abc
import inspect
import logging

from app import AppDatabase, AppDatabaseTask, AppESI, AppStructureEvent
from support.telemetry import otel


class AppEventConsumerTask(AppDatabaseTask):

    @otel
    def __init__(self, client_session: collections.abc.MutableMapping, esi: AppESI, db: AppDatabase, eventqueue: asyncio.Queue, logger: logging.Logger | None = None) -> None:
        super().__init__(client_session, esi, db, eventqueue, logger)

    async def run(self, client_session: collections.abc.MutableMapping):
        while True:
            msg: AppStructureEvent = await self.outbound.get()
            self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {msg=}")
