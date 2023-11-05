import asyncio
import collections
import collections.abc
import inspect
import logging
import pprint
import typing

import discord
import discord.app_commands
import discord.ext.commands
import discord.ui
import quart

from app import (AppConstants, AppDatabase, AppDatabaseTask, AppFunctions,
                 AppStructureEvent, MoonExtractionCompletedEvent,
                 MoonExtractionScheduledEvent, SSOEvent, SSOLoginEvent,
                 SSOLogoutEvent, StructureStateChangedEvent)
from support.telemetry import otel


class AppStructureNotificationTask(AppDatabaseTask):

    @otel
    def __init__(self, client_session: collections.abc.MutableMapping, db: AppDatabase, outbound: asyncio.Queue, logger: logging.Logger | None = None) -> None:
        super().__init__(client_session, db, outbound, logger)

    async def run(self, client_session: collections.abc.MutableMapping):
        while True:
            msg: AppStructureEvent = await self.outbound.get()
            self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {msg=}")
