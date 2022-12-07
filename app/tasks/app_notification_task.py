import asyncio
import collections
import collections.abc
import datetime
import inspect
import logging
import os
import pprint
import traceback
import typing
import urllib.parse

import discord
import discord.app_commands
import discord.ext.commands
import discord.ui
import opentelemetry.trace

from support.telemetry import otel, otel_add_exception

from .. import (AppDatabase, AppSSO, AppTables, MoonExtractionCompletedEvent,
                MoonExtractionScheduledEvent, StructureStateChangedEvent)
from .task import AppDatabaseTask


class DiscordStructures(discord.Client):

    GUILD: typing.Final = discord.Object(id=1047924757351370792)
    USERNAME: typing.Final = "structures"

    def __init__(self, intents: discord.Intents, messages: asyncio.Queue, *args, **kwargs: typing.Any) -> None:
        intents = intents or discord.Intents.default()
        super().__init__(intents=intents, *args, **kwargs)

        self._default_channels: typing.Final[dict[int, discord.TextChannel]] = dict()

        self.tree = discord.app_commands.CommandTree(self)
        self._messages: typing.Final = messages
        self._message_task = None

    async def setup_hook(self) -> None:
        print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
        self._message_task = self.loop.create_task(self.message_task())

    async def message_task(self) -> None:
        await self.wait_until_ready()
        while not self.is_closed():
            msg = await self._messages.get()
            print(msg)
            await self._default_channels[self.GUILD.id].send(str(msg))

    async def on_ready(self):
        print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {self.user} ({self.user.id} / {self.user.name})")
        await self.user.edit(username=self.USERNAME)
        for ch in self.get_all_channels():
            if ch.type == discord.ChannelType.text:
                print(f"{ch.guild}/{ch.name} {ch.last_message}")
                if ch.name == 'structures':
                    self._default_channels[ch.guild.id] = ch
        pprint.pprint(self._default_channels)
        await self._default_channels[self.GUILD.id].send("HELLO")
                


    async def on_message(self, message: discord.Message):
        if any([message.author.bot, message.author.id == self.user.id]):
            return

        print(str(message))
        await message.add_reaction('\N{WHITE HEAVY CHECK MARK}')
        await message.channel.send("sup", delete_after=10.0)


class AppStructureNotificationTask(AppDatabaseTask):

    @otel
    def __init__(self, client_session: collections.abc.MutableMapping, db: AppDatabase, outbound: asyncio.Queue, logger: logging.Logger | None = None) -> None:
        super().__init__(client_session, db, outbound, logger)

        intents = discord.Intents(messages=True, guilds=True)
        self._discord_messages: typing.Final = asyncio.Queue()
        self._discord = DiscordStructures(intents, self._discord_messages)

    async def run(self, client_session: collections.abc.MutableSet):
        # tracer: typing.Final = opentelemetry.trace.get_tracer_provider().get_tracer(inspect.currentframe().f_code.co_name)
        task: typing.Final = asyncio.create_task(self._discord.start(token=os.getenv("DISCORD_BOT_TOKEN"), reconnect=True))
        while True:
            msg = await self.outbound.get()
            self.logger.info(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {msg}")
            await self._discord_messages.put(msg)
        task.cancel()