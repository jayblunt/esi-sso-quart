import asyncio
import inspect
import logging
import os
import traceback
import typing
import urllib.parse

import discord
import discord.app_commands
import discord.ext.commands
import discord.ui


class DiscordStructures(discord.Client):

    GUILD: typing.Final = discord.Object(id=1047924757351370792)
    USERNAME: typing.Final = "structures"

    def __init__(self, intents: discord.Intents | None = None, *args, **kwargs: typing.Any) -> None:
        intents = intents or discord.Intents.default()
        super().__init__(intents=intents, *args, **kwargs)

        self.tree = discord.app_commands.CommandTree(self)

        self._default_channels: typing.Final[dict[int, discord.TextChannel]] = dict()
        self._task = None

    # async def command_feedback(self, interaction: discord.Interaction):
    #     await interaction.response.send_modal(Feedback())

    # async def command_foo(self, interaction: discord.Interaction, *, query: str):
    #     await interaction.response.send_message("what", view=Google(query))

    async def setup_hook(self) -> None:
        print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
        # self.tree.add_command(discord.app_commands.Command(name="feedback", description="FEEDBACK", callback=self.command_feedback), guild=self.GUILD)
        # self.tree.add_command(discord.app_commands.Command(name="foo", description="FOO", callback=self.command_foo), guild=self.GUILD)
        # self.tree.copy_global_to(guild=self.GUILD)
        await self.tree.sync(guild=self.GUILD)
        self._task = self.loop.create_task(self.)

    async def on_ready(self):
        print(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {self.user} ({self.user.id} / {self.user.name})")
        await self.user.edit(username=self.USERNAME)
        for ch in self.get_all_channels():
            if ch.type == discord.ChannelType.text:
                print(f"{ch.guild}/{ch.name} {ch.last_message}")
                if ch.name == 'structures':
                    self._default_channels[ch.guild] = ch
                    # embed = discord.Embed(
                    #     title="structures",
                    #     url="https://structures.castabouts.net",
                    # )
                    # embed.add_field(name="1", value="one")
                    # embed.add_field(name="2", value="two")
                    # embed.add_field(name="3", value="three")


                    embed = discord.Embed(
                        title="Text Formatting",
                        url="https://realdrewdata.medium.com/",
                        description="Here are some ways to format text",
                        color=discord.Color.blue())

                    # embed.set_author(name=self.user.display_name, icon_url=self.user.avatar.url)
                    # embed.set_thumbnail(url="https://i.imgur.com/axLm3p6.jpeg")
                    embed.add_field(name="*Italics*", value="Surround your text in asterisks (\*)", inline=False)
                    embed.add_field(name="**Bold**", value="Surround your text in double asterisks (\*\*)", inline=False)
                    embed.add_field(name="__Underline__", value="Surround your text in double underscores (\_\_)", inline=False)
                    embed.add_field(name="~~Strikethrough~~", value="Surround your text in double tildes (\~\~)", inline=False)
                    embed.add_field(name="`Code Chunks`", value="Surround your text in backticks (\`)", inline=False)
                    embed.add_field(name="Blockquotes", value="> Start your text with a greater than symbol (\>)", inline=False)
                    embed.add_field(name="Secrets", value="||Surround your text with double pipes (\|\|)||", inline=False)
                    embed.set_footer(text="Learn more here: realdrewdata.medium.com")

                    r = await ch.send(embed=embed)
                    print(r)
                    # await ch.send('[structures](https://structures.castabouts.net)')

    async def on_message(self, message: discord.Message):
        if any([message.author.bot, message.author.id == self.user.id]):
            return

        print(str(message))
        await message.add_reaction('\N{WHITE HEAVY CHECK MARK}')
        await message.channel.send("sup", delete_after=10.0)


intents = discord.Intents(messages=True, guilds=True)
structures = DiscordStructures(intents=intents)


async def async_main(client: discord.Client):
    await client.start(token=os.getenv("DISCORD_BOT_TOKEN"), reconnect=True)
    while True:
        asyncio.sleep(60)

if __name__ == '__main__':
    asyncio.run(async_main(structures))
    # structures.run(os.getenv("DISCORD_BOT_TOKEN"))
