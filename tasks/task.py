import asyncio
from typing import Final

import quart
import quart.sessions


class EveTask:

    def __init__(self, session: quart.sessions.SessionMixin):
        self.session: Final = session
        self.name: Final = self.__class__.__name__
        if self.session.get(self.name, False):
            self.task: asyncio.Task = None
        else:
            self.session[self.name] = True
            self.task: asyncio.Task = asyncio.create_task(self.run())

    async def run(self):
        del self.session[self.name]
