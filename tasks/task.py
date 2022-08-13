import abc
import asyncio
import inspect
import logging
import os
from typing import Any, Final, List, Union

import aiohttp
import aiohttp.client_exceptions
from db import EveDatabase
from sso import EveSSO


class EveSession(metaclass=abc.ABCMeta):

    def __init__(self) -> None:
        pass

    @abc.abstractmethod
    def get(self, key, default=None) -> Union[str, int, bool, List[str], List[int]]:
        return default


class EveTask(metaclass=abc.ABCMeta):

    LIMIT_PER_HOST: Final = 17
    CONFIGDIR: Final = "CONFIGDIR"

    def __init__(self, session: EveSession, db: EveDatabase, logger: logging.Logger = logging.getLogger()):
        self.session: Final = session
        self.db: Final = db
        self.logger: Final = logger
        self.name: Final = self.__class__.__name__
        self.configdir: Final = os.path.abspath(self.session.get(self.CONFIGDIR, "."))

        if self.session.get(self.name, False):
            self.task: asyncio.Task = None
        else:
            self.session[self.name] = True
            self.task: asyncio.Task = asyncio.create_task(self.manage())

    async def manage(self):
        self.logger.info("> {}.{}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name))
        await self.run()
        self.session[self.name] = False
        self.logger.info("< {}.{}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name))

    @abc.abstractmethod
    async def run():
        pass

    @property
    def common_params(self) -> dict:
        return {
            "datasource": "tranquility",
            "language": "en",
        }

    async def get_pages(self, url: str) -> List[Any]:

        session_headers: Final = dict()
        esi_access_token: Final = self.session.get(EveSSO.ESI_ACCESS_TOKEN, '')
        if len(esi_access_token) > 0:
            session_headers["Authorization"] = f"Bearer {esi_access_token}"

        maxpageno: int = 1
        results: Final = list()

        async with aiohttp.ClientSession(headers=session_headers) as client_session:
            async with client_session.get(url, params=self.common_params) as response:
                # print(f"{response.url} -> {response.status}")
                self.logger.info("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"{response.url} -> {response.status}"))

                if response.status in [200]:
                    maxpageno = int(response.headers.get('X-Pages', 1))
                    results.extend(await response.json())

        pages = list(range(2, 1 + int(maxpageno)))
        errorcount = 0

        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=self.LIMIT_PER_HOST), headers=session_headers) as client_session:
            while len(pages):
                if errorcount > 11:
                    break

                pageno = pages[0]

                request_params = {**self.common_params, **{
                    "page": pageno
                }}

                async with client_session.get(url, params=request_params) as response:
                    # print(f"{response.url} -> {response.status}")
                    self.logger.info("- {}.{}: {}".format(self.__class__.__name__,
                                     inspect.currentframe().f_code.co_name,  f"{response.url} -> {response.status}"))
                    if response.status in [200]:
                        data = await response.json()
                        if len(data) > 0:
                            results.extend(data)
                        pages.remove(pageno)
                    else:
                        errorcount += 1
                        asyncio.sleep(17)

        return results
