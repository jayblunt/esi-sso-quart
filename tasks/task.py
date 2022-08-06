import abc
import asyncio
import inspect
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

    def __init__(self, session: EveSession, db: EveDatabase):
        self.session: Final = session
        self.db: Final = db
        self.name: Final = self.__class__.__name__
        if self.session.get(self.name, False):
            self.task: asyncio.Task = None
        else:
            self.session[self.name] = True
            self.task: asyncio.Task = asyncio.create_task(self.manage())

    async def manage(self):
        print("> {}.{}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name))
        await self.run()
        self.session[self.name] = False
        print("< {}.{}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name))

    @abc.abstractmethod
    async def run():
        pass

    async def get_pages(self, url: str) -> List[Any]:

        session_headers = {
            "Authorization": f"Bearer {self.session.get(EveSSO.ESI_ACCESS_TOKEN, '')}"
        }
        common_params = {
            "datasource": "tranquility"
        }

        maxpageno: int = 1
        results: Final = list()

        async with aiohttp.ClientSession(headers=session_headers) as client_session:
            async with client_session.get(url, params=common_params) as response:
                print(f"{response.url} -> {response.status}")
                if response.status in [200]:
                    maxpageno = int(response.headers.get('X-Pages', 1))
                    results.extend(await response.json())

        async with aiohttp.ClientSession(headers=session_headers) as client_session:
            pages = list(range(2, 1 + int(maxpageno)))
            errorcount = 0
            while len(pages):
                if errorcount > 11:
                    break

                pageno = pages[0]

                request_params = {**common_params, **{
                    "page": pageno
                }}

                async with client_session.get(url, params=request_params) as response:
                    print(f"{response.url} -> {response.status}")
                    if response.status in [200]:
                        results.extend(await response.json())
                        pages.remove(pageno)
                    else:
                        errorcount += 1
                        asyncio.sleep(30)

        return results
