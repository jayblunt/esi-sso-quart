import abc
import asyncio
import collections.abc
import inspect
import logging
import os
from typing import Any, Final, List

import aiohttp
import aiohttp.client_exceptions
from db import EveDatabase
from sso import EveSSO


class EveTask(metaclass=abc.ABCMeta):

    LIMIT_PER_HOST: Final = 37
    ERROR_SLEEP_TIME: Final = 7
    ERROR_RETRY_COUNT: Final = 11

    CONFIGDIR: Final = "CONFIGDIR"

    def __init__(self, client_session: collections.abc.MutableMapping, db: EveDatabase, logger: logging.Logger = logging.getLogger()):
        self.client_session: Final = client_session
        self.db: Final = db
        self.logger: Final = logger
        self.name: Final = self.__class__.__name__
        self.configdir: Final = os.path.abspath(self.client_session.get(self.CONFIGDIR, "."))

        if self.client_session.get(self.name, False):
            self.task: asyncio.Task = None
        else:
            self.client_session[self.name] = True
            self.task: asyncio.Task = asyncio.create_task(self.manage_task())

    async def manage_task(self):
        self.logger.info(f"> {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")
        await self.run()
        self.client_session[self.name] = False
        self.logger.info(f"< {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}")

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
        esi_access_token: Final = self.client_session.get(EveSSO.ESI_ACCESS_TOKEN, '')
        if len(esi_access_token) > 0:
            session_headers["Authorization"] = f"Bearer {esi_access_token}"

        maxpageno: int = 1
        results: Final = list()

        async with aiohttp.ClientSession(headers=session_headers) as http_session:
            async with http_session.get(url, params=self.common_params) as response:
                self.logger.info("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"{response.url} -> {response.status}"))

                if response.status in [200]:
                    maxpageno = int(response.headers.get('X-Pages', 1))
                    results.extend(await response.json())

        pages = list(range(2, 1 + int(maxpageno)))

        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=self.LIMIT_PER_HOST), headers=session_headers) as http_session:
            task_list: Final = list()
            for page in pages:
                task_list.append(asyncio.ensure_future(self._get_page(url, page, http_session)))
            if len(task_list) > 0:
                result_list = await asyncio.gather(*task_list)
                results += sum(result_list, [])

        self.logger.info("- {}.{}: {}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  url, results))
        return results

    async def _get_page(self, url: str, page: int, http_session: aiohttp.ClientSession) -> List:
        request_params = {**self.common_params, **{
            "page": page
        }}
        attempts_remaining = self.ERROR_RETRY_COUNT
        while attempts_remaining > 0:
            async with http_session.get(url, params=request_params) as response:
                if response.status in [200]:
                    return await response.json()
                else:
                    attempts_remaining -= 1
                    self.logger.warning("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"{response.url} -> {response.status}"))
                    asyncio.sleep(self.ERROR_SLEEP_TIME)
        return []
