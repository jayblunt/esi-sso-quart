import abc
import asyncio
import collections.abc
import inspect
import logging
import os
from typing import Any, Final

import aiohttp
import aiohttp.client_exceptions
from db import EveDatabase
from telemetry import otel, otel_add_error


class EveTask(metaclass=abc.ABCMeta):

    LIMIT_PER_HOST: Final = 37
    ERROR_SLEEP_TIME: Final = 7
    ERROR_RETRY_COUNT: Final = 11

    CONFIGDIR: Final = "CONFIGDIR"

    def __init__(self, client_session: collections.abc.MutableMapping, db: EveDatabase, logger: logging.Logger = logging.getLogger()):
        self.db: Final = db
        self.logger: Final = logger
        self.name: Final = self.__class__.__name__
        self.configdir: Final = os.path.abspath(client_session.get(self.CONFIGDIR, "."))

        if client_session.get(self.name, False):
            self.task: asyncio.Task = None
        else:
            client_session[self.name] = True
            self.task: asyncio.Task = asyncio.create_task(self.manage_task(client_session))

    async def manage_task(self, client_session: collections.abc.MutableMapping):
        await self.run(client_session)
        client_session[self.name] = False

    @abc.abstractmethod
    async def run(self, client_session: collections.abc.MutableMapping):
        pass

    @property
    def common_params(self) -> dict:
        return {
            "datasource": "tranquility",
            "language": "en",
        }

    @otel
    async def get_pages(self, url: str, access_token: str) -> list[Any]:

        session_headers: Final = dict()
        if len(access_token) > 0:
            session_headers["Authorization"] = f"Bearer {access_token}"

        maxpageno: int = 1
        results: Final = list()

        async with aiohttp.ClientSession(headers=session_headers) as http_session:
            async with await http_session.get(url, params=self.common_params) as response:
                if response.status in [200]:
                    maxpageno = int(response.headers.get('X-Pages', 1))
                    results.extend(await response.json())
                else:
                    otel_add_error(f"{response.url} -> {response.status}")
                    self.logger.warning("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"{response.url} -> {response.status}"))

            pages = list(range(2, 1 + int(maxpageno)))

            task_list: Final = list()
            for page in pages:
                task_list.append(asyncio.ensure_future(self._get_page(url, page, http_session)))
            if len(task_list) > 0:
                result_list = await asyncio.gather(*task_list)
                results += sum(result_list, [])

        return results

    async def _get_page(self, url: str, page: int, http_session: aiohttp.ClientSession) -> list:
        request_params = {**self.common_params, **{
            "page": page
        }}
        attempts_remaining = self.ERROR_RETRY_COUNT
        while attempts_remaining > 0:
            async with await http_session.get(url, params=request_params) as response:
                if response.status in [200]:
                    return await response.json()
                else:
                    attempts_remaining -= 1
                    otel_add_error(f"{response.url} -> {response.status}")
                    self.logger.warning("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"{response.url} -> {response.status}"))
                    await asyncio.sleep(self.ERROR_SLEEP_TIME)
        return list()
