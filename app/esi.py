import asyncio
import inspect
import logging
import typing
import urllib.parse

import aiohttp
import aiohttp.client_exceptions
import redis.asyncio
import yarl

from support.telemetry import otel, otel_add_error

from .constants import AppConstants


class AppESI:

    self: typing.ClassVar = None

    @classmethod
    def factory(cls, logger: logging.Logger | None = None):
        if cls.self is None:
            cls.self = cls(logger)
        return cls.self

    def __init__(self, logger: logging.Logger | None) -> None:
        self.logger: typing.Final = logger or logging.getLogger(self.__class__.__name__)
        self.redis: typing.Final = redis.asyncio.from_url("redis://localhost/1")

    async def url(self, url: str, params: dict | None = None) -> yarl.URL:
        u = yarl.URL(url)
        if params is not None:
            u = u.update_query(params)
        return u

    @otel
    async def get(self, http_session: aiohttp.ClientSession, url: str, request_params: dict | None = None) -> list | None:

        request_headers: typing.Final = dict()

        # u = self.url(url, request_params)
        # request_etag: typing.Final = await self.redis.getex(str(u))
        # if request_etag is not None:
        #     request_headers['etag'] = request_etag

        # request_params = request_params or dict()
        attempts_remaining = AppConstants.ESI_ERROR_RETRY_COUNT
        while attempts_remaining > 0:
            async with await http_session.get(url, headers=request_headers, params=request_params) as response:
                if response.status in [200]:
                    return await response.json()
                else:
                    attempts_remaining -= 1
                    otel_add_error(f"{response.url} -> {response.status}")
                    self.logger.warning(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {response.url} -> {response.status} / {await response.text()}")
                    if response.status in [400, 403]:
                        attempts_remaining = 0
                    if attempts_remaining > 0:
                        await asyncio.sleep(AppConstants.ESI_ERROR_SLEEP_TIME * AppConstants.ESI_ERROR_SLEEP_MODIFIERS.get(response.status, 1))

        return None

    @otel
    async def post(self, http_session: aiohttp.ClientSession, url: str, body: dict, request_params: dict | None = None) -> list | None:

        request_headers: typing.Final = dict()

        attempts_remaining = AppConstants.ESI_ERROR_RETRY_COUNT
        while attempts_remaining > 0:
            async with await http_session.post(url, headers=request_headers, data=body, params=request_params) as response:
                if response.status in [200]:
                    return await response.json()
                else:
                    attempts_remaining -= 1
                    otel_add_error(f"{response.url} -> {response.status}")
                    self.logger.warning(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {response.url} -> {response.status} / {await response.text()}")
                    if response.status in [400, 403]:
                        attempts_remaining = 0
                    if attempts_remaining > 0:
                        await asyncio.sleep(AppConstants.ESI_ERROR_SLEEP_TIME * AppConstants.ESI_ERROR_SLEEP_MODIFIERS.get(response.status, 1))

        return None

    @staticmethod
    def valid_url(url: str) -> bool:
        return True
        try:
            u: urllib.parse.ParseResult = urllib.parse.urlunparse(url)
            return all([u.scheme, u.netloc])
        except Exception as ex:
            self: typing.Final = AppESI.factory()
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {url} -> {ex}")
            return False

    @staticmethod
    async def get_url(http_session: aiohttp.ClientSession, url: str, request_params: dict | None = None) -> list | None:

        self: typing.Final = AppESI.factory()
        return await self.get(http_session, url, request_params)

    @staticmethod
    async def post_url(http_session: aiohttp.ClientSession, url: str, request_body: dict, request_params: dict | None = None) -> list | None:

        self: typing.Final = AppESI.factory()
        return await self.post(http_session, url, request_body, request_params)
