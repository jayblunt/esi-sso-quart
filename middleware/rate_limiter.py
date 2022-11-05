import asyncio
import collections
import typing

import asgiref.typing


class RateLimiterMiddleware:
    def __init__(
        self,
        app: asgiref.typing.ASGI3Application,
        threshold: int = 16,
        interval: float = 0.25,
    ) -> None:
        self.app: typing.Final = app
        self.threshold: typing.Final = threshold
        self.interval: typing.Final = interval
        self.source_counter: typing.Final = collections.defaultdict(int)
        self.task = None


    def __del__(self):
        if self.task is not None:
            if not self.task.cancelled():
                self.task.cancel()


    async def decrement_task(self):
        while True:
            await asyncio.sleep(self.interval)

            delete_keys = set()

            for k in self.source_counter.keys():
                if self.source_counter[k] > 0:
                    self.source_counter[k] -= 1
                else:
                    delete_keys.add(k)

            for k in delete_keys:
                del self.source_counter[k]

            if len(self.source_counter.keys()) == 0:
                if not self.task.cancelled():
                    self.task.cancel()
                self.task = None


    async def nope(self, scope: asgiref.typing.Scope, receive: asgiref.typing.ASGIReceiveCallable, send: asgiref.typing.ASGISendCallable):
        await asyncio.sleep(self.interval * self.threshold)
        await send({
            'type': 'http.disconnect',
        })
        # await send({
        #     'type': 'http.response.start',
        #     'status': 400,
        #     'headers': [(b'content-length', b'0')],
        # })
        # await send({
        #     'type': 'http.response.body',
        #     'body': b'',
        #     'more_body': False,
        # })


    async def __call__(
        self, scope: asgiref.typing.Scope, receive: asgiref.typing.ASGIReceiveCallable, send: asgiref.typing.ASGISendCallable
    ) -> None:
        if scope["type"] in ("http", "websocket"):
            if self.task is None:
                self.task = asyncio.create_task(self.decrement_task())

            client_host, _ = scope["client"]
            self.source_counter[client_host] += 1
            print(f"{client_host}: {self.source_counter[client_host]}")
            if self.source_counter[client_host] >= self.threshold:
                return await self.nope(scope, receive, send)

        return await self.app(scope, receive, send)
