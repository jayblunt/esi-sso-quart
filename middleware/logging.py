import pprint

import asgiref.typing


class LoggingMiddleware(object):
    def __init__(self, app: asgiref.typing.ASGI3Application):
        self.app = app

    async def __call__(self, scope: asgiref.typing.Scope, receive: asgiref.typing.ASGIReceiveCallable, send: asgiref.typing.ASGISendCallable):
        if scope["type"] in ("http", "websocket"):
            pprint.pprint(dict(scope["headers"]))
        return await self.app(scope, receive, send)
