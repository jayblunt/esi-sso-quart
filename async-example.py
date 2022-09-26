import asyncio
import functools
import inspect
import logging
import typing

import aiohttp
import opentelemetry.exporter.otlp.proto.http.trace_exporter
import opentelemetry.instrumentation.aiohttp_client
import opentelemetry.sdk.resources
import opentelemetry.sdk.trace
import opentelemetry.sdk.trace.export
import opentelemetry.semconv.resource
import opentelemetry.trace


def otel(func: typing.Callable):

    if asyncio.iscoroutinefunction(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            tracer = opentelemetry.trace.get_tracer_provider().get_tracer(func.__module__)
            with tracer.start_as_current_span(func.__qualname__):
                try:
                    return await func(*args, **kwargs)
                except Exception as ex:
                    span = opentelemetry.trace.get_current_span()
                    if span.is_recording():
                        span.record_exception(ex)
                        span.set_status(opentelemetry.trace.Status(opentelemetry.trace.StatusCode.ERROR))
                    raise
        return async_wrapper
    else:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            tracer = opentelemetry.trace.get_tracer_provider().get_tracer(func.__module__)
            with tracer.start_as_current_span(func.__qualname__):
                try:
                    return func(*args, **kwargs)
                except Exception as ex:
                    span = opentelemetry.trace.get_current_span()
                    if span.is_recording():
                        span.record_exception(ex)
                        span.set_status(opentelemetry.trace.Status(opentelemetry.trace.StatusCode.ERROR))
                    raise
        return wrapper


def otel_initialize() -> opentelemetry.sdk.trace.Tracer:

    opentelemetry.instrumentation.aiohttp_client.AioHttpClientInstrumentor().instrument()

    provider: typing.Final = opentelemetry.sdk.trace.TracerProvider()
    exporter: typing.Final = opentelemetry.exporter.otlp.proto.http.trace_exporter.OTLPSpanExporter()
    processor: typing.Final = opentelemetry.sdk.trace.export.BatchSpanProcessor(exporter)
    provider.add_span_processor(processor)

    # provider: typing.Final = opentelemetry.sdk.trace.TracerProvider(
    #     opentelemetry.sdk.trace.export.BatchSpanProcessor(
    #         opentelemetry.exporter.otlp.proto.http.trace_exporter.OTLPSpanExporter()
    #     )
    # )
    opentelemetry.trace.set_tracer_provider(provider)
    return opentelemetry.trace.get_tracer_provider().get_tracer(__name__)


class AsyncGetPages():

    LIMIT_PER_HOST: typing.Final = 37
    ERROR_SLEEP_TIME: typing.Final = 7
    ERROR_RETRY_COUNT: typing.Final = 11

    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger

    @property
    def common_params(self) -> dict:
        return {
            "datasource": "tranquility",
            "language": "en",
        }

    @otel
    async def get(self, url: str) -> list[typing.Any]:

        session_headers: typing.Final = dict()

        async with aiohttp.ClientSession(headers=session_headers) as http_session:
            maxpageno: int = 0
            results: typing.Final = list()
            async with await http_session.get(url, params=self.common_params) as response:
                if response.status in [200]:
                    maxpageno = int(response.headers.get('X-Pages', 1))
                    results.extend(await response.json())
                else:
                    self.logger.warning("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"{response.url} -> {response.status}"))

            pages = list(range(2, 1 + int(maxpageno)))

            task_list: typing.Final = [asyncio.ensure_future(self._get_page_n(url, page, http_session)) for page in pages]
            if len(task_list) > 0:
                results += sum(await asyncio.gather(*task_list), [])

        return list()

    @otel
    async def _get_page_n(self, url: str, page: int, http_session: aiohttp.ClientSession) -> list:
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
                    self.logger.warning("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"{response.url} -> {response.status}"))
                    await asyncio.sleep(self.ERROR_SLEEP_TIME)
        return list()


async def async_main(logger: logging.Logger):
    page_getter: typing.Final = AsyncGetPages(logger)
    results: typing.Final = await page_getter.get("https://esi.evetech.net/v1/universe/types/")
    print(len(results))

if __name__ == '__main__':
    otel_initialize()
    logging.basicConfig(level=logging.DEBUG)
    logger: typing.Final = logging.getLogger()
    asyncio.run(async_main(logger))

