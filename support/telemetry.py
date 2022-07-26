import asyncio
import functools
import typing

import opentelemetry.exporter.otlp.proto.http.trace_exporter
import opentelemetry.instrumentation.aiohttp_client
import opentelemetry.instrumentation.asyncpg
import opentelemetry.sdk.resources
import opentelemetry.sdk.trace
import opentelemetry.sdk.trace.export
import opentelemetry.semconv.resource
import opentelemetry.trace


_OTEL_INITIALIZED: bool = False


def otel_initialize() -> opentelemetry.sdk.trace.Tracer:

    global _OTEL_INITIALIZED
    if not _OTEL_INITIALIZED:

        # opentelemetry.instrumentation.aiohttp_client.AioHttpClientInstrumentor().instrument()
        opentelemetry.instrumentation.asyncpg.AsyncPGInstrumentor().instrument()

        provider: typing.Final = opentelemetry.sdk.trace.TracerProvider()

        exporter: typing.Final = opentelemetry.exporter.otlp.proto.http.trace_exporter.OTLPSpanExporter()

        processor: typing.Final = opentelemetry.sdk.trace.export.BatchSpanProcessor(exporter)

        provider.add_span_processor(processor)

        opentelemetry.trace.set_tracer_provider(provider)

        _OTEL_INITIALIZED = True

    return opentelemetry.trace.get_tracer_provider().get_tracer(__name__)


def otel(func: typing.Callable) -> typing.Callable:

    global _OTEL_INITIALIZED

    if _OTEL_INITIALIZED:
        if asyncio.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs) -> typing.Any | None:
                tracer = opentelemetry.trace.get_tracer_provider().get_tracer(func.__module__)
                with tracer.start_as_current_span(func.__qualname__):
                    return await func(*args, **kwargs)
            return async_wrapper
        else:
            @functools.wraps(func)
            def wrapper(*args, **kwargs) -> typing.Any | None:
                tracer = opentelemetry.trace.get_tracer_provider().get_tracer(func.__module__)
                with tracer.start_as_current_span(func.__qualname__):
                    return func(*args, **kwargs)
            return wrapper
    else:
        if asyncio.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs) -> typing.Any | None:
                return await func(*args, **kwargs)
            return async_wrapper
        else:
            @functools.wraps(func)
            def wrapper(*args, **kwargs) -> typing.Any | None:
                return func(*args, **kwargs)
            return wrapper


def otel_add_event(name: str, attributes: dict | None = None) -> None:
    global _OTEL_INITIALIZED
    if not _OTEL_INITIALIZED:
        return

    span = opentelemetry.trace.get_current_span()
    if span.is_recording():
        span.add_event(name, attributes={f"event.{k}": v for k, v in attributes.items()})


def otel_add_error(description: str | None = None) -> None:
    global _OTEL_INITIALIZED
    if not _OTEL_INITIALIZED:
        return

    span = opentelemetry.trace.get_current_span()
    if span.is_recording():
        span.set_status(opentelemetry.trace.Status(opentelemetry.trace.StatusCode.ERROR), description=description)


def otel_add_exception(ex: Exception) -> None:
    global _OTEL_INITIALIZED
    if not _OTEL_INITIALIZED:
        return

    span = opentelemetry.trace.get_current_span()
    if span.is_recording():
        span.record_exception(ex)
        span.set_status(opentelemetry.trace.Status(opentelemetry.trace.StatusCode.ERROR))
