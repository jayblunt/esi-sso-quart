import asyncio
import functools
import typing

import opentelemetry.exporter.otlp.proto.grpc.metric_exporter
import opentelemetry.exporter.otlp.proto.http.trace_exporter
import opentelemetry.instrumentation.aiohttp_client
import opentelemetry.instrumentation.asyncpg
import opentelemetry.instrumentation.jinja2
import opentelemetry.instrumentation.system_metrics
import opentelemetry.metrics
import opentelemetry.sdk.metrics
import opentelemetry.sdk.metrics.export
import opentelemetry.sdk.resources
import opentelemetry.sdk.trace
import opentelemetry.sdk.trace.export
import opentelemetry.semconv.resource
import opentelemetry.trace

_OTEL_INITIALIZED: bool = False


def otel_initialize() -> opentelemetry.trace.Tracer:

    global _OTEL_INITIALIZED
    if not _OTEL_INITIALIZED:

        trace_exporter: typing.Final = opentelemetry.exporter.otlp.proto.http.trace_exporter.OTLPSpanExporter()

        span_processor: typing.Final = opentelemetry.sdk.trace.export.BatchSpanProcessor(trace_exporter)

        trace_provider: typing.Final = opentelemetry.sdk.trace.TracerProvider()

        trace_provider.add_span_processor(span_processor)

        opentelemetry.trace.set_tracer_provider(trace_provider)

        instrumentor = opentelemetry.instrumentation.jinja2.Jinja2Instrumentor()
        if not instrumentor.is_instrumented_by_opentelemetry:
            instrumentor.instrument

        instrumentor = opentelemetry.instrumentation.aiohttp_client.AioHttpClientInstrumentor()
        if not instrumentor.is_instrumented_by_opentelemetry:
            instrumentor.instrument

        # instrumentor = opentelemetry.instrumentation.psycopg2.Psycopg2Instrumentor()
        # if not instrumentor.is_instrumented_by_opentelemetry:
        #     instrumentor.instrument()

        instrumentor = opentelemetry.instrumentation.asyncpg.AsyncPGInstrumentor()
        if not instrumentor.is_instrumented_by_opentelemetry:
            instrumentor.instrument()

        meter_exporter: typing.Final = opentelemetry.exporter.otlp.proto.grpc.metric_exporter.OTLPMetricExporter()

        meter_provider: typing.Final = opentelemetry.sdk.metrics.MeterProvider(
            metric_readers=[opentelemetry.sdk.metrics.export.PeriodicExportingMetricReader(
                export_interval_millis=1800_000,
                exporter=meter_exporter)],
        )

        opentelemetry.metrics.set_meter_provider(meter_provider)

        instrumentor = opentelemetry.instrumentation.system_metrics.SystemMetricsInstrumentor()
        if not instrumentor.is_instrumented_by_opentelemetry:
            instrumentor.instrument()

        _OTEL_INITIALIZED = True

    return opentelemetry.trace.get_tracer_provider().get_tracer(__name__)


FuncParams = typing.ParamSpec("FuncParams")
FuncReturns = typing.TypeVar("FuncReturns")


def otel(func: typing.Callable[FuncParams, FuncReturns]) -> typing.Callable[FuncParams, FuncReturns]:

    global _OTEL_INITIALIZED

    if _OTEL_INITIALIZED:
        if asyncio.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(*args: FuncParams.args, **kwargs: FuncParams.kwargs) -> FuncReturns:
                tracer = opentelemetry.trace.get_tracer_provider().get_tracer(func.__module__)
                with tracer.start_as_current_span(f"{func.__class__.__name__}.{func.__name__}"):
                    return await func(*args, **kwargs)
            return async_wrapper
        else:
            @functools.wraps(func)
            def wrapper(*args: FuncParams.args, **kwargs: FuncParams.kwargs) -> FuncReturns:
                tracer = opentelemetry.trace.get_tracer_provider().get_tracer(func.__module__)
                with tracer.start_as_current_span(f"{func.__class__.__name__}.{func.__name__}"):
                    return func(*args, **kwargs)
            return wrapper
    else:
        if asyncio.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_noop_wrapper(*args: FuncParams.args, **kwargs: FuncParams.kwargs) -> FuncReturns:
                return await func(*args, **kwargs)
            return async_noop_wrapper
        else:
            @functools.wraps(func)
            def noop_wrapper(*args: FuncParams.args, **kwargs: FuncParams.kwargs) -> FuncReturns:
                return func(*args, **kwargs)
            return noop_wrapper


def otel_add_event(name: str, attributes: dict = dict()) -> None:
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
        span.set_status(status=opentelemetry.trace.StatusCode.ERROR, description=description)


def otel_add_exception(ex: Exception) -> None:
    global _OTEL_INITIALIZED
    if not _OTEL_INITIALIZED:
        return

    span = opentelemetry.trace.get_current_span()
    if span.is_recording():
        span.record_exception(ex)
        span.set_status(status=opentelemetry.trace.StatusCode.ERROR)
