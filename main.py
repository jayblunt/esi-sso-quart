import asyncio
import datetime
import inspect
import logging
import os
import typing
import uuid
import wsgiref.handlers

import opentelemetry.instrumentation.asgi
import quart
import quart.sessions
import quart_session

from app import (AppDatabase, AppFunctions, AppRequest, AppSSO, AppTables,
                 AppTemplates)
from app.tasks import (AppAccessControlTask, AppMoonYieldTask,
                       AppStructureNotificationTask, AppStructurePollingTask,
                       AppStructureTask, AppTask, ESIAllianceBackfillTask,
                       ESIUniverseConstellationsBackfillTask,
                       ESIUniverseRegionsBackfillTask,
                       ESIUniverseSystemsBackfillTask)
# from support.middleware import RateLimiterMiddleware
from support.telemetry import otel, otel_initialize

app: typing.Final = quart.Quart(__name__)

app.logger.setLevel(logging.INFO)

app.config.from_mapping(
    {
        "DEBUG": False,
        "PORT": 5050,
        "SECRET_KEY": uuid.uuid4().hex,
        "SESSION_TYPE": "redis",
        "SESSION_REVERSE_PROXY": True,
        "BASEDIR": os.path.dirname(os.path.realpath(__file__)),
        "EVEONLINE_CLIENT_ID": os.getenv("EVEONLINE_CLIENT_ID", ""),
        "EVEONLINE_CLIENT_SECRET": os.getenv("EVEONLINE_CLIENT_SECRET", ""),
        "SQLALCHEMY_DB_URL": os.getenv("SQLALCHEMY_DB_URL", ""),
        "TEMPLATES_AUTO_RELOAD": True,
        # "SEND_FILE_MAX_AGE_DEFAULT": 300,
        "SEND_FILE_MAX_AGE_DEFAULT": 30,
        "MAX_CONTENT_LENGTH": 512 * 1024,
        "BODY_TIMEOUT": 15,
        "RESPONSE_TIMEOUT": 15,
    }
)

evesso_config: typing.Final = {
    "client_id": app.config.get("EVEONLINE_CLIENT_ID"),
    "client_secret": app.config.get("EVEONLINE_CLIENT_SECRET"),
    "scopes": [
        "publicData",
        "esi-characters.read_corporation_roles.v1",
        "esi-corporations.read_structures.v1",
        "esi-industry.read_corporation_mining.v1"
    ]
}

evedb: typing.Final = AppDatabase(
    app.config.get("SQLALCHEMY_DB_URL", "sqlite+pysqlite://"),
)
quart_session.Session(app)
eveevents: typing.Final = asyncio.Queue()
evesso: typing.Final = AppSSO(app, evedb, eveevents, **evesso_config)
evesession: typing.Final = app.session_interface.session_class(sid="global", permanent=False)
evesession[AppTask.CONFIGDIR] = os.path.abspath(os.path.join(app.config.get("BASEDIR", "."), "data"))


@app.before_serving
@otel
async def _before_serving() -> None:
    if not bool(evesession.get("setup_tasks_started", False)):
        evesession["setup_tasks_started"] = True

        AppStructureNotificationTask(evesession, evedb, eveevents, app.logger)

        ESIUniverseRegionsBackfillTask(evesession, evedb, eveevents, app.logger)
        ESIUniverseConstellationsBackfillTask(evesession, evedb, eveevents, app.logger)
        ESIUniverseSystemsBackfillTask(evesession, evedb, eveevents, app.logger)
        ESIAllianceBackfillTask(evesession, evedb, eveevents, app.logger)

        AppAccessControlTask(evesession, evedb, eveevents, app.logger)
        AppMoonYieldTask(evesession, evedb, eveevents, app.logger)

        AppStructurePollingTask(evesession, evedb, eveevents, app.logger)


@app.errorhandler(404)
@otel
async def error_404(path: str) -> quart.Response:
    return quart.redirect("/")


@app.route("/usage/", methods=["GET"])
@otel
async def _usage() -> quart.Response:

    ar: typing.Final[AppRequest] = await AppFunctions.get_app_request(evedb, quart.session, quart.request)
    if ar.character_id > 0 and ar.permitted:

        permitted_data: typing.Final = list()
        denied_data: typing.Final = list()

        try:
            async with await evedb.sessionmaker() as session:
                permitted_data += await AppFunctions.get_usage(session, True, ar.ts)
                denied_data += await AppFunctions.get_usage(session, False, ar.ts)

        except Exception as ex:
            app.logger.error(f"{inspect.currentframe().f_code.co_name}: {ex}")

        return await quart.render_template(
            "usage.html",
            character_id=ar.character_id,
            permitted_usage=permitted_data, denied_usage=denied_data)

    return quart.redirect("/")


@app.route("/about/", methods=["GET"])
@otel
async def _about() -> quart.Response:

    _: typing.Final = await AppFunctions.get_app_request(evedb, quart.session, quart.request)

    return await quart.render_template("about.html")


@app.route('/moon', defaults={'moon_id': 0})
@app.route('/moon/<int:moon_id>')
@otel
async def _moon(moon_id: int) -> quart.Response:

    ar: typing.Final[AppRequest] = await AppFunctions.get_app_request(evedb, quart.session, quart.request)
    if ar.character_id > 0 and ar.permitted:

        moon_history: typing.Final = list()
        moon_yield: typing.Final = list()

        try:
            async with await evedb.sessionmaker() as session:
                moon_history += await AppFunctions.get_moon_history(session, moon_id, ar.ts)
                moon_yield += await AppFunctions.get_moon_yield(session, moon_id, ar.ts)

        except Exception as ex:
            app.logger.error(f"{inspect.currentframe().f_code.co_name}: {ex}")

        time_chunking = 3
        return await quart.render_template(
            "moon.html",
            character_id=ar.character_id,
            moon_id=moon_id,
            moon_history=moon_history,
            moon_yield=moon_yield,
            weekday_names=['M', 'T', 'W', 'T', 'F', 'S', 'S'],
            timeofday_names=[f"{(x-time_chunking):02d}-{(x):02d}" for x in range(time_chunking, 24 + time_chunking) if x % time_chunking == 0],
        )

    return quart.redirect("/")


@app.route("/", methods=["GET"])
@otel
async def _root() -> quart.Response:

    ar: typing.Final[AppRequest] = await AppFunctions.get_app_request(evedb, quart.session, quart.request)
    if ar.character_id > 0 and ar.permitted:

        if bool(ar.session.get(AppSSO.ESI_CHARACTER_IS_STATION_MANAGER_ROLE, False)):
            AppStructureTask(ar.session, evedb, eveevents, app.logger)

        active_timer_results: typing.Final[list[AppTables.Structure]] = list()
        completed_extraction_results: typing.Final = list()
        scheduled_extraction_results: typing.Final = list()
        structure_fuel_results: typing.Final[list[AppTables.Structure]] = list()
        last_update_results: typing.Final = list()
        # page_expires = ar.ts + datetime.timedelta(minutes=5)

        try:
            async with await evedb.sessionmaker() as session:
                active_timer_results += await AppFunctions.get_active_timers(session, ar.ts)
                completed_extraction_results += await AppFunctions.get_completed_extractions(session, ar.ts)
                scheduled_extraction_results += await AppFunctions.get_scheduled_extractions(session, ar.ts)
                structure_fuel_results += await AppFunctions.get_structure_fuel_expiries(session, ar.ts)
                last_update_results += await AppFunctions.get_refresh_times(session, ar.ts)

        except Exception as ex:
            app.logger.error(f"{inspect.currentframe().f_code.co_name}: {ex}")

        return await app.make_response(
            await quart.render_template(
                "home.html",
                character_id=ar.character_id,
                active_timers=active_timer_results,
                completed_extractions=completed_extraction_results,
                scheduled_extractions=scheduled_extraction_results,
                structure_fuel_expiries=structure_fuel_results,
                last_update=last_update_results,
                character_trusted=ar.trusted,
                character_contributor=ar.contributor,
            ),
        )

    elif ar.character_id > 0 and not ar.permitted:
        app.logger.warning(f"{ar.character_id} not permitted")
        return await quart.render_template(
            "permission.html",
            character_id=ar.character_id,
        )

    return await quart.render_template("login.html")


if __name__ == "__main__":

    # logging.basicConfig(level=logging.DEBUG)
    otel_initialize()

    AppTemplates.add_templates(app, evedb)

    app_debug: typing.Final = app.config.get("DEBUG", False)
    app_port: typing.Final = app.config.get("PORT", 5050)
    app_host: typing.Final = app.config.get("HOST", "127.0.0.1")

    # app_log_file: typing.Final = os.path.join(app.config.get('BASEDIR', os.path.basename(os.path.abspath(os.path.splitext(__file__)[0]))), "logs", "app.log")
    # app_log_dir: typing.Final = os.path.dirname(app_log_file)
    # if not os.path.isdir(app_log_dir):
    #     os.makedirs(app_log_dir, 0o755)

    # logging.basicConfig(level=logging.INFO, filename=app_log_file)

    if app_debug:
        app.run(host=app_host, port=app_port, debug=app_debug)
    else:
        import hypercorn.asyncio
        import hypercorn.config
        from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

        app_trusted_hosts: typing.Final = ["127.0.0.1", "::1"]
        app_bind_hosts: typing.Final = [x for x in app_trusted_hosts]

        # XXX: hack for development server.
        development_flag_file = os.path.join(app.config.get("BASEDIR", "."), "development.txt")
        if os.path.exists(development_flag_file):
            with open(development_flag_file) as ifp:
                app_bind_hosts.clear()
                app_bind_hosts.append("0.0.0.0")
                for line in [line.strip() for line in ifp.readlines()]:
                    app_trusted_hosts.append(line)

        config: typing.Final = hypercorn.config.Config()
        config.bind = [f"{host}:{app_port}" for host in app_bind_hosts]
        config.accesslog = "-"

        async def async_main():
            await evedb._initialize()

            app.asgi_app = opentelemetry.instrumentation.asgi.OpenTelemetryMiddleware(
                app.asgi_app
            )

            # app.asgi_app = RateLimiterMiddleware(
            #     app.asgi_app,
            #     threshold=32
            # )

            app.asgi_app = ProxyHeadersMiddleware(
                app.asgi_app, trusted_hosts=app_trusted_hosts
            )

            await hypercorn.asyncio.serve(app, config)

        asyncio.run(async_main())
