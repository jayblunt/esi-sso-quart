import asyncio
import contextlib
import http
import inspect
import logging
import os
import platform
import typing
import uuid

import colorlog
import hypercorn.asyncio
import hypercorn.config
import hypercorn.middleware
import opentelemetry.instrumentation.asgi
# import opentelemetry.trace
import quart
import quart.sessions
import quart_session

from app import (AppAccessEvent, AppDatabase, AppESI, AppFunctions, AppRequest,
                 AppSSO, AppSSOHookProvider, AppTables, AppTask, AppTemplates,
                 CCPSSOProvider)
from support.telemetry import otel, otel_initialize
from tasks import (AppAccessControlTask, AppEventConsumerTask,
                   AppMarketHistoryTask, AppMoonYieldTask,
                   AppStructurePollingTask, ESIAllianceBackfillTask,
                   ESINPCorporationBackfillTask,
                   ESIUniverseConstellationsBackfillTask,
                   ESIUniverseRegionsBackfillTask,
                   ESIUniverseSystemsBackfillTask)

BASEDIR: typing.Final = os.path.dirname(os.path.realpath(__file__))

for environment_variable in ['ESI_CLIENT_ID', 'ESI_SQLALCHEMY_DB_URL']:
    assert len(os.getenv(environment_variable, '')) > 0, f'{environment_variable} is required'

app: typing.Final = quart.Quart(__name__)

app.logger.setLevel(logging.INFO)
for handler in app.logger.handlers:
    if isinstance(handler, logging.StreamHandler):
        app.logger.removeHandler(handler)
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter('%(log_color)s[%(asctime)s] %(levelname)s in %(module)s: %(message)s'))
app.logger.addHandler(handler)

app.config.from_mapping({
    "DEBUG": False,
    "PORT": 5050,
    "SECRET_KEY": uuid.uuid4().hex,

    "SESSION_TYPE": "redis",
    "SESSION_REVERSE_PROXY": True,
    "SESSION_PERMANENT": True,
    "SESSION_COOKIE_HTTPONLY": True,
    "SESSION_COOKIE_SAMESITE": "Lax",
    "SESSION_COOKIE_SECURE": True,

    "TEMPLATES_AUTO_RELOAD": True,
    "SEND_FILE_MAX_AGE_DEFAULT": 300,
    "MAX_CONTENT_LENGTH": 512 * 1024,
    "BODY_TIMEOUT": 15,
    "RESPONSE_TIMEOUT": 15,
})


quart_session.Session(app)

esi: typing.Final = AppESI.factory(app.logger)
db: typing.Final = AppDatabase(
    os.getenv("ESI_SQLALCHEMY_DB_URL", ""),
    app.logger
)
eventqueue: typing.Final = asyncio.Queue()
sso = AppSSO(app, esi, db, eventqueue,
             client_id=os.getenv("ESI_CLIENT_ID", ""),
             provider=CCPSSOProvider(),
             hook_provider=AppSSOHookProvider(esi, db, eventqueue, app.logger),
             client_scopes=[
                 "publicData",
                 "esi-corporations.read_structures.v1",
                 "esi-characters.read_corporation_roles.v1",
                 "esi-industry.read_corporation_mining.v1",
             ])
evesession: typing.Final = app.session_interface.session_class(sid="global", permanent=False)
evesession[AppTask.CONFIGDIR] = os.path.abspath(os.path.join(BASEDIR, "data"))
shutdown_event: typing.Final = asyncio.Event()


@app.before_serving
@otel
async def _before_serving() -> None:
    if not bool(evesession.get("setup_tasks_started", False)):
        evesession["setup_tasks_started"] = True

        AppEventConsumerTask(evesession, esi, db, eventqueue, app.logger)

        ESIUniverseRegionsBackfillTask(evesession, esi, db, eventqueue, app.logger)
        ESIUniverseConstellationsBackfillTask(evesession, esi, db, eventqueue, app.logger)
        ESIUniverseSystemsBackfillTask(evesession, esi, db, eventqueue, app.logger)
        ESIAllianceBackfillTask(evesession, esi, db, eventqueue, app.logger)
        ESINPCorporationBackfillTask(evesession, esi, db, eventqueue, app.logger)

        AppMarketHistoryTask(evesession, esi, db, eventqueue, app.logger)
        AppAccessControlTask(evesession, esi, db, eventqueue, app.logger)
        AppMoonYieldTask(evesession, esi, db, eventqueue, app.logger)

        AppStructurePollingTask(evesession, esi, db, eventqueue, app.logger)


@app.errorhandler(http.HTTPStatus.NOT_FOUND)
async def _404(_: Exception) -> quart.ResponseReturnValue:
    return await quart.render_template("404.html"), http.HTTPStatus.NOT_FOUND


@app.route('/robots.txt', methods=["GET"])
@otel
async def _robots() -> quart.ResponseReturnValue:
    return await app.send_static_file('robots.txt')


@app.route("/usage/", methods=["GET"])
@otel
async def _usage() -> quart.ResponseReturnValue:

    ar: typing.Final[AppRequest] = await AppFunctions.get_app_request(db, quart.session, quart.request)
    if ar.character_id > 0 and ar.suspect:
        quart.session.clear()

    elif ar.character_id > 0 and ar.permitted and ar.contributor:

        permitted_data: typing.Final = list()
        denied_data: typing.Final = list()

        try:
            async with await db.sessionmaker() as session:
                permitted_data.extend(await AppFunctions.get_usage(session, True, ar.ts))
                denied_data.extend(await AppFunctions.get_usage(session, False, ar.ts))

        except Exception as ex:
            app.logger.error(f"{inspect.currentframe().f_code.co_name}: {ex=}")

        return await quart.render_template(
            "usage.html",
            character_id=ar.character_id,
            is_contributor_character=ar.contributor,
            is_magic_character=ar.magic_character,
            permitted_usage=permitted_data,
            denied_usage=denied_data
        )

    elif ar.character_id > 0:
        await eventqueue.put(AppAccessEvent(character_id=ar.character_id, url=quart.request.url, permitted=False))

    return quart.redirect("/about/")


@app.route("/about/", methods=["GET"])
@otel
async def _about() -> quart.ResponseReturnValue:

    ar: typing.Final = await AppFunctions.get_app_request(db, quart.session, quart.request)

    platform_info: typing.Final = {
        'python_version': platform.python_version(),
        'system': platform.system(),
        'release': platform.release(),
        'processor': platform.processor()
    }

    return await quart.render_template(
        "about.html",
        character_id=ar.character_id,
        is_contributor_character=ar.contributor,
        is_magic_character=ar.magic_character,
        is_about_page=True,
        platform_info=platform_info
    )


@app.route('/top', defaults={'top_type': 'characters'}, methods=["GET"])
@app.route('/top/<string:top_type>', methods=["GET"])
@otel
async def _top(top_type: str) -> quart.ResponseReturnValue:

    ar: typing.Final[AppRequest] = await AppFunctions.get_app_request(db, quart.session, quart.request)
    if ar.character_id > 0 and ar.suspect:
        quart.session.clear()

    elif ar.character_id > 0 and ar.permitted:

        if any([ar.contributor, ar.magic_character]):

            # tracer_name: typing.Final = inspect.currentframe().f_code.co_name
            # tracer: typing.Final = opentelemetry.trace.get_tracer_provider().get_tracer(tracer_name)
            history = None

            # with tracer.start_as_current_span(f"{tracer_name}.collect"):
            with contextlib.nullcontext():
                try:
                    async with await db.sessionmaker() as session:
                        history = await AppFunctions.get_moon_mining_top(session, ar.ts)

                except Exception as ex:
                    app.logger.error(f"{inspect.currentframe().f_code.co_name}: {ex=}")

            # with tracer.start_as_current_span(f"{tracer_name}.process"):
            with contextlib.nullcontext():
                top_observers: typing.Final = list()
                top_observers_isk_dict: typing.Final = dict()
                for (observer_id, isk) in history.observers:
                    top_observers.append(observer_id)
                    top_observers_isk_dict[observer_id] = isk

                top_characters: typing.Final = list()
                top_characters_isk_dict: typing.Final = dict()
                for (character_id, isk) in history.characters:
                    top_characters.append(character_id)
                    top_characters_isk_dict[character_id] = isk

            # with tracer.start_as_current_span(f"{tracer_name}.render"):
            with contextlib.nullcontext():
                return await quart.render_template(
                    "mining_rankings.html",
                    character_id=ar.character_id,
                    is_contributor_character=ar.contributor,
                    is_magic_character=ar.magic_character,
                    top_period_start=history.start_date,
                    top_period_end=history.end_date,
                    top_characters=top_characters,
                    top_characters_isk=top_characters_isk_dict,
                    top_observers=top_observers,
                    top_observers_isk=top_observers_isk_dict,
                    observer_timestamps=history.observer_timestamps,
                    observer_names=history.observer_names)

        else:
            await eventqueue.put(AppAccessEvent(character_id=ar.character_id, url=quart.request.url, permitted=False))

        return quart.redirect("/")

    elif ar.character_id > 0:
        await eventqueue.put(AppAccessEvent(character_id=ar.character_id, url=quart.request.url, permitted=False))

    return quart.redirect(sso.login_route)


@app.route('/moon', defaults={'moon_id': 0}, methods=["GET"])
@app.route('/moon/<int:moon_id>', methods=["GET"])
@otel
async def _moon(moon_id: int) -> quart.ResponseReturnValue:

    ar: typing.Final[AppRequest] = await AppFunctions.get_app_request(db, quart.session, quart.request)
    if ar.character_id > 0 and ar.suspect:
        quart.session.clear()

    elif ar.character_id > 0 and ar.permitted:

        moon_extraction_history: typing.Final = list()
        moon_mining_history: typing.Final = list()
        moon_mining_history_timestamp = None
        moon_yield: typing.Final = list()
        mined_types_set = set()
        miner_list = list()
        mined_quantity_dict = dict()
        mined_isk_dict: typing.Final = dict()
        structure = None

        try:
            async with await db.sessionmaker() as session:
                structure = await AppFunctions.get_moon_structure(session, moon_id, ar.ts)
                moon_yield.extend(await AppFunctions.get_moon_yield(session, moon_id, ar.ts))
                moon_extraction_history.extend(await AppFunctions.get_moon_extraction_history(session, moon_id, ar.ts))
                moon_mining_history_timestamp, mm_history, mm_isk = await AppFunctions.get_moon_mining_history(session, moon_id, ar.ts)
                moon_mining_history.extend(mm_history)
                mined_isk_dict.update(mm_isk)

        except Exception as ex:
            app.logger.error(f"{inspect.currentframe().f_code.co_name}: {ex=}")

        if any([ar.contributor, ar.magic_character]):
            for character_id, mining_history_dict in moon_mining_history:
                for type_id in mining_history_dict.keys():
                    mined_types_set.add(type_id)
                miner_list.append(character_id)
                mined_quantity_dict[character_id] = mining_history_dict

        time_chunking = 3
        return await quart.render_template(
            "moon.html",
            character_id=ar.character_id,
            is_contributor_character=ar.contributor,
            is_magic_character=ar.magic_character,
            moon_id=moon_id,
            structure=structure,
            moon_yield=moon_yield,
            moon_extraction_history=moon_extraction_history,
            miners=miner_list,
            mined_quantity=mined_quantity_dict,
            mined_isk=mined_isk_dict,
            mined_quantity_timestamp=moon_mining_history_timestamp,
            mined_types=sorted(list(mined_types_set)),
            weekday_names=['M', 'T', 'W', 'T', 'F', 'S', 'S'],
            timeofday_names=[f"{(x - time_chunking):02d}-{(x):02d}" for x in range(time_chunking, 24 + time_chunking) if x % time_chunking == 0],
        )

    elif ar.character_id > 0:
        await eventqueue.put(AppAccessEvent(character_id=ar.character_id, url=quart.request.url, permitted=False))

    return quart.redirect(sso.login_route)


@app.route("/", methods=["GET"])
@otel
async def _root() -> quart.ResponseReturnValue:

    ar: typing.Final[AppRequest] = await AppFunctions.get_app_request(db, quart.session, quart.request)
    if ar.character_id > 0 and ar.suspect:
        quart.session.clear()

    elif ar.character_id > 0 and ar.permitted:

        active_timer_results: typing.Final[list[AppTables.Structure]] = list()
        completed_extraction_results: typing.Final = list()
        scheduled_extraction_results: typing.Final = list()
        unscheduled_extraction_results: typing.Final = list()
        structure_fuel_results: typing.Final[list[AppTables.Structure]] = list()
        structures_without_fuel_results: typing.Final[list[AppTables.Structure]] = list()
        last_update_results: typing.Final = list()
        structure_counts: typing.Final = list()

        try:
            async with await db.sessionmaker() as session:
                active_timer_results.extend(await AppFunctions.get_active_timers(session, ar.ts))
                completed_extraction_results.extend(await AppFunctions.get_completed_extractions(session, ar.ts))
                scheduled_extraction_results.extend(await AppFunctions.get_scheduled_extractions(session, ar.ts))
                unscheduled_extraction_results.extend(await AppFunctions.get_unscheduled_structures(session, ar.ts))
                structure_fuel_results.extend(await AppFunctions.get_structure_fuel_expiries(session, ar.ts))
                structures_without_fuel_results.extend(await AppFunctions.get_structures_without_fuel(session, ar.ts))
                structure_count_dict: typing.Final = await AppFunctions.get_structure_counts(session, ar.ts)

                last_refresh_times: typing.Final = await AppFunctions.get_refresh_times(session, ar.ts)
                for obj in last_refresh_times:
                    if obj.corporation_id in structure_count_dict.keys():
                        last_update_results.append(obj)
                        structure_counts.append(structure_count_dict[obj.corporation_id])

        except Exception as ex:
            app.logger.error(f"{inspect.currentframe().f_code.co_name}: {ex=}")

        return await quart.render_template(
            "home.html",
            character_id=ar.character_id,
            is_contributor_character=ar.contributor,
            is_magic_character=ar.magic_character,
            active_timers=active_timer_results,
            completed_extractions=completed_extraction_results,
            scheduled_extractions=scheduled_extraction_results,
            structure_fuel_expiries=structure_fuel_results,
            structures_without_fuel=structures_without_fuel_results,
            unscheduled_extractions=unscheduled_extraction_results,
            last_update=last_update_results,
            structure_counts=structure_counts,
            is_homne_page=True
        )

    elif ar.character_id > 0:
        await eventqueue.put(AppAccessEvent(character_id=ar.character_id, url=quart.request.url, permitted=False))
        app.logger.warning(f"{ar.character_id} not permitted")
        return await quart.render_template(
            "permission.html",
            character_id=ar.character_id,
            is_contributor_character=ar.contributor,
            is_magic_character=ar.magic_character,
            is_homne_page=True
        )

    return await quart.render_template("login.html")


if __name__ == "__main__":

    otel_initialize()

    AppTemplates.add_templates(app, db)

    app_debug: typing.Final = app.config.get("DEBUG", False)
    app_port = app.config.get("PORT", 5050)
    app_host: typing.Final = app.config.get("HOST", "127.0.0.1")

    # app_log_file: typing.Final = os.path.join(BASEDIR, "logs", "app.log")
    # app_log_dir: typing.Final = os.path.dirname(app_log_file)
    # if not os.path.isdir(app_log_dir):
    #     os.makedirs(app_log_dir, 0o755)

    # logging.basicConfig(level=logging.INFO, filename=app_log_file)

    if app_debug:
        app.run(host=app_host, port=app_port, debug=app_debug)
    else:

        app_trusted_hosts: typing.Final = ["127.0.0.1", "::1"]
        app_bind_hosts: typing.Final = [x for x in app_trusted_hosts]

        script_name: typing.Final = os.path.splitext(os.path.basename(__file__))[0]
        with open(os.path.join(BASEDIR, f"{script_name}.pid"), 'w') as ofp:
            ofp.write(f"{os.getpid()}{os.linesep}")

        # XXX: hack for development server.
        development_flag_file = os.path.join(BASEDIR, "development.txt")
        if os.path.exists(development_flag_file):
            app_port = 5055
            with open(development_flag_file) as ifp:
                app_bind_hosts.clear()
                app_bind_hosts.append("0.0.0.0")
                for line in [line.strip() for line in ifp.readlines()]:
                    app_trusted_hosts.append(line)

        config: typing.Final = hypercorn.config.Config()
        config.bind = [f"{host}:{app_port}" for host in app_bind_hosts]
        config.accesslog = "-"

        async def async_main():
            await db._initialize()

            app.asgi_app = opentelemetry.instrumentation.asgi.OpenTelemetryMiddleware(
                app.asgi_app
            )

            app.asgi_app = hypercorn.middleware.ProxyFixMiddleware(
                app.asgi_app
            )

            await hypercorn.asyncio.serve(app, config, shutdown_trigger=shutdown_event.wait)

        asyncio.run(async_main())
