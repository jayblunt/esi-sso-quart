import asyncio
import datetime
import functools
import inspect
import logging
import os
import typing
import uuid

import opentelemetry.instrumentation.asgi
import quart
import quart.sessions
import quart_session

from app_functions import AppFunctions
from db import EveDatabase, EveTables
from middleware import RateLimiterMiddleware
from sso import EveSSO
from tasks import (EveAccessControlTask, EveAllianceTask, EveMoonYieldTask,
                   EveStructurePollingTask, EveStructureTask, EveTask,
                   EveUniverseConstellationsTask, EveUniverseRegionsTask,
                   EveUniverseSystemsTask)
from telemetry import otel, otel_initialize

app: typing.Final = quart.Quart(__name__)

app.logger.setLevel(logging.INFO)

app.config.from_mapping(
    {
        "DEBUG": False,
        "PORT": 5050,
        "HOST": "127.0.0.1",
        "SECRET_KEY": uuid.uuid4().hex,
        "SESSION_TYPE": "redis",
        "SESSION_REVERSE_PROXY": True,
        "BASEDIR": os.path.dirname(os.path.realpath(__file__)),
        "EVEONLINE_CLIENT_ID": os.getenv("EVEONLINE_CLIENT_ID", ""),
        "EVEONLINE_CLIENT_SECRET": os.getenv("EVEONLINE_CLIENT_SECRET", ""),
        "SQLALCHEMY_DB_URL": os.getenv("SQLALCHEMY_DB_URL", ""),
        "SEND_FILE_MAX_AGE_DEFAULT": 300,
        "MAX_CONTENT_LENGTH": 512 * 1024,
        "BODY_TIMEOUT": 15,
        "RESPONSE_TIMEOUT": 15,
    }
)

evesso_config: typing.Final = {
    "client_id": app.config.get("EVEONLINE_CLIENT_ID"),
    "client_secret": app.config.get("EVEONLINE_CLIENT_SECRET"),
    "scopes": ["publicData", "esi-characters.read_corporation_roles.v1",
               "esi-corporations.read_structures.v1", "esi-industry.read_corporation_mining.v1"]
}

evedb: typing.Final = EveDatabase(
    app.config.get("SQLALCHEMY_DB_URL", "sqlite+pysqlite://"),
)
quart_session.Session(app)
evesso: typing.Final = EveSSO(app, evedb, **evesso_config)
evesession: typing.Final = app.session_interface.session_class(sid="global", permanent=False)
evesession[EveTask.CONFIGDIR] = os.path.abspath(os.path.join(app.config.get("BASEDIR", "."), "data"))
evesession[EveSSO.ESI_CHARACTER_NAME] = dict()


@app.before_serving
@otel
async def _before_serving():
    if not bool(evesession.get("setup_tasks_started", False)):
        evesession["setup_tasks_started"] = True

        EveAccessControlTask(evesession, evedb, app.logger)
        EveMoonYieldTask(evesession, evedb, app.logger)

        EveUniverseRegionsTask(evesession, evedb, app.logger)
        EveUniverseConstellationsTask(evesession, evedb, app.logger)
        EveUniverseSystemsTask(evesession, evedb, app.logger)
        EveAllianceTask(evesession, evedb, app.logger)

        EveStructurePollingTask(evesession, evedb, app.logger)


@app.errorhandler(404)
@otel
async def error_404(path: str) -> quart.Response:
    return quart.redirect("/")


@app.template_filter("login_type")
@otel
def _login_type(input: str):
    client_session: typing.Final = quart.session
    login_type = client_session.get(EveSSO.APP_SESSION_TYPE, "USER")
    if login_type == "CONTRIBUTOR":
        return "contributor"
    else:
        return "user"


@app.template_filter("character_name")
@otel
async def _character_name(input: str):
    character_id = int(input)
    character_name = evesession[EveSSO.ESI_CHARACTER_NAME].get(character_id)
    if not character_name:
        character_name = await AppFunctions.get_character_name(evedb, character_id)
        if character_name:
            evesession[EveSSO.ESI_CHARACTER_NAME][character_id] = character_name
    return character_name


@app.template_filter("zkillboard")
@otel
async def _zkillboard(input: str):
    character_id = int(input)
    return f"https://zkillboard.com/character/{character_id}/"


@app.template_filter("structure_state")
@otel
def _structure_state(state: str) -> str:
    map: typing.Final = {
        "deploy_vulnerable": "Deploy / Vulnerable",
        "anchoring": "Anchoring",
        "anchor_vulnerable": "Anchoring / Vulnerable",
        "onlining_vulnerable": "Onlining / Vulnerable",
        "shield_vulnerable": "Shield / Vulnerable",
        "hull_reinforce": "Hull Reinforced",
        "hull_vulnerable": "Hull / Vulnerable",
        "armor_reinforce": "Armor Reinforced",
        "armor_vulnerable": "Armor / Vulnerable",
    }
    return map.get(state, "Unknown")


@app.template_filter("timestamp_age")
@otel
def _timestamp_age(dt: datetime.datetime) -> str:
    age_days: typing.Final = (datetime.datetime.now(datetime.timezone.utc) - dt.replace(tzinfo=datetime.timezone.utc)).days
    if age_days >= 3:
        return "stale"
    return "fresh"


@app.template_filter("datetime")
@otel
def _datetime(dt: datetime.datetime) -> str:
    return dt.replace(tzinfo=None).isoformat(sep=" ", timespec="minutes")


@app.route("/usage/", methods=["GET"])
@otel
async def _usage() -> quart.Response:

    client_session: typing.Final = quart.session

    now: typing.Final = datetime.datetime.now(tz=datetime.timezone.utc)

    character_id: typing.Final = client_session.get(EveSSO.ESI_CHARACTER_ID, 0)
    if character_id > 0:

        # if character_id in [92923556]:
        permitted_character_ids = {92923556}
        permitted_character_ids = permitted_character_ids.union({2115300524})
        permitted_character_ids = permitted_character_ids.union({2113162721})
        permitted_character_ids = permitted_character_ids.union({93692517, 96477045, 96602200, 96732252})
        if character_id in permitted_character_ids:
            permitted_data = list()
            denied_data = list()

            try:
                async with await evedb.sessionmaker() as session:
                    permitted_data = await AppFunctions.get_usage(session, True, now)
                    denied_data = await AppFunctions.get_usage(session, False, now)

            except Exception as ex:
                app.logger.error(f"{inspect.currentframe().f_code.co_name}: {ex}")

            return await quart.render_template(
                "usage.html",
                character_name=client_session.get(EveSSO.ESI_CHARACTER_NAME),
                character_id=client_session.get(EveSSO.ESI_CHARACTER_ID),
                permitted_usage=permitted_data, denied_usage=denied_data)

    return quart.redirect("/")


@app.route("/about/", methods=["GET"])
@otel
async def _about() -> quart.Response:
    client_session: typing.Final = quart.session
    character_id: typing.Final = client_session.get(EveSSO.ESI_CHARACTER_ID, 0)
    if character_id > 0:

        corpporation_id: typing.Final = client_session.get(EveSSO.ESI_CORPORATION_ID, 0)
        alliance_id: typing.Final = client_session.get(EveSSO.ESI_ALLIANCE_ID, 0)
        character_permitted: typing.Final = await AppFunctions.is_permitted(evedb, character_id, corpporation_id, alliance_id)

        try:
            async with await evedb.sessionmaker() as session, session.begin():

                session.add(EveTables.AccessHistory(character_id=character_id, permitted=bool(character_permitted), path=quart.request.path))
                await session.commit()

        except Exception as ex:
            app.logger.error(f"{inspect.currentframe().f_code.co_name}: {ex}")

    return await quart.render_template("about.html")


@app.route("/", methods=["GET"])
@otel
async def root() -> quart.Response:

    client_session: typing.Final = quart.session

    now: typing.Final = datetime.datetime.now(tz=datetime.timezone.utc)

    character_id: typing.Final = client_session.get(EveSSO.ESI_CHARACTER_ID, 0)
    corpporation_id: typing.Final = client_session.get(EveSSO.ESI_CORPORATION_ID, 0)
    alliance_id: typing.Final = client_session.get(EveSSO.ESI_ALLIANCE_ID, 0)

    character_permitted: typing.Final = await AppFunctions.is_permitted(evedb, character_id, corpporation_id, alliance_id)

    if character_id > 0:

        try:
            async with await evedb.sessionmaker() as session, session.begin():

                session.add(EveTables.AccessHistory(character_id=character_id, permitted=bool(character_permitted), path=quart.request.path))
                await session.commit()

        except Exception as ex:
            app.logger.error(f"{inspect.currentframe().f_code.co_name}: {ex}")

    if character_id > 0 and character_permitted:

        if bool(client_session.get(EveSSO.ESI_CHARACTER_HAS_STATION_MANAGER_ROLE, False)):
            EveStructureTask(client_session, evedb, app.logger)

        active_timer_results: typing.Final = list()
        completed_extraction_results: typing.Final = list()
        scheduled_extraction_results: typing.Final = list()
        structure_fuel_results: typing.Final = list()
        last_update_results: typing.Final = list()

        try:
            async with await evedb.sessionmaker() as session:

                active_timer_results += await AppFunctions.get_active_timers(session, now)
                completed_extraction_results += await AppFunctions.get_completed_extractions(session, now)
                scheduled_extraction_results += await AppFunctions.get_scheduled_extractions(session, now)
                structure_fuel_results += await AppFunctions.get_structure_fuel_expiries(session, now)

                last_update_dict: typing.Final = dict()
                for obj in structure_fuel_results:
                    if isinstance(obj, EveTables.Structure):
                        if obj.corporation_id not in last_update_dict.keys():
                            last_update_dict[obj.corporation_id] = obj
                        elif obj.timestamp > last_update_dict[obj.corporation_id].timestamp:
                            last_update_dict[obj.corporation_id] = obj

                last_update_results += sorted(last_update_dict.values(), reverse=False, key=lambda x: x.timestamp)

        except Exception as ex:
            app.logger.error(f"{inspect.currentframe().f_code.co_name}: {ex}")

        return await quart.render_template(
            "home.html",
            character_name=client_session.get(EveSSO.ESI_CHARACTER_NAME),
            character_id=client_session.get(EveSSO.ESI_CHARACTER_ID),
            active_timers=active_timer_results,
            completed_extractions=completed_extraction_results,
            scheduled_extractions=scheduled_extraction_results,
            structure_fuel_expiries=structure_fuel_results,
            last_update=last_update_results,
        )

    elif character_id > 0 and not character_permitted:
        app.logger.warning(f"{character_id} not permitted")
        return await quart.render_template(
            "permission.html",
            character_id=character_id,
            character_name=client_session.get(EveSSO.ESI_CHARACTER_NAME),
        )

    else:
        return await quart.render_template("login.html")


if __name__ == "__main__":

    # logging.basicConfig(level=logging.DEBUG)
    otel_initialize()

    app_debug = app.config.get("DEBUG", False)
    app_port = app.config.get("PORT", 5050)
    app_host = app.config.get("HOST", "127.0.0.1")

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

        config = hypercorn.config.Config()
        config.bind = [f"{app_host}:{app_port}"]
        config.accesslog = "-"

        async def async_main():
            await evedb._initialize()

            app.asgi_app = opentelemetry.instrumentation.asgi.OpenTelemetryMiddleware(
                app.asgi_app
            )

            app.asgi_app = RateLimiterMiddleware(
                app.asgi_app,
                threshold=32
            )

            app.asgi_app = ProxyHeadersMiddleware(
                app.asgi_app, trusted_hosts=["127.0.0.1"]
            )

            await hypercorn.asyncio.serve(app, config)

        asyncio.run(async_main())
