import asyncio
import datetime
import logging
import os
import syslog
import uuid
from typing import Final

import quart
import quart.sessions
import quart_session
import sqlalchemy
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql

from db import EveDatabase, EveTables
from sso import EveSSO
from tasks import (
    EveStructureTask,
    EsiAllianceTask,
    EveEsiAlliancMemberTask,
    EveUniverseRegionsTask,
    EveUniverseConstellationsTask,
    EveUniverseSystemsTask,
    EveMoonYieldTask,
    EveTask,
)

syslog.openlog(
    os.path.basename(__file__), logoption=syslog.LOG_PID, facility=syslog.LOG_AUTH
)


app: Final = quart.Quart(__name__)

app.config.from_mapping(
    {
        "DEBUG": False,
        "PORT": 5500,
        "HOST": "0.0.0.0",
        "SECRET_KEY": uuid.uuid4().hex,
        "SESSION_TYPE": "redis",
        "SESSION_PROTECTION": True,
        "SESSION_REVERSE_PROXY": True,
        "BASEDIR": os.path.dirname(os.path.realpath(__file__)),
        "EVEONLINE_CLIENT_ID": os.getenv("EVEONLINE_CLIENT_ID", ""),
        "EVEONLINE_CLIENT_SECRET": os.getenv("EVEONLINE_CLIENT_SECRET", ""),
        "EVEONLINE_CLIENT_SCOPES": os.getenv("EVEONLINE_CLIENT_SCOPES", ""),
        "SQLALCHEMY_DB_URL": os.getenv("SQLALCHEMY_DB_URL", ""),
    }
)

evesso_config: Final = {
    "client_id": app.config.get("EVEONLINE_CLIENT_ID"),
    "client_secret": app.config.get("EVEONLINE_CLIENT_SECRET"),
    "scopes": app.config.get("EVEONLINE_CLIENT_SCOPES", "publicData").split(),
}

evedb: Final = EveDatabase(
    app.config.get("SQLALCHEMY_DB_URL", "sqlite+pysqlite:///:memory:")
)
quart_session.Session(app)
evesso: Final = EveSSO(app, evedb, **evesso_config)
evesession: Final = app.session_interface.session_class(sid="global", permanent=False)
evesession[EveTask.CONFIGDIR] = os.path.abspath(os.path.join(app.config.get("BASEDIR", "."), "static"))


@app.errorhandler(404)
async def error_404(path: str) -> quart.Response:
    return quart.redirect("/")


@app.template_filter("datetime")
def _datetime(dt: datetime.datetime):
    return dt.replace(tzinfo=None).isoformat(sep=" ", timespec="minutes")


@app.route("/", methods=["GET"])
async def root() -> quart.Response:

    # print(f"evesession: {dict(evesession)}")
    if not bool(evesession.get("tasks_started", False)):
        evesession["tasks_started"] = True
        # EveMoonYieldTask(evesession, evedb, app.logger)
        # EsiAllianceTask(evesession, evedb, app.logger)
        # EveUniverseRegionsTask(evesession, evedb, app.logger)
        # EveUniverseConstellationsTask(evesession, evedb, app.logger)
        # EveUniverseSystemsTask(evesession, evedb, app.logger)

    # print(f"session: {dict(quart.session)}")
    if quart.session.get(EveSSO.ESI_CHARACTER_NAME):

        if not bool(quart.session.get("tasks_started", False)):
            quart.session["tasks_started"] = True

            if bool(quart.session.get(EveSSO.ESI_CHARACTER_STATION_MANAGER_ROLE, False)):
                # EveEnumerateStructureTask(quart.session, evedb, app.logger)
                EveStructureTask(quart.session, evedb, app.logger)

            # if bool(quart.session.get(EveSSO.ESI_CHARACTER_DIRECTOR_ROLE, False)):
            #     EveEsiAlliancMemberTask(quart.session, evedb, app.logger)

        extraction_results: Final = list()
        structure_results: Final = list()
        async with await evedb.sessionmaker() as session:

            utcnow = datetime.datetime.utcnow()

            extraction_query: Final = (
                sqlalchemy.select(EveTables.Extraction)
                .where(
                    EveTables.Extraction.natural_decay_time >= utcnow,
                    EveTables.Extraction.extraction_start_time < utcnow,
                )
                .order_by(EveTables.Extraction.chunk_arrival_time)
                .options(sqlalchemy.orm.selectinload(EveTables.Extraction.structure))
                .options(sqlalchemy.orm.selectinload(EveTables.Extraction.corporation))
                .options(sqlalchemy.orm.selectinload(EveTables.Extraction.moon))
            )

            extraction_query_result = await session.execute(extraction_query)
            extraction_results += [
                result for result in extraction_query_result.scalars()
            ]
            print(extraction_results)

            structure_query: Final = (
                sqlalchemy.select(
                    (
                        EveTables.Structure,
                        EveTables.UniverseSystem.name.label("system_name"),
                        EveTables.Corporation.name.label("corporation_name"),
                    )
                )
                .join(EveTables.Structure.system)
                .join(EveTables.Structure.corporation)
                .order_by(EveTables.Structure.fuel_expires)
                .options(sqlalchemy.orm.selectinload(EveTables.Structure.system))
                .options(sqlalchemy.orm.selectinload(EveTables.Structure.corporation))
            )
            structure_query_result = await session.execute(structure_query)
            structure_results += [result for result in structure_query_result.scalars()]
            logging.getLogger().debug(structure_results)

        return await quart.render_template(
            "home.html",
            character_name=quart.session.get(EveSSO.ESI_CHARACTER_NAME),
            character_id=quart.session.get(EveSSO.ESI_CHARACTER_ID),
            extractions=extraction_results,
            structures=structure_results,
        )
    else:
        return await quart.render_template("login.html")


if __name__ == "__main__":
    # logging.basicConfig(level=logging.DEBUG)

    app_debug = app.config.get("DEBUG", False)
    app_port = app.config.get("PORT", 5025)
    app_host = app.config.get("HOST", "127.0.0.1")

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

            app.asgi_app = ProxyHeadersMiddleware(
                app.asgi_app, trusted_hosts=["127.0.0.1", "::1"]
            )

            await hypercorn.asyncio.serve(app, config)

        asyncio.run(async_main())
