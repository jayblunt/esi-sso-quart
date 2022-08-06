import asyncio
import os
import syslog
import uuid
from typing import Final

import quart
import quart.sessions
import quart_session

from db import EveDatabase
from sso import EveSSO
from tasks import (EveAlliancMemberTask, EveEnumerateExtractionTask,
                   EveEnumerateStructureTask, EveInventoryTask,
                   EveStructureSearchTask)

syslog.openlog(
    os.path.basename(__file__), logoption=syslog.LOG_PID, facility=syslog.LOG_AUTH
)


local_basedir: Final = os.path.dirname(os.path.realpath(__file__))


app: Final = quart.Quart(__name__)

app.config.from_mapping({
    "DEBUG": False,
    "PORT": 5500,
    "HOST": '0.0.0.0',
    "SECRET_KEY":  uuid.uuid4().hex,
    "SESSION_TYPE": "redis",
    "EVEONLINE_CLIENT_ID": os.getenv("EVEONLINE_CLIENT_ID", ""),
    "EVEONLINE_CLIENT_SECRET": os.getenv("EVEONLINE_CLIENT_SECRET", ""),
    "EVEONLINE_CLIENT_SCOPES": os.getenv("EVEONLINE_CLIENT_SCOPES", ""),
    "SQLALCHEMY_DB_URL": os.getenv("SQLALCHEMY_DB_URL", ""),
})

evesso_config = {
    "client_id": app.config.get("EVEONLINE_CLIENT_ID"),
    "client_secret": app.config.get("EVEONLINE_CLIENT_SECRET"),
    "scopes": app.config.get("EVEONLINE_CLIENT_SCOPES", "publicData").split(),
}

quart_session.Session(app)
evesso = EveSSO(app, **evesso_config)
evedb = EveDatabase(app.config.get("SQLALCHEMY_DB_URL", "sqlite+pysqlite:///:memory:"))


@app.errorhandler(404)
async def error_404(path: str) -> quart.Response:
    return quart.redirect("/")


@app.route("/", methods=["GET"])
async def root() -> quart.Response:
    print(dict(quart.session))
    if quart.session.get(EveSSO.ESI_CHARACTER_NAME):

        # if bool(quart.session.get(EveSSO.ESI_CHARACTER_STATION_MANAGER_ROLE, False)):
        #     EveEnumerateStructureTask(quart.session, evedb)
        #     EveEnumerateExtractionTask(quart.session, evedb)

        # if bool(quart.session.get(EveSSO.ESI_CHARACTER_DIRECTOR_ROLE, False)):
        #     EveAlliancMemberTask(quart.session, evedb)

        return await quart.render_template("home.html",
                                           character_name=quart.session.get(
                                               EveSSO.ESI_CHARACTER_NAME),
                                           character_id=quart.session.get(EveSSO.ESI_CHARACTER_ID))
    else:
        return await quart.render_template("login.html")


if __name__ == "__main__":
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
                app.asgi_app, trusted_hosts=["127.0.0.1", "::1"])

            await hypercorn.asyncio.serve(app, config)

        asyncio.run(async_main())
