import asyncio
import contextlib
import os
import syslog
import uuid
from typing import Final, MutableSet, Optional

import aiohttp
import aiohttp.client_exceptions
import quart
import quart.sessions
import quart_session

from sso import EveSSO
from tasks import EveInventoryTask, EveEnumerateStructureTask, EveAllianceInfoTask, EveStructureSearchTask

syslog.openlog(
    os.path.basename(__file__), logoption=syslog.LOG_PID, facility=syslog.LOG_AUTH
)


local_basedir: Final = os.path.dirname(os.path.realpath(__file__))


app: Final = quart.Quart(__name__)
app.config.from_mapping({
    "DEBUG": True,
    "PORT": 5025,
    "HOST": '0.0.0.0',
    "SECRET_KEY":  uuid.uuid4().hex,
    "SESSION_TYPE": "redis",
    "EVEONLINE_CLIENT_ID": os.getenv("EVEONLINE_CLIENT_ID", ""),
    "EVEONLINE_CLIENT_SECRET": os.getenv("EVEONLINE_CLIENT_SECRET", "")
})
quart_session.Session(app)


evesso_scopes: Final = ['publicData', 'esi-location.read_location.v1', 'esi-location.read_ship_type.v1', 'esi-mail.read_mail.v1', 'esi-skills.read_skills.v1', 'esi-skills.read_skillqueue.v1', 'esi-wallet.read_character_wallet.v1', 'esi-wallet.read_corporation_wallet.v1', 'esi-search.search_structures.v1', 'esi-clones.read_clones.v1', 'esi-characters.read_contacts.v1', 'esi-universe.read_structures.v1', 'esi-bookmarks.read_character_bookmarks.v1', 'esi-killmails.read_killmails.v1', 'esi-corporations.read_corporation_membership.v1', 'esi-assets.read_assets.v1', 'esi-planets.manage_planets.v1', 'esi-fleets.read_fleet.v1', 'esi-fleets.write_fleet.v1', 'esi-ui.write_waypoint.v1', 'esi-characters.write_contacts.v1', 'esi-fittings.read_fittings.v1', 'esi-fittings.write_fittings.v1', 'esi-markets.structure_markets.v1', 'esi-corporations.read_structures.v1', 'esi-characters.read_loyalty.v1', 'esi-characters.read_opportunities.v1', 'esi-characters.read_chat_channels.v1', 'esi-characters.read_medals.v1', 'esi-characters.read_standings.v1', 'esi-characters.read_agents_research.v1', 'esi-industry.read_character_jobs.v1', 'esi-markets.read_character_orders.v1', 'esi-characters.read_blueprints.v1',
                        'esi-characters.read_corporation_roles.v1', 'esi-location.read_online.v1', 'esi-contracts.read_character_contracts.v1', 'esi-clones.read_implants.v1', 'esi-characters.read_fatigue.v1', 'esi-killmails.read_corporation_killmails.v1', 'esi-corporations.track_members.v1', 'esi-wallet.read_corporation_wallets.v1', 'esi-characters.read_notifications.v1', 'esi-corporations.read_divisions.v1', 'esi-corporations.read_contacts.v1', 'esi-assets.read_corporation_assets.v1', 'esi-corporations.read_titles.v1', 'esi-corporations.read_blueprints.v1', 'esi-bookmarks.read_corporation_bookmarks.v1', 'esi-contracts.read_corporation_contracts.v1', 'esi-corporations.read_standings.v1', 'esi-corporations.read_starbases.v1', 'esi-industry.read_corporation_jobs.v1', 'esi-markets.read_corporation_orders.v1', 'esi-corporations.read_container_logs.v1', 'esi-industry.read_character_mining.v1', 'esi-industry.read_corporation_mining.v1', 'esi-planets.read_customs_offices.v1', 'esi-corporations.read_facilities.v1', 'esi-corporations.read_medals.v1', 'esi-characters.read_titles.v1', 'esi-characters.read_fw_stats.v1', 'esi-corporations.read_fw_stats.v1', 'esi-characterstats.read.v1']
evesso_config = {
    "client_id": app.config.get("EVEONLINE_CLIENT_ID"),
    "client_secret": app.config.get("EVEONLINE_CLIENT_SECRET"),
    "scopes": evesso_scopes,
}
evesso = EveSSO(app, **evesso_config)


@app.route("/", methods=["GET"])
async def root() -> quart.Response:
    print(dict(quart.session))
    if quart.session.get(EveSSO.ESI_CHARACTER_NAME):

        # EveInventoryTask(quart.session)
        # EveEnumerateStructureTask(quart.session)
        # EveAllianceInfoTask(quart.session)
        # EveStructureSearchTask(quart.session)

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

        app.asgi_app = ProxyHeadersMiddleware(
            app.asgi_app, trusted_hosts=["127.0.0.1", "::1"])
        asyncio.run(hypercorn.asyncio.serve(app, config))
