import asyncio
import os
import syslog
import contextlib
import uuid
from typing import Final, List, MutableSet, Optional

import aiohttp
import aiohttp.client_exceptions
import quart
import quart_session

from sso import EveSSO

syslog.openlog(
    os.path.basename(__file__), logoption=syslog.LOG_PID, facility=syslog.LOG_AUTH
)


local_basedir: Final = os.path.dirname(os.path.realpath(__file__))


# Search for structures in a list of systems
async def esi_structure_search(access_token: str, character_id: str, corporation_id: str, alliance_id: Optional[str] = None) -> None:
    session_headers = {
        "Authorization": f"Bearer {access_token}"
    }
    common_params = {
        "datasource": "tranquility"
    }

    structure_set: Final[MutableSet[str]] = set()
    system_list: Final = ["RF-GGF", "BMNV-P", "31-MLU", "LSC$-P", "9GYL-O", "A9D-R0"]

    if len(system_list):
        system_list.sort()
        async with aiohttp.ClientSession(headers=session_headers) as client_session:

            for system_name in system_list:
                url = f"https://esi.evetech.net/latest/characters/{character_id}/search/"
                url_params = {**common_params, **{
                    "categories": "structure",
                    "language": "en",
                    "strict": "false",
                    "search": system_name
                }}
                with contextlib.suppress(aiohttp.client_exceptions.ClientResponseError):
                    async with client_session.get(url, params=url_params) as response:
                        print(f"{response.url} -> {response.status}")
                        if response.status in [200]:
                            data = dict(await response.json())
                            for structure_id in data.get('structure', []):
                                structure_set.add(str(structure_id))

    if len(structure_set):
        async with aiohttp.ClientSession(headers=session_headers) as client_session:

            print(f"structure_list: {structure_set}")
            for structure_id in structure_set:
                url = f"https://esi.evetech.net/latest/universe/structures/{structure_id}/"
                with contextlib.suppress(aiohttp.client_exceptions.ClientResponseError):
                    async with client_session.get(url, params=common_params) as response:
                        print(f"{response.url} -> {response.status}")
                        if response.status in [200]:
                            data = await response.json()
                            print(f"{structure_id}: {quart.json.dumps(data, ensure_ascii=True, indent=4)}")


    quart.session["STRUCTURE_SEARCH_TASK"] = False


# Get corporation (and/or alliance) info
async def esi_collect_corporation_info(access_token: str, character_id: str, corporation_id: str, alliance_id: Optional[str] = None) -> None:
    session_headers = {
        "Authorization": f"Bearer {access_token}"
    }
    common_params = {
        "datasource": "tranquility"
    }

    corporation_set: Final[MutableSet[str]] = set()
    async with aiohttp.ClientSession(headers=session_headers) as client_session:

        if alliance_id is None:
            corporation_set.append(int(corporation_id))           
        else:

            url = f"https://esi.evetech.net/latest/alliances/{alliance_id}/corporations/"
            with contextlib.suppress(aiohttp.client_exceptions.ClientResponseError):
                async with client_session.get(url, params=common_params) as response:
                    print(f"{response.url} -> {response.status}")
                    if response.status in [200]:
                        for corporation_id in list(await response.json()):
                            corporation_set.add(int(corporation_id))

    if len(corporation_set):
        print(f"corporation_list: {corporation_set}")
        async with aiohttp.ClientSession(headers=session_headers) as client_session:

            for corporation_id in corporation_set:

                url = f"https://esi.evetech.net/latest/corporations/{corporation_id}"
                with contextlib.suppress(aiohttp.client_exceptions.ClientResponseError):
                    async with client_session.get(url, params=common_params) as response:
                        print(f"{response.url} -> {response.status}")
                        if response.status in [200]:
                            results = dict(await response.json())
                            print(f"{corporation_id}: {quart.json.dumps(results, ensure_ascii=True, indent=4)}")

                url = f"https://esi.evetech.net/latest/corporations/{corporation_id}/structures"
                with contextlib.suppress(aiohttp.client_exceptions.ClientResponseError):
                    async with client_session.get(url, params=common_params) as response:
                        print(f"{response.url} -> {response.status}")
                        if response.status in [200]:
                            results = await response.json()
                            print(f"{corporation_id}: {quart.json.dumps(results, ensure_ascii=True, indent=4)}")

    quart.session["CORPORATION_INFO_TASK"] = False


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


evesso_scopes: Final = ['publicData', 'esi-location.read_location.v1', 'esi-location.read_ship_type.v1', 'esi-mail.read_mail.v1', 'esi-skills.read_skills.v1', 'esi-skills.read_skillqueue.v1', 'esi-wallet.read_character_wallet.v1', 'esi-wallet.read_corporation_wallet.v1', 'esi-search.search_structures.v1', 'esi-clones.read_clones.v1', 'esi-characters.read_contacts.v1', 'esi-universe.read_structures.v1', 'esi-bookmarks.read_character_bookmarks.v1', 'esi-killmails.read_killmails.v1', 'esi-corporations.read_corporation_membership.v1', 'esi-assets.read_assets.v1', 'esi-planets.manage_planets.v1', 'esi-fleets.read_fleet.v1', 'esi-fleets.write_fleet.v1', 'esi-ui.write_waypoint.v1', 'esi-characters.write_contacts.v1', 'esi-fittings.read_fittings.v1', 'esi-fittings.write_fittings.v1', 'esi-markets.structure_markets.v1', 'esi-corporations.read_structures.v1', 'esi-characters.read_loyalty.v1', 'esi-characters.read_opportunities.v1', 'esi-characters.read_chat_channels.v1', 'esi-characters.read_medals.v1', 'esi-characters.read_standings.v1', 'esi-characters.read_agents_research.v1', 'esi-industry.read_character_jobs.v1', 'esi-markets.read_character_orders.v1', 'esi-characters.read_blueprints.v1', 'esi-characters.read_corporation_roles.v1', 'esi-location.read_online.v1', 'esi-contracts.read_character_contracts.v1', 'esi-clones.read_implants.v1', 'esi-characters.read_fatigue.v1', 'esi-killmails.read_corporation_killmails.v1', 'esi-corporations.track_members.v1', 'esi-wallet.read_corporation_wallets.v1', 'esi-characters.read_notifications.v1', 'esi-corporations.read_divisions.v1', 'esi-corporations.read_contacts.v1', 'esi-assets.read_corporation_assets.v1', 'esi-corporations.read_titles.v1', 'esi-corporations.read_blueprints.v1', 'esi-bookmarks.read_corporation_bookmarks.v1', 'esi-contracts.read_corporation_contracts.v1', 'esi-corporations.read_standings.v1', 'esi-corporations.read_starbases.v1', 'esi-industry.read_corporation_jobs.v1', 'esi-markets.read_corporation_orders.v1', 'esi-corporations.read_container_logs.v1', 'esi-industry.read_character_mining.v1', 'esi-industry.read_corporation_mining.v1', 'esi-planets.read_customs_offices.v1', 'esi-corporations.read_facilities.v1', 'esi-corporations.read_medals.v1', 'esi-characters.read_titles.v1', 'esi-characters.read_fw_stats.v1', 'esi-corporations.read_fw_stats.v1', 'esi-characterstats.read.v1']
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

        # if not quart.session.get("CORPORATION_INFO_TASK", False):
        #     quart.session["CORPORATION_INFO_TASK"] = True
        #     asyncio.create_task(
        #         esi_collect_corporation_info(quart.session.get(EveSSO.ESI_ACCESS_TOKEN),
        #             quart.session.get(EveSSO.ESI_CHARACTER_ID),
        #             quart.session.get(EveSSO.ESI_CORPORATEION_ID),
        #             quart.session.get(EveSSO.ESI_ALLIANCE_ID)))
                    
        # if not quart.session.get("STRUCTURE_SEARCH_TASK", False):
        #     quart.session["STRUCTURE_SEARCH_TASK"] = True
        #     asyncio.create_task(
        #         esi_structure_search(quart.session.get(EveSSO.ESI_ACCESS_TOKEN),
        #             quart.session.get(EveSSO.ESI_CHARACTER_ID),
        #             quart.session.get(EveSSO.ESI_CORPORATEION_ID),
        #             quart.session.get(EveSSO.ESI_ALLIANCE_ID)))
                    
        return await quart.render_template("home.html",
            character_name=quart.session.get(EveSSO.ESI_CHARACTER_NAME),
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

        app.asgi_app = ProxyHeadersMiddleware(app.asgi_app, trusted_hosts=["127.0.0.1", "::1"])
        asyncio.run(hypercorn.asyncio.serve(app, config))
