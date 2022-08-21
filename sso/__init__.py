import asyncio
import base64
import collections.abc
import datetime
import inspect
import logging
import urllib.parse
import uuid
import zoneinfo
from typing import Final, List

import aiohttp
import aiohttp.client_exceptions
import dateutil.parser
import jose.exceptions
import jose.jwt
import quart
import sqlalchemy
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql
from db import EveAccessType, EveDatabase, EveTables


class EveSSO:

    CONNFIGURATION_URL: Final = 'https://login.eveonline.com/.well-known/oauth-authorization-server'

    JWT_ISSUERS: Final = ["login.eveonline.com", "https://login.eveonline.com"]
    JWT_AUDIENCE: Final = "EVE Online"

    APP_PERMITTED: Final = "app_permitted"
    APP_SESSION_STATE: Final = "app_session_state"

    ESI_CHARACTER_NAME: Final = "character_name"
    ESI_CHARACTER_ID: Final = "character_id"
    ESI_CORPORATION_ID: Final = "corporation_id"
    ESI_ALLIANCE_ID: Final = "alliance_id"

    ESI_CHARACTER_HAS_ACCOUNTANT_ROLE: Final = "has_accountant_role"
    ESI_CHARACTER_HAS_DIRECTOR_ROLE: Final = "has_director_role"
    ESI_CHARACTER_HAS_STATION_MANAGER_ROLE: Final = "has_station_manager_role"

    ESI_ACCESS_TOKEN: Final = "access_token"
    ESI_ACCESS_TOKEN_ISSUED: Final = "access_token_issued"
    ESI_ACCESS_TOKEN_EXPIRY: Final = "access_token_expiry"
    ESI_ACCESS_TOKEN_SCOPES: Final = "access_token_scopes"

    ESI_REFRESH_TOKEN: Final = "refresh_token"

    ESI_DEBUG_TOKEN: Final = "debug_token"
    ESI_DEBUG_ACCESS_TOKEN: Final = "debug_access_token"

    def __init__(self,
                 app: quart.Quart,
                 db: EveDatabase,
                 client_id: str,
                 client_secret: str,
                 configuration_url: str = None,
                 scopes: list[str] = ['publicData'],
                 login_route: str = '/sso/login',
                 logout_route: str = '/sso/logout',
                 callback_route: str = '/sso/callback',
                 logger: logging.Logger = logging.getLogger()) -> None:

        self.app: Final = app
        self.db: Final = db
        self.logger: Final = logger

        self.client_id: Final = client_id
        self.client_secret: Final = client_secret

        self.configuration_url: Final = configuration_url or self.CONNFIGURATION_URL
        self.scopes: Final = scopes

        self.login_route: Final = login_route
        self.logout_route: Final = logout_route
        self.callback_route: Final = callback_route

        self.configuration: dict = None
        self.jwks_uri: str = None
        self.jwks: list[dict] = None
        self.jwks_task: asyncio.Task = None

        self.refresh_task: asyncio.Task = None

        @app.before_serving
        async def esi_sso_setup():
            self.configuration = await self.get_json(self.configuration_url)

            required_configuration_keys = ["token_endpoint", "authorization_endpoint", "issuer", "jwks_uri"]
            if not all(map(lambda x: bool(self.configuration.get(x)), required_configuration_keys)):
                raise Exception(f"{inspect.currentframe().f_code.co_name}: configuration at {self.configuration_url} is invalid")

            self.jwks_uri = self.configuration["jwks_uri"]
            self.jwks = await self.get_jwks(self.jwks_uri)
            self.jwks_task = asyncio.create_task(self.get_jwks_task())

            self.refresh_task = asyncio.create_task(self.refresh_token_task())

            app.add_url_rule(self.callback_route, self.callback_endpoint, view_func=self.esi_sso_callback, methods=["GET"])
            app.add_url_rule(self.logout_route, self.logout_endpoint, view_func=self.esi_sso_logout, methods=["GET"])
            app.add_url_rule(self.login_route, self.login_endpoint, view_func=self.esi_sso_login, methods=["GET"])

        @app.after_serving
        async def esi_sso_teardown():
            for task in [self.jwks_task, self.refresh_task]:
                if task is None:
                    continue
                if not task.cancelled():
                    task.cancel()

    @property
    def base_url(self) -> str:
        headers = {k.lower(): v for k, v in dict(quart.request.headers).items()}
        parsed_url = urllib.parse.urlparse(quart.request.base_url)
        scheme = headers["x-forwarded-proto"] if "x-forwarded-proto" in headers else parsed_url[0]
        port = f":{int(headers['x-forwarded-port'])}" if "x-forwarded-port" in headers else ""
        return f"{scheme}://{quart.request.host}{port}"

    @property
    def callback_url(self) -> str:
        return f"{self.base_url}{self.callback_route}"

    @property
    def login_endpoint(self) -> str:
        return f"login_{self.client_id}"

    @property
    def logout_endpoint(self) -> str:
        return f"logout_{self.client_id}"

    @property
    def callback_endpoint(self) -> str:
        return f"callback_{self.client_id}"

    async def get_json(self, url: str, esi_access_token: str = '') -> dict:
        session_headers: Final = dict()
        if len(esi_access_token) > 0:
            session_headers["Authorization"] = f"Bearer {esi_access_token}"

        json = None
        async with aiohttp.ClientSession(headers=session_headers) as http_session:
            async with http_session.get(url) as response:
                if response.status in [200]:
                    json = dict(await response.json())
        return json

    async def get_jwks(self, url: str) -> list[dict]:
        payload: Final = await self.get_json(url)
        return payload.get("keys", [])

    async def get_jwks_task(self) -> None:
        while True:
            await asyncio.sleep(300)
            try:
                new_jwks: Final = await self.get_jwks(self.jwks_uri)
                if new_jwks is not None:
                    self.jwks = new_jwks
            except Exception as ex:
                self.app.logger.error(f"{inspect.currentframe().f_code.co_name}: {ex}")

    async def refresh_token_task(self) -> None:
        while True:
            refresh_window: Final = datetime.timedelta(seconds=60)

            refresh_obj_list: Final[List[EveTables.PeriodicCredentials]] = list()
            refresh_obj_list.clear()
            async with await self.db.sessionmaker() as db, db.begin():
                query = sqlalchemy.select(EveTables.PeriodicCredentials).order_by(sqlalchemy.asc(EveTables.PeriodicCredentials.access_token_exiry)).limit(1)
                results = await db.execute(query)
                rl = [x for x in results.scalars()]
                refresh_obj_list.extend(rl)

            if len(refresh_obj_list) <= 0:
                await asyncio.sleep(refresh_window.total_seconds())
                continue

            refresh_time: Final = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc) - refresh_window

            refresh_obj: Final[EveTables.PeriodicCredentials] = refresh_obj_list.pop()
            refresh_obj_token_expiry: Final[datetime.datetime] = refresh_obj.access_token_exiry
            refresh_time_remaining = min(refresh_window.total_seconds(), (refresh_obj_token_expiry - refresh_time).total_seconds())
            refresh_time_remaining = int(refresh_time_remaining)
            if refresh_time_remaining > 0:
                # self.logger.info("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"sleeping {refresh_time_remaining} seconds"))
                await asyncio.sleep(refresh_time_remaining)
                continue

            refresh_character_id: Final = refresh_obj.character_id
            client_session = dict({
                EveSSO.ESI_ACCESS_TOKEN_ISSUED: int(refresh_obj.access_token_issued.timestamp()),
                EveSSO.ESI_ACCESS_TOKEN_EXPIRY: int(refresh_obj.access_token_exiry.timestamp()),
                EveSSO.ESI_CHARACTER_ID: refresh_obj.character_id,
                EveSSO.ESI_CORPORATION_ID: refresh_obj.corporation_id,
                EveSSO.ESI_REFRESH_TOKEN: refresh_obj.refresh_token,
            })

            async with await self.db.sessionmaker() as db, db.begin():
                query = await db.execute(
                    sqlalchemy.select(EveTables.PeriodicCredentials)
                    .where(EveTables.PeriodicCredentials.character_id == refresh_character_id))
                update_obj_set: Final = {result for result in query.scalars()}
                if not len(update_obj_set) == 1:
                    self.logger.error("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  "WUT"))
                    continue

                obj = update_obj_set.pop()
                if await self.esi_sso_refresh(client_session):

                    update_dict = {
                        "access_token_exiry": datetime.datetime.fromtimestamp(client_session.get(EveSSO.ESI_ACCESS_TOKEN_EXPIRY, 0)).replace(tzinfo=datetime.timezone.utc),
                        "access_token_issued": datetime.datetime.fromtimestamp(client_session.get(EveSSO.ESI_ACCESS_TOKEN_ISSUED, 0)).replace(tzinfo=datetime.timezone.utc),
                        "access_token": client_session.get(EveSSO.ESI_ACCESS_TOKEN, ''),
                        "refresh_token": client_session.get(EveSSO.ESI_REFRESH_TOKEN, '')
                    }

                    for k, v in update_dict.items():
                        if hasattr(obj, k):
                            setattr(obj, k, v)

                    self.logger.info("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"refresh completed for {refresh_obj.character_id}"))
                else:
                    self.logger.error("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"refresh failed for {refresh_obj.character_id}"))

                    obj.is_enabled = False
                await db.commit()

    async def esi_update_client_session(self, client_session: collections.abc.MutableMapping, token_response: dict, decoded_acess_token: dict):

        token_response = token_response or dict()
        decoded_acess_token = decoded_acess_token or dict()

        if len(decoded_acess_token) == 0:
            client_session.clear()
            return

        character_id: Final = int(decoded_acess_token.get('sub', '0').split(':')[-1])
        if character_id <= 0:
            client_session.clear()
            return

        client_session[EveSSO.ESI_CHARACTER_ID] = character_id
        client_session[EveSSO.ESI_CHARACTER_NAME] = decoded_acess_token.get('name', '')

        client_session[EveSSO.ESI_ACCESS_TOKEN] = token_response.get("access_token", '')
        client_session[EveSSO.ESI_REFRESH_TOKEN] = token_response.get('refresh_token', '')

        client_session[EveSSO.ESI_DEBUG_TOKEN] = token_response
        client_session[EveSSO.ESI_DEBUG_ACCESS_TOKEN] = decoded_acess_token

        client_session[EveSSO.ESI_ACCESS_TOKEN_ISSUED] = decoded_acess_token.get('iat', int(datetime.datetime.utcnow().timestamp()))
        client_session[EveSSO.ESI_ACCESS_TOKEN_EXPIRY] = decoded_acess_token.get('exp', int(datetime.datetime.utcnow().timestamp()))
        client_session[EveSSO.ESI_ACCESS_TOKEN_SCOPES] = decoded_acess_token.get('scp', [])

        corporation_id = client_session.get(EveSSO.ESI_CORPORATION_ID, 0)
        if corporation_id <= 0:
            session_headers: Final = {
                "Authorization": f"Bearer {client_session.get(EveSSO.ESI_ACCESS_TOKEN)}"
            }

            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=17), headers=session_headers) as http_session:
                async def _get_dict(url: str, http_session: aiohttp.ClientSession) -> dict:
                    request_params: Final = {
                        "datasource": "tranquility",
                        "language": "en"
                        }
                    attempts_remaining = 3
                    while attempts_remaining > 0:
                        async with http_session.get(url, params=request_params) as response:
                            if response.status in [200]:
                                return await response.json()
                            else:
                                attempts_remaining -= 1
                                self.logger.warning("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"{response.url} -> {response.status}"))
                                await asyncio.sleep(3)
                    return dict()

                task_list: Final = [
                    asyncio.ensure_future(_get_dict(f"https://esi.evetech.net/latest/characters/{character_id}/", http_session)),
                ]
                if "esi-characters.read_corporation_roles.v1" in client_session.get(EveSSO.ESI_ACCESS_TOKEN_SCOPES, []):
                    task_list.append(
                        asyncio.ensure_future(_get_dict(f"https://esi.evetech.net/latest/characters/{character_id}/roles/", http_session))
                    )

                character_result = None
                for gather_result in await asyncio.gather(*task_list):
                    result: Final[dict] = gather_result
                    if bool(result.get('roles', False)):
                        roles_result = result
                        client_session[EveSSO.ESI_CHARACTER_HAS_ACCOUNTANT_ROLE] = 'Accountant' in roles_result.get('roles', [])
                        client_session[EveSSO.ESI_CHARACTER_HAS_DIRECTOR_ROLE] = 'Director' in roles_result.get('roles', [])
                        client_session[EveSSO.ESI_CHARACTER_HAS_STATION_MANAGER_ROLE] = 'Station_Manager' in roles_result.get('roles', [])
                    elif bool(result.get('name', False)):
                        character_result = result
                        for k in [EveSSO.ESI_CORPORATION_ID, EveSSO.ESI_ALLIANCE_ID]:
                            v = character_result.get(k, None)
                            if v is not None:
                                client_session[k] = v
                        corporation_id = client_session.get(EveSSO.ESI_CORPORATION_ID, 0)

                # Save the character info
                if self.db is not None and character_result is not None:

                    async with await self.db.sessionmaker() as db, db.begin():
                        character_query = sqlalchemy.select(EveTables.Character).where(
                            EveTables.Character.character_id == character_id)
                        character_query_results = await db.execute(character_query)
                        character_set = {result for result in character_query_results.scalars()}

                        if len(character_set) > 0:
                            [await db.delete(x) for x in character_set]

                        edict = dict({
                            "character_id": int(character_id)
                        })
                        for k, v in character_result.items():
                            if k in ["corporation_id", "alliance_id"]:
                                v = int(v)
                            elif k in ["birthday"]:
                                v = dateutil.parser.parse(v).replace(tzinfo=zoneinfo.ZoneInfo("UTC"))
                            elif k not in ["name"]:
                                continue
                            edict[k] = v

                        obj = EveTables.Character(**edict)
                        if obj is not None:
                            db.add(obj)
                            await db.commit()

                # Setup the periodic credentials
                if self.db is not None:

                    periodic_credentials = {
                        "character_id": character_id,
                        "corporation_id": corporation_id,
                        "access_token_issued": datetime.datetime.fromtimestamp(client_session.get(EveSSO.ESI_ACCESS_TOKEN_ISSUED, 0)).replace(tzinfo=datetime.timezone.utc),
                        "access_token_exiry": datetime.datetime.fromtimestamp(client_session.get(EveSSO.ESI_ACCESS_TOKEN_EXPIRY, 0)).replace(tzinfo=datetime.timezone.utc),
                        "access_token": client_session.get(EveSSO.ESI_ACCESS_TOKEN, ''),
                        "refresh_token": client_session.get(EveSSO.ESI_REFRESH_TOKEN, ''),
                        "is_station_manager_role": bool(client_session.get(EveSSO.ESI_CHARACTER_HAS_STATION_MANAGER_ROLE, False)),
                        "is_director_role": bool(client_session.get(EveSSO.ESI_CHARACTER_HAS_DIRECTOR_ROLE, False)),
                        "is_accountant_role": bool(client_session.get(EveSSO.ESI_CHARACTER_HAS_ACCOUNTANT_ROLE, False)),
                        "is_permitted": bool(client_session.get(EveSSO.ESI_CHARACTER_HAS_STATION_MANAGER_ROLE, False)),
                        "is_enabled": bool(client_session.get(EveSSO.ESI_CHARACTER_HAS_STATION_MANAGER_ROLE, False)),
                    }

                    async with await self.db.sessionmaker() as db, db.begin():
                        character_query = sqlalchemy.select(EveTables.PeriodicCredentials).where(
                            EveTables.PeriodicCredentials.character_id == client_session[EveSSO.ESI_CHARACTER_ID])
                        character_query_results = await db.execute(character_query)
                        character_set = {result for result in character_query_results.scalars()}
                        obj = None
                        if len(character_set) > 0:
                            obj = character_set.pop()
                            for k, v in periodic_credentials.items():
                                if hasattr(obj, k):
                                    setattr(obj, k, v)
                        else:
                            new_periodic_credentials = {
                            }
                            obj = EveTables.PeriodicCredentials(**{**periodic_credentials, **new_periodic_credentials})
                            db.add(obj)
                        db.commit()

    async def esi_process_token(self, client_session: collections.abc.MutableMapping, token_response: dict) -> dict:

        jwt_unverified_header: Final = jose.jwt.get_unverified_header(token_response["access_token"])
        jwt_key = None
        for jwk_candidate in self.jwks:
            if not type(jwk_candidate) == dict:
                continue

            jwt_key_match = True
            for header_key in set(jwt_unverified_header.keys()).intersection({"kid", "alg"}):
                if jwt_unverified_header.get(header_key) != jwk_candidate.get(header_key):
                    jwt_key_match = False
                    break

            if jwt_key_match:
                jwt_key = jwk_candidate
                break

        decoded_acess_token = None
        if jwt_key is not None:
            try:
                decoded_acess_token = jose.jwt.decode(token_response["access_token"], key=jwt_key, issuer=self.JWT_ISSUERS, audience=self.JWT_AUDIENCE)
            except jose.exceptions.JWTError as ex:
                self.logger.error(f"{inspect.currentframe().f_code.co_name}: {ex}")

        return decoded_acess_token or dict()

    def esi_sso_login(self) -> quart.redirect:
        client_session: Final = quart.session

        client_session[self.APP_SESSION_STATE] = uuid.uuid4().hex

        redirect_params: Final = [
            'response_type=code',
            f'redirect_uri={self.callback_url}',
            f'client_id={self.client_id}',
            f'scope={urllib.parse.quote(" ".join(self.scopes))}',
            f'state={client_session[self.APP_SESSION_STATE]}'
        ]

        redirect_url: Final = f"{self.configuration['authorization_endpoint']}?{'&'.join(redirect_params)}"

        return quart.redirect(redirect_url)

    async def esi_sso_logout(self) -> quart.redirect:
        client_session: Final = quart.session

        # Disable periodic on explicit logout
        if self.db is not None:
            async with await self.db.sessionmaker() as db, db.begin():
                character_query = sqlalchemy.select(EveTables.PeriodicCredentials).where(
                    EveTables.PeriodicCredentials.character_id == client_session[EveSSO.ESI_CHARACTER_ID])
                character_query_results = await db.execute(character_query)
                character_set = {result for result in character_query_results.scalars()}
                obj = None
                if len(character_set) > 0:
                    obj: EveTables.PeriodicCredentials = character_set.pop()
                    obj.is_enabled = False
                    obj.is_permitted = False
                    await db.commit()

        client_session.clear()

        return quart.redirect("/")

    async def esi_sso_callback(self) -> quart.redirect:

        client_session: Final = quart.session

        required_callback_keys: Final = ["code", "state"]
        if not all(map(lambda x: bool(quart.request.args.get(x)), required_callback_keys)):
            quart.abort(500, f"invalid call to {self.callback_route}")

        if quart.request.args["state"] != client_session.get(self.APP_SESSION_STATE):
            quart.abort(500, f"invalid session state in {self.callback_route}")

        post_token_url = f"{self.configuration['token_endpoint']}"
        basic_auth: Final = f"{self.client_id}:{self.client_secret}"
        post_session_headers: Final = {
            "Authorization": f"Basic {base64.urlsafe_b64encode(basic_auth.encode('utf-8')).decode()}",
            "Host": self.configuration["issuer"],
        }
        post_body: Final = {
            "grant_type": "authorization_code",
            "code": quart.request.args['code'],
        }

        token_response = dict()
        async with aiohttp.ClientSession(headers=post_session_headers) as http_session:
            async with http_session.post(post_token_url, data=post_body) as response:
                if response.status in [200]:
                    token_response = dict(await response.json())

        required_response_keys: Final = ["access_token", "token_type", "refresh_token"]
        if not all(map(lambda x: bool(token_response.get(x)), required_response_keys)):
            quart.abort(500, f"invalid response to {post_token_url}")

        decoded_acess_token: Final = await self.esi_process_token(client_session, token_response)
        if decoded_acess_token is None:
            quart.abort(500, "invalid jwt in token_response")

        await self.esi_update_client_session(client_session, token_response, decoded_acess_token)

        acl_set: Final = set()
        async with await self.db.sessionmaker() as db, db.begin():
            acl_query = sqlalchemy.select(EveTables.AccessControls)
            acl_query_result = await db.execute(acl_query)
            acl_set |= {result for result in acl_query_result.scalars()}

        acl_pass = False
        acl_evaluations = [
            (EveAccessType.ALLIANCE, client_session.get(EveSSO.ESI_ALLIANCE_ID, 0)),
            (EveAccessType.CORPORATION, client_session.get(EveSSO.ESI_CORPORATION_ID, 0)),
            (EveAccessType.CHARACTER, client_session.get(EveSSO.ESI_CHARACTER_ID, 0)),
        ]

        for acl_type, acl_id in acl_evaluations:
            for acl in filter(lambda x: x.type == acl_type, acl_set):
                if not isinstance(acl, EveTables.AccessControls):
                    continue
                if acl_id == acl.id:
                    acl_pass = acl.permit
                    self.logger.info(f"{inspect.currentframe().f_code.co_name}: {acl}: {acl_pass}")

        client_session[EveSSO.APP_PERMITTED] = acl_pass

        return quart.redirect("/")

    async def esi_sso_refresh(self, client_session: collections.abc.MutableMapping) -> None:

        now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

        if now < datetime.datetime.fromtimestamp(client_session.get(EveSSO.ESI_ACCESS_TOKEN_ISSUED, 0)).replace(tzinfo=datetime.timezone.utc):
            return

        if (now + datetime.timedelta(minutes=5)) < datetime.datetime.fromtimestamp(client_session.get(EveSSO.ESI_ACCESS_TOKEN_EXPIRY, 0)).replace(tzinfo=datetime.timezone.utc):
            return

        token_response = dict()
        decoded_acess_token = dict()

        post_session_headers: Final = {
            "Host": self.configuration["issuer"],
        }

        async with aiohttp.ClientSession(headers=post_session_headers) as http_session:
            post_token_url = f"{self.configuration['token_endpoint']}"
            post_body: Final = {
                "grant_type": "refresh_token",
                "refresh_token": client_session.get(EveSSO.ESI_REFRESH_TOKEN, ''),
                "client_id": self.client_id
            }
            async with http_session.post(post_token_url, data=post_body) as response:
                if response.status in [200]:
                    token_response = dict(await response.json())

        required_response_keys: Final = ["access_token", "token_type", "refresh_token"]
        if all(map(lambda x: bool(token_response.get(x)), required_response_keys)):
            decoded_acess_token = await self.esi_process_token(client_session, token_response)

        if len(decoded_acess_token) > 0:
            await self.esi_update_client_session(client_session, token_response, decoded_acess_token)

        return len(decoded_acess_token) > 0
