import asyncio
import base64
import collections.abc
import datetime
import inspect
import logging
import urllib.parse
import uuid
import zoneinfo
from typing import Final

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
    # ESI_CHARACTER_BIRTHDAY: Final = "character_birthday"
    # ESI_CHARACTER_SECURITY_STATUS: Final = "character_security_status"

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

        @app.before_serving
        async def esi_sso_setup():
            self.configuration = await self.get_json(self.configuration_url)

            required_configuration_keys = ["token_endpoint", "authorization_endpoint", "issuer", "jwks_uri"]
            if not all(map(lambda x: bool(self.configuration.get(x)), required_configuration_keys)):
                raise Exception(f"{inspect.currentframe().f_code.co_name}: configuration at {self.configuration_url} is invalid")

            self.jwks_uri = self.configuration["jwks_uri"]
            self.jwks = await self.get_jwks(self.jwks_uri)
            self.jwks_task = asyncio.create_task(self.get_jwks_task())

            app.add_url_rule(self.callback_route, self.callback_endpoint, view_func=self.esi_sso_callback, methods=["GET"])
            app.add_url_rule(self.logout_route, self.logout_endpoint, view_func=self.esi_sso_logout, methods=["GET"])
            app.add_url_rule(self.login_route, self.login_endpoint, view_func=self.esi_sso_login, methods=["GET"])

        @app.after_serving
        async def esi_sso_teardown():
            if self.jwks_task:
                if not self.jwks_task.cancelled():
                    self.jwks_task.cancel()

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

    async def esi_process_token(self, client_session: collections.abc.MutableMapping, token_response: dict) -> bool:

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
                quart.abort(500, "invalid jwt in token_response")

        if decoded_acess_token is None:
            client_session[self.ESI_CHARACTER_ID] = 0

            for k in {self.ESI_ACCESS_TOKEN, self.ESI_REFRESH_TOKEN, self.ESI_DEBUG_TOKEN, self.ESI_DEBUG_ACCESS_TOKEN}.intersection(set(client_session.keys())):
                del client_session[k]

            client_session[self.ESI_ACCESS_TOKEN_ISSUED] = int(datetime.datetime.utcnow().timestamp())
            client_session[self.ESI_ACCESS_TOKEN_EXPIRY] = int(datetime.datetime.utcnow().timestamp())
            client_session[self.ESI_ACCESS_TOKEN_SCOPES] = []

        else:
            client_session[self.ESI_CHARACTER_ID] = int(decoded_acess_token.get('sub', '').split(':')[-1])
            client_session[self.ESI_CHARACTER_NAME] = decoded_acess_token.get('name', '')

            client_session[self.ESI_ACCESS_TOKEN] = token_response.get("access_token", '')
            client_session[self.ESI_ACCESS_TOKEN_ISSUED] = token_response.get('iat', int(datetime.datetime.utcnow().timestamp()))
            client_session[self.ESI_ACCESS_TOKEN_EXPIRY] = token_response.get('exp', int(datetime.datetime.utcnow().timestamp()))
            client_session[self.ESI_ACCESS_TOKEN_SCOPES] = decoded_acess_token.get('scp', [])

            client_session[self.ESI_REFRESH_TOKEN] = token_response.get('refresh_token', '')

            client_session[self.ESI_DEBUG_TOKEN] = token_response
            client_session[self.ESI_DEBUG_ACCESS_TOKEN] = decoded_acess_token

        if self.db is not None and decoded_acess_token is not None and self.db is not None:

            async with await self.db.sessionmaker() as db, db.begin():
                character_query = sqlalchemy.select(EveTables.Credentials).where(
                    EveTables.Credentials.character_id == client_session[self.ESI_CHARACTER_ID])
                character_query_results = await db.execute(character_query)
                character_set = {result for result in character_query_results.scalars()}

                if len(character_set) > 0:
                    [await db.delete(x) for x in character_set]

                edict = dict({
                    "character_id": client_session[self.ESI_CHARACTER_ID],
                    "json": token_response
                })

                obj = EveTables.Credentials(**edict)
                if obj is not None:
                    db.add(obj)
                    await db.commit()

        return decoded_acess_token is not None

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

        required_response_keys: Final = ["access_token", "token_type", "expires_in", "refresh_token"]
        if not all(map(lambda x: bool(token_response.get(x)), required_response_keys)):
            quart.abort(500, f"invalid response to {post_token_url}")

        await self.esi_process_token(client_session, token_response)

        character_id: Final = client_session.get(self.ESI_CHARACTER_ID, 0)
        if character_id > 0:

            common_params: Final = {"datasource": "tranquility"}
            session_headers: Final = {
                "Authorization": f"Bearer {client_session.get(self.ESI_ACCESS_TOKEN)}"
            }

            character_response = None
            async with aiohttp.ClientSession(headers=session_headers) as http_session:
                post_token_url = f"https://esi.evetech.net/latest/characters/{character_id}/"
                async with http_session.get(post_token_url, params=common_params) as response:
                    if response.status in [200]:
                        character_response = dict(await response.json())

            character_roles_response = None
            if "esi-characters.read_corporation_roles.v1" in client_session.get(self.ESI_ACCESS_TOKEN_SCOPES, []):
                async with aiohttp.ClientSession(headers=session_headers) as http_session:
                    post_character_roles_url = f"https://esi.evetech.net/latest/characters/{character_id}/roles"
                    async with http_session.get(post_character_roles_url, params=common_params) as response:
                        if response.status in [200]:
                            character_roles_response = dict(await response.json())

            if self.db is not None and character_response is not None:

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
                    for k, v in character_response.items():
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

            if character_response is not None:
                # for k in [self.ESI_CORPORATION_ID, self.ESI_ALLIANCE_ID, self.ESI_CHARACTER_SECURITY_STATUS, self.ESI_CHARACTER_BIRTHDAY]:
                for k in [self.ESI_CORPORATION_ID, self.ESI_ALLIANCE_ID]:
                    v = character_response.get(k, None)
                    if v is not None:
                        client_session[k] = v

            if character_roles_response is not None:
                client_session[self.ESI_CHARACTER_HAS_ACCOUNTANT_ROLE] = 'Accountant' in character_roles_response.get('roles', [])
                client_session[self.ESI_CHARACTER_HAS_DIRECTOR_ROLE] = 'Director' in character_roles_response.get('roles', [])
                client_session[self.ESI_CHARACTER_HAS_STATION_MANAGER_ROLE] = 'Station_Manager' in character_roles_response.get('roles', [])

        acl_set: Final = set()
        async with await self.db.sessionmaker() as db, db.begin():
            acl_query = sqlalchemy.select(EveTables.AccessControls)
            acl_query_result = await db.execute(acl_query)
            acl_set |= {result for result in acl_query_result.scalars()}

        acl_pass = False
        acl_evaluations = [
            (EveAccessType.ALLIANCE, client_session.get(self.ESI_ALLIANCE_ID, 0)),
            (EveAccessType.CORPORATION, client_session.get(self.ESI_CORPORATION_ID, 0)),
            (EveAccessType.CHARACTER, client_session.get(self.ESI_CHARACTER_ID, 0)),
        ]

        for acl_type, acl_id in acl_evaluations:
            for acl in filter(lambda x: x.type == acl_type, acl_set):
                if acl_id == acl.id:
                    acl_pass = acl.permit
                    self.logger.info(f"{inspect.currentframe().f_code.co_name}: {acl}: {acl_pass}")

        client_session[EveSSO.APP_PERMITTED] = acl_pass

        return quart.redirect("/")

    async def esi_sso_refresh(self, client_session: collections.abc.MutableMapping) -> quart.redirect:

        now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

        if now < datetime.datetime.fromtimestamp(client_session.get(self.ESI_ACCESS_TOKEN_ISSUED, 0)).replace(tzinfo=datetime.timezone.utc):
            return

        if (now + datetime.timedelta(minutes=5)) < datetime.datetime.fromtimestamp(client_session.get(self.ESI_ACCESS_TOKEN_EXPIRY, 0)).replace(tzinfo=datetime.timezone.utc):
            return

        post_token_url = f"{self.configuration['token_endpoint']}"
        post_session_headers: Final = {
            "Host": self.configuration["issuer"],
        }
        post_body: Final = {
            "grant_type": "refresh_token",
            "refresh_token": client_session.get(self.ESI_REFRESH_TOKEN, ''),
            "client_id": self.client_id
        }

        token_response = dict()
        async with aiohttp.ClientSession(headers=post_session_headers) as http_session:
            async with http_session.post(post_token_url, data=post_body) as response:
                if response.status in [200]:
                    token_response = dict(await response.json())

        required_response_keys: Final = ["access_token", "token_type", "refresh_token"]
        if all(map(lambda x: bool(token_response.get(x)), required_response_keys)):
            await self.esi_process_token(client_session, token_response)
