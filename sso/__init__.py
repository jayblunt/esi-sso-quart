import asyncio
import base64
import collections.abc
import datetime
import inspect
import logging
import typing
import urllib.parse
import uuid

import aiohttp
import aiohttp.client_exceptions
import dateutil.parser
import jose.exceptions
import jose.jwt
import opentelemetry.trace
import quart
import sqlalchemy
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql

from db import EveAuthType, EveDatabase, EveTables
from telemetry import otel, otel_add_error, otel_add_event, otel_add_exception


class EveSSO:

    CONNFIGURATION_URL: typing.Final = 'https://login.eveonline.com/.well-known/oauth-authorization-server'

    JWT_ISSUERS: typing.Final = ["login.eveonline.com", "https://login.eveonline.com"]
    JWT_AUDIENCE: typing.Final = "EVE Online"

    APP_SESSION_ID: typing.Final = "app_session_id"
    APP_SESSION_SCOPES: typing.Final = "app_session_scopes"
    APP_SESSION_TYPE: typing.Final = "app_session_type"

    ESI_CHARACTER_NAME: typing.Final = "character_name"
    ESI_CHARACTER_ID: typing.Final = "character_id"
    ESI_CORPORATION_ID: typing.Final = "corporation_id"
    ESI_ALLIANCE_ID: typing.Final = "alliance_id"

    ESI_CHARACTER_HAS_ACCOUNTANT_ROLE: typing.Final = "has_accountant_role"
    ESI_CHARACTER_HAS_DIRECTOR_ROLE: typing.Final = "has_director_role"
    ESI_CHARACTER_HAS_STATION_MANAGER_ROLE: typing.Final = "has_station_manager_role"

    ESI_ACCESS_TOKEN: typing.Final = "access_token"
    ESI_ACCESS_TOKEN_ISSUED: typing.Final = "access_token_issued"
    ESI_ACCESS_TOKEN_EXPIRY: typing.Final = "access_token_expiry"
    ESI_ACCESS_TOKEN_SCOPES: typing.Final = "access_token_scopes"

    ESI_REFRESH_TOKEN: typing.Final = "refresh_token"

    ESI_DEBUG_TOKEN: typing.Final = "debug_token"
    ESI_DEBUG_ACCESS_TOKEN: typing.Final = "debug_access_token"

    ERROR_SLEEP_TIME: typing.Final = 7
    ERROR_RETRY_COUNT: typing.Final = 11

    @otel
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
                 logger: logging.Logger | None = None) -> None:

        self.app: typing.Final = app
        self.db: typing.Final = db
        self.logger: typing.Final = app.logger

        self.client_id: typing.Final = client_id
        self.client_secret: typing.Final = client_secret

        self.configuration_url: typing.Final = configuration_url or self.CONNFIGURATION_URL
        self.scopes: typing.Final = scopes

        self.login_route: typing.Final = login_route
        self.logout_route: typing.Final = logout_route
        self.callback_route: typing.Final = callback_route

        self.common_params: typing.Final = dict()
        self.configuration: dict = None
        self.jwks_uri: str = None
        self.jwks: list[dict] = None
        self.refresh_jwks_task: asyncio.Task = None

        self.refresh_token_task: asyncio.Task = None

        @app.before_serving
        @otel
        async def _esi_sso_setup() -> None:
            self.configuration = await self._get_json(self.configuration_url)

            required_configuration_keys = ["token_endpoint", "authorization_endpoint", "issuer", "jwks_uri"]
            if not all(map(lambda x: bool(self.configuration.get(x)), required_configuration_keys)):
                raise Exception(f"{inspect.currentframe().f_code.co_name}: configuration at {self.configuration_url} is invalid")

            self.jwks_uri = self.configuration["jwks_uri"]
            self.jwks = await self._get_jwks(self.jwks_uri)
            self.refresh_jwks_task = asyncio.create_task(self._refresh_jwks_task(), name="_refresh_jwks_task")

            self.refresh_token_task = asyncio.create_task(self._refresh_token_task(), name="_refresh_token_task")

            app.add_url_rule(self.callback_route, self.callback_endpoint, view_func=self.esi_sso_callback, methods=["GET"])
            app.add_url_rule(self.logout_route, self.logout_endpoint, view_func=self.esi_sso_logout, methods=["GET"])
            app.add_url_rule(self.login_route, self.login_endpoint, view_func=self.esi_sso_login, methods=["GET"], defaults={'variant': 'default'})
            app.add_url_rule(f"{self.login_route}/<string:variant>", self.login_endpoint, view_func=self.esi_sso_login, methods=["GET"])

        @app.after_serving
        @otel
        async def _esi_sso_teardown() -> None:
            for task in [self.refresh_jwks_task, self.refresh_token_task]:
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

    @otel
    async def _get_url(self, http_session: aiohttp.ClientSession, url: str, request_params: dict | None = None) -> list | None:

        request_params = request_params or dict()
        attempts_remaining = self.ERROR_RETRY_COUNT
        while attempts_remaining > 0:
            async with await http_session.get(url, params=self.common_params | request_params) as response:
                if response.status in [200]:
                    return await response.json()
                else:
                    attempts_remaining -= 1
                    otel_add_error(f"{response.url} -> {response.status}")
                    self.logger.warning(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {response.url} -> {response.status}")
                    if response.status in [403]:
                        return None
                    if attempts_remaining > 0:
                        await asyncio.sleep(self.ERROR_SLEEP_TIME)

        return None

    @otel
    async def _post_url(self, http_session: aiohttp.ClientSession, url: str, body: dict) -> list | None:

        attempts_remaining = self.ERROR_RETRY_COUNT
        while attempts_remaining > 0:
            async with await http_session.post(url, data=body) as response:
                if response.status in [200]:
                    return await response.json()
                else:
                    attempts_remaining -= 1
                    otel_add_error(f"{response.url} -> {response.status}")
                    self.logger.warning(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {response.url} -> {response.status}")
                    if response.status in [403]:
                        return None
                    if attempts_remaining > 0:
                        await asyncio.sleep(self.ERROR_SLEEP_TIME)

        return None

    @otel
    async def _get_json(self, url: str, esi_access_token: str = '') -> dict:
        session_headers: typing.Final = dict()
        if len(esi_access_token) > 0:
            session_headers["Authorization"] = f"Bearer {esi_access_token}"

        async with aiohttp.ClientSession(headers=session_headers) as http_session:
            return await self._get_url(http_session, url)
        return None

    @otel
    async def _get_jwks(self, url: str) -> list[dict]:
        payload: typing.Final = await self._get_json(url)
        return payload.get("keys", [])

    async def _refresh_jwks_task(self) -> None:
        while True:
            await asyncio.sleep(300)
            try:
                new_jwks: typing.Final = await self._get_jwks(self.jwks_uri)
                if new_jwks is not None:
                    self.jwks = new_jwks
            except Exception as ex:
                otel_add_exception(ex)
                self.app.logger.error(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

    async def _refresh_token_task(self) -> None:
        refresh_buffer: typing.Final = datetime.timedelta(seconds=30)
        refresh_interval: typing.Final = datetime.timedelta(seconds=300) - refresh_buffer
        while True:
            now: typing.Final = datetime.datetime.now(tz=datetime.timezone.utc)

            refresh_obj: EveTables.PeriodicCredentials = None
            try:
                async with await self.db.sessionmaker() as session:
                    session: sqlalchemy.ext.asyncio.AsyncSession

                    query = (
                        sqlalchemy.select(EveTables.PeriodicCredentials)
                        .where(EveTables.PeriodicCredentials.is_permitted.is_(True))
                        .order_by(sqlalchemy.asc(EveTables.PeriodicCredentials.access_token_expiry))
                        .limit(1)
                    )
                    query_results: sqlalchemy.engine.Result = await session.execute(query)
                    refresh_obj: EveTables.PeriodicCredentials = query_results.scalar_one_or_none()

            except Exception as ex:
                otel_add_exception(ex)
                self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")
                await asyncio.sleep(refresh_interval.total_seconds())
                continue

            if refresh_obj is None:
                self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: no permitted credentials")
                await asyncio.sleep(refresh_interval.total_seconds())
                continue

            if refresh_obj.access_token_expiry > now + refresh_buffer:
                remaining_interval: datetime.timedelta = (refresh_obj.access_token_expiry) - (now + refresh_buffer)
                remaining_sleep_interval = min(refresh_interval.total_seconds(), remaining_interval.total_seconds())
                # self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {refresh_obj.character_id} refresh in {int(remaining_interval.total_seconds())}, sleeping {int(remaining_sleep_interval)}")
                await asyncio.sleep(remaining_sleep_interval)
                continue

            refresh_character_id: typing.Final = refresh_obj.character_id

            client_session = dict({
                EveSSO.ESI_ACCESS_TOKEN_ISSUED: int(refresh_obj.access_token_issued.timestamp()),
                EveSSO.ESI_ACCESS_TOKEN_EXPIRY: int(refresh_obj.access_token_expiry.timestamp()),
                EveSSO.ESI_CHARACTER_ID: refresh_obj.character_id,
                EveSSO.ESI_CORPORATION_ID: refresh_obj.corporation_id,
                EveSSO.APP_SESSION_ID: refresh_obj.session_id,
                EveSSO.ESI_REFRESH_TOKEN: refresh_obj.refresh_token,
            })

            try:
                async with await self.db.sessionmaker() as session, session.begin():

                    query = (
                        sqlalchemy.select(EveTables.PeriodicCredentials)
                        .where(EveTables.PeriodicCredentials.character_id == refresh_character_id)
                        .limit(1)
                    )
                    query_results: sqlalchemy.engine.Result = await session.execute(query)
                    refresh_character_obj: EveTables.PeriodicCredentials = query_results.scalar_one_or_none()

                    if refresh_character_obj is None:
                        self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {refresh_character_id} not found")
                        await asyncio.sleep(int(refresh_interval.total_seconds()))
                        continue

                    if await self.esi_sso_refresh(session, client_session):
                        update_dict = {
                            "access_token_expiry": datetime.datetime.fromtimestamp(client_session.get(EveSSO.ESI_ACCESS_TOKEN_EXPIRY, 0), tz=datetime.timezone.utc),
                            "access_token_issued": datetime.datetime.fromtimestamp(client_session.get(EveSSO.ESI_ACCESS_TOKEN_ISSUED, 0), tz=datetime.timezone.utc),
                            "access_token": client_session.get(EveSSO.ESI_ACCESS_TOKEN, ''),
                            "refresh_token": client_session.get(EveSSO.ESI_REFRESH_TOKEN, '')
                        }

                        for k, v in update_dict.items():
                            if hasattr(refresh_character_obj, k) and getattr(refresh_character_obj, k) != v:
                                setattr(refresh_character_obj, k, v)

                        self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {refresh_obj.character_id} token refreshed")
                    else:
                        otel_add_error("token refreshed failed")
                        self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {refresh_obj.character_id} token refreshed failed")

                        refresh_character_obj.is_enabled = False
                        refresh_character_obj.is_permitted = False
                    session.add(refresh_character_obj)
                    await session.commit()

            except Exception as ex:
                otel_add_exception(ex)
                self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

            try:
                async with await self.db.sessionmaker() as session, session.begin():
                    session.add(EveTables.AuthLog(character_id=client_session.get(EveSSO.ESI_CHARACTER_ID, 0), session_id=client_session.get(EveSSO.APP_SESSION_ID, ''), auth_type=EveAuthType.REFRESH))
                    await session.commit()
            except Exception as ex:
                otel_add_exception(ex)
                self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

    @otel
    async def esi_decode_token(self, client_session: collections.abc.MutableMapping, token_response: dict) -> dict:

        jwt_unverified_header: typing.Final = jose.jwt.get_unverified_header(token_response["access_token"])
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
                otel_add_exception(ex)
                self.logger.error(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        return decoded_acess_token or dict()

    @otel
    async def esi_sso_login(self, variant: str) -> quart.redirect:
        client_session: typing.Final = quart.session

        client_session[EveSSO.APP_SESSION_ID] = uuid.uuid4().hex

        login_scopes = ['publicData']
        if variant == 'user':
            login_scopes = ['publicData']
        elif variant == 'contributor':
            login_scopes = self.scopes

        if len(login_scopes) == 1 and 'publicData' in login_scopes:
            client_session[EveSSO.APP_SESSION_TYPE] = "USER"
        else:
            client_session[EveSSO.APP_SESSION_TYPE] = "CONTRIBUTOR"

        client_session[EveSSO.APP_SESSION_SCOPES] = login_scopes

        redirect_params: typing.Final = [
            'response_type=code',
            f'redirect_uri={self.callback_url}',
            f'client_id={self.client_id}',
            f'scope={urllib.parse.quote(" ".join(login_scopes))}',
            f'state={client_session[EveSSO.APP_SESSION_ID]}'
        ]

        redirect_url: typing.Final = f"{self.configuration['authorization_endpoint']}?{'&'.join(redirect_params)}"

        return quart.redirect(redirect_url)

    @otel
    async def esi_sso_logout(self) -> quart.redirect:
        client_session: typing.Final = quart.session

        character_id: typing.Final = client_session.get(EveSSO.ESI_CHARACTER_ID, 0)
        session_id: typing.Final = client_session.get(EveSSO.APP_SESSION_ID, '')

        if character_id > 0 and len(session_id) > 0:

            try:
                # Disable periodic on explicit logout
                async with await self.db.sessionmaker() as session, session.begin():
                    session: sqlalchemy.ext.asyncio.AsyncSession

                    query = (
                        sqlalchemy.select(EveTables.PeriodicCredentials)
                        .where(EveTables.PeriodicCredentials.character_id == character_id)
                        .limit(1)
                    )

                    query_results: sqlalchemy.engine.Result = await session.execute(query)
                    obj: EveTables.PeriodicCredentials = query_results.scalar_one_or_none()
                    if obj:
                        obj.is_enabled = False
                        obj.is_permitted = False
                        session.add(obj)

                    await session.commit()

            except Exception as ex:
                otel_add_exception(ex)
                self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

            try:
                async with await self.db.sessionmaker() as session, session.begin():
                    session.add(EveTables.AuthLog(character_id=character_id, session_id=session_id, auth_type=EveAuthType.LOGOUT))
                    await session.commit()
            except Exception as ex:
                otel_add_exception(ex)
                self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        otel_add_event("logout", {"session_id": session_id})

        client_session.clear()

        return quart.redirect("/")

    @otel
    async def esi_sso_callback(self) -> quart.redirect:

        client_session: typing.Final = quart.session

        session_id: typing.Final = client_session.get(EveSSO.APP_SESSION_ID, '')

        required_callback_keys: typing.Final = ["code", "state"]
        if not all(map(lambda x: bool(quart.request.args.get(x)), required_callback_keys)):
            quart.abort(500, f"invalid call to {self.callback_route}")

        if quart.request.args["state"] != session_id:
            quart.abort(500, f"invalid session state in {self.callback_route}")

        post_token_url = self.configuration['token_endpoint']
        basic_auth: typing.Final = f"{self.client_id}:{self.client_secret}"
        post_session_headers: typing.Final = {
            "Authorization": f"Basic {base64.urlsafe_b64encode(basic_auth.encode('utf-8')).decode()}",
            "Host": self.configuration["issuer"],
        }
        post_body: typing.Final = {
            "grant_type": "authorization_code",
            "code": quart.request.args['code'],
        }

        token_response = dict()
        async with aiohttp.ClientSession(headers=post_session_headers) as http_session:
            token_response = await self._post_url(http_session, post_token_url, post_body)

        required_response_keys: typing.Final = ["access_token", "token_type", "refresh_token"]
        if not all(map(lambda x: bool(token_response.get(x)), required_response_keys)):
            quart.abort(500, f"invalid response to {post_token_url}")

        decoded_acess_token: typing.Final = await self.esi_decode_token(client_session, token_response)
        if decoded_acess_token is None:
            quart.abort(500, "invalid jwt in token_response")

        try:
            async with await self.db.sessionmaker() as session, session.begin():
                session: sqlalchemy.ext.asyncio.AsyncSession
                await self.esi_update_client_session(session, client_session, token_response, decoded_acess_token)
                await session.commit()
        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        character_id: typing.Final = client_session.get(EveSSO.ESI_CHARACTER_ID, 0)

        try:
            async with await self.db.sessionmaker() as session, session.begin():
                login_auth_type = EveAuthType.LOGIN_USER
                if len(client_session.get(EveSSO.APP_SESSION_SCOPES, [])) > 1:
                    login_auth_type = EveAuthType.LOGIN_CONTRIBUTOR
                session.add(EveTables.AuthLog(character_id=character_id, session_id=session_id, auth_type=login_auth_type))
                await session.commit()
        except Exception as ex:
            otel_add_exception(ex)
            self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

        otel_add_event("login", {"session_id": session_id})

        return quart.redirect("/")

    @otel
    async def esi_sso_refresh(self, session: sqlalchemy.ext.asyncio.AsyncSession, client_session: collections.abc.MutableMapping) -> None:

        now: typing.Final = datetime.datetime.now(tz=datetime.timezone.utc)

        if now < datetime.datetime.fromtimestamp(client_session.get(EveSSO.ESI_ACCESS_TOKEN_ISSUED, 0), tz=datetime.timezone.utc):
            return

        if (now + datetime.timedelta(minutes=5)) < datetime.datetime.fromtimestamp(client_session.get(EveSSO.ESI_ACCESS_TOKEN_EXPIRY, 0), tz=datetime.timezone.utc):
            return

        token_response = dict()
        decoded_acess_token = dict()

        post_session_headers: typing.Final = {
            "Host": self.configuration["issuer"],
        }

        async with aiohttp.ClientSession(headers=post_session_headers) as http_session:
            post_token_url = self.configuration['token_endpoint']
            post_body: typing.Final = {
                "grant_type": "refresh_token",
                "refresh_token": client_session.get(EveSSO.ESI_REFRESH_TOKEN, ''),
                "client_id": self.client_id
            }
            token_response = await self._post_url(http_session, post_token_url, post_body)

        required_response_keys: typing.Final = ["access_token", "token_type", "refresh_token"]
        if all(map(lambda x: bool(token_response.get(x)), required_response_keys)):
            decoded_acess_token = await self.esi_decode_token(client_session, token_response)

        if len(decoded_acess_token) > 0:
            await self.esi_update_client_session(session, client_session, token_response, decoded_acess_token)

        return len(decoded_acess_token) > 0

    @otel
    async def esi_update_client_session(self, session: sqlalchemy.ext.asyncio.AsyncSession, client_session: collections.abc.MutableMapping, token_response: dict, decoded_acess_token: dict):

        token_response = token_response or dict()
        decoded_acess_token = decoded_acess_token or dict()

        if len(decoded_acess_token) == 0:
            client_session.clear()
            return

        character_id: typing.Final = int(decoded_acess_token.get('sub', '0').split(':')[-1])
        if character_id <= 0:
            client_session.clear()
            return

        now: typing.Final = datetime.datetime.now(tz=datetime.timezone.utc)

        client_session[EveSSO.ESI_CHARACTER_ID] = character_id
        client_session[EveSSO.ESI_CHARACTER_NAME] = decoded_acess_token.get('name', '')

        client_session[EveSSO.ESI_ACCESS_TOKEN] = token_response.get("access_token", '')
        client_session[EveSSO.ESI_REFRESH_TOKEN] = token_response.get('refresh_token', '')

        client_session[EveSSO.ESI_DEBUG_TOKEN] = token_response
        client_session[EveSSO.ESI_DEBUG_ACCESS_TOKEN] = decoded_acess_token

        client_session[EveSSO.ESI_ACCESS_TOKEN_ISSUED] = decoded_acess_token.get('iat', int(now.timestamp()))
        client_session[EveSSO.ESI_ACCESS_TOKEN_EXPIRY] = decoded_acess_token.get('exp', int(now.timestamp()))
        client_session[EveSSO.ESI_ACCESS_TOKEN_SCOPES] = decoded_acess_token.get('scp', [])

        corporation_id = client_session.get(EveSSO.ESI_CORPORATION_ID, 0)

        # What is going on here?
        # Looks like only collecting information if the corporation_id is 0
        if not corporation_id > 0:
            session_headers: typing.Final = {
                "Authorization": f"Bearer {client_session.get(EveSSO.ESI_ACCESS_TOKEN)}"
            }

            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=17), headers=session_headers) as http_session:
                request_params: typing.Final = {
                    "datasource": "tranquility",
                    "language": "en"
                }

                task_list: typing.Final = [
                    self._get_url(http_session, f"https://esi.evetech.net/latest/characters/{character_id}/", request_params),
                ]
                if "esi-characters.read_corporation_roles.v1" in client_session.get(EveSSO.ESI_ACCESS_TOKEN_SCOPES, []):
                    task_list.append(
                        self._get_url(http_session, f"https://esi.evetech.net/latest/characters/{character_id}/roles/", request_params)
                    )

                characters_result = None
                characters_roles_result = dict()

                for gather_result in await asyncio.gather(*task_list):
                    result: dict = gather_result
                    if bool(result.get('roles', False)):
                        characters_roles_result |= result
                    elif bool(result.get('name', False)):
                        characters_result = result
                        for k in [EveSSO.ESI_CORPORATION_ID, EveSSO.ESI_ALLIANCE_ID]:
                            v = characters_result.get(k, None)
                            if v is not None:
                                client_session[k] = v
                        corporation_id = client_session.get(EveSSO.ESI_CORPORATION_ID, 0)

                client_session[EveSSO.ESI_CHARACTER_HAS_ACCOUNTANT_ROLE] = 'Accountant' in characters_roles_result.get('roles', [])
                client_session[EveSSO.ESI_CHARACTER_HAS_DIRECTOR_ROLE] = 'Director' in characters_roles_result.get('roles', [])
                client_session[EveSSO.ESI_CHARACTER_HAS_STATION_MANAGER_ROLE] = 'Station_Manager' in characters_roles_result.get('roles', [])

                # Save the character info - we re-do this on each cycle why?
                if characters_result is not None:

                    edict = dict({
                        "character_id": int(character_id)
                    })
                    for k, v in characters_result.items():
                        if k in ["corporation_id", "alliance_id"]:
                            v = int(v)
                        elif k in ["birthday"]:
                            v = dateutil.parser.parse(v).replace(tzinfo=datetime.timezone.utc)
                        elif k not in ["name"]:
                            continue
                        edict[k] = v

                    try:

                        query = (
                            sqlalchemy.select(EveTables.Character)
                            .where(EveTables.Character.character_id == character_id)
                        )
                        result: sqlalchemy.engine.Result = await session.execute(query)
                        obj: EveTables.Character = result.scalar_one_or_none()
                        if obj:
                            for k, v in edict.items():
                                if k in obj.__table__.columns.keys():
                                    if getattr(obj, k) == v:
                                        continue
                                    self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: character_id:{character_id} {k} changed from {getattr(obj, k)} to {v}")
                                    obj[k] = v
                        else:
                            obj = EveTables.Character(**edict)

                        session.add(obj)

                    except Exception as ex:
                        otel_add_exception(ex)
                        self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")

                try:
                    # Setup / Update the periodic credentials record

                    periodic_credentials = {
                        "character_id": character_id,
                        "corporation_id": corporation_id,
                        "session_id": client_session.get(EveSSO.APP_SESSION_ID, ''),
                        "access_token_issued": datetime.datetime.fromtimestamp(client_session.get(EveSSO.ESI_ACCESS_TOKEN_ISSUED, 0), tz=datetime.timezone.utc),
                        "access_token_expiry": datetime.datetime.fromtimestamp(client_session.get(EveSSO.ESI_ACCESS_TOKEN_EXPIRY, 0), tz=datetime.timezone.utc),
                        "access_token": client_session.get(EveSSO.ESI_ACCESS_TOKEN, ''),
                        "refresh_token": client_session.get(EveSSO.ESI_REFRESH_TOKEN, ''),
                        "is_station_manager_role": bool(client_session.get(EveSSO.ESI_CHARACTER_HAS_STATION_MANAGER_ROLE, False)),
                        "is_director_role": bool(client_session.get(EveSSO.ESI_CHARACTER_HAS_DIRECTOR_ROLE, False)),
                        "is_accountant_role": bool(client_session.get(EveSSO.ESI_CHARACTER_HAS_ACCOUNTANT_ROLE, False)),
                        "is_permitted": bool(client_session.get(EveSSO.ESI_CHARACTER_HAS_STATION_MANAGER_ROLE, False)),
                        "is_enabled": bool(client_session.get(EveSSO.ESI_CHARACTER_HAS_STATION_MANAGER_ROLE, False)),
                    }

                    query = (
                        sqlalchemy.select(EveTables.PeriodicCredentials)
                        .where(EveTables.PeriodicCredentials.character_id == character_id)
                        .limit(1)
                    )
                    query_result: sqlalchemy.engine.Result = await session.execute(query)
                    obj: EveTables.PeriodicCredentials = query_result.scalar_one_or_none()
                    if obj:
                        self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: character_id:{character_id} {obj}")
                        for k, v in periodic_credentials.items():
                            if hasattr(obj, k) and getattr(obj, k) != v:
                                setattr(obj, k, v)
                        self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: character_id:{character_id} {obj}")
                    else:
                        obj = EveTables.PeriodicCredentials(**periodic_credentials)
                    session.add(obj)

                except Exception as ex:
                    otel_add_exception(ex)
                    self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex}")
