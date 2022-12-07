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

from support.telemetry import (otel, otel_add_error, otel_add_event,
                               otel_add_exception)

from .db import AppAuthType, AppDatabase, AppTables
from .events import SSOLoginEvent, SSOLogoutEvent, SSOTokenRefreshEvent


class AppSSOFunctions:


    @staticmethod
    async def authlog(db: AppDatabase, character_id: int, session_id: str, auth_type: AppAuthType) -> bool:
        try:
            async with await db.sessionmaker() as session:
                session.begin()
                session.add(AppTables.AuthLog(character_id=character_id, session_id=session_id, auth_type=auth_type))
                await session.commit()
            return True
        except Exception as ex:
            otel_add_exception(ex)
        return False


    @staticmethod
    async def remove_credentials(db: AppDatabase, character_id: int) -> bool:
        if not character_id > 0:
            return False

        try:
            async with await db.sessionmaker() as session:
                session: sqlalchemy.ext.asyncio.AsyncSession

                session.begin()
                query = (
                    sqlalchemy.delete(AppTables.PeriodicCredentials)
                    .where(AppTables.PeriodicCredentials.character_id == character_id)
                )
                await session.execute(query)
                await session.commit()
            return True
        except Exception as ex:
            otel_add_exception(ex)

        return False


    @staticmethod
    async def update_credentials(db: AppDatabase, character_id: int, edict: dict) -> bool:
        if not character_id > 0:
            return False

        epoch: typing.Final = datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)

        try:
            async with await db.sessionmaker() as session:
                session: sqlalchemy.ext.asyncio.AsyncSession

                session.begin()

                if edict.get(AppSSO.ESI_CHARACTER_NAME) is not None:
                    query = (
                        sqlalchemy.delete(AppTables.Character)
                        .where(AppTables.Character.character_id == character_id)
                    )
                    await session.execute(query)

                    session.add(AppTables.Character(
                        character_id=edict.get(AppSSO.ESI_CHARACTER_ID, 0),
                        corporation_id=edict.get(AppSSO.ESI_CORPORATION_ID, 0),
                        alliance_id=edict.get(AppSSO.ESI_ALLIANCE_ID, 0),
                        birthday=edict.get(AppSSO.ESI_CHARACTER_BIRTHDAY, epoch),
                        name=edict.get(AppSSO.ESI_CHARACTER_NAME, '')
                    ))

                query = (
                    sqlalchemy.delete(AppTables.PeriodicCredentials)
                    .where(AppTables.PeriodicCredentials.character_id == character_id)
                )
                await session.execute(query)

                session.add(AppTables.PeriodicCredentials(
                    character_id=edict.get(AppSSO.ESI_CHARACTER_ID, 0),
                    corporation_id=edict.get(AppSSO.ESI_CORPORATION_ID, 0),
                    is_permitted=bool(edict.get(AppSSO.ESI_CHARACTER_IS_STATION_MANAGER_ROLE, False)),
                    is_enabled=bool(edict.get(AppSSO.ESI_CHARACTER_IS_STATION_MANAGER_ROLE, False)),
                    is_director_role=bool(edict.get(AppSSO.ESI_CHARACTER_IS_DIRECTOR_ROLE, False)),
                    is_accountant_role=bool(edict.get(AppSSO.ESI_CHARACTER_IS_ACCOUNTANT_ROLE, False)),
                    is_station_manager_role=bool(edict.get(AppSSO.ESI_CHARACTER_IS_STATION_MANAGER_ROLE, False)),
                    session_id=edict.get(AppSSO.APP_SESSION_ID, ''),
                    access_token_issued=edict.get(AppSSO.ESI_ACCESS_TOKEN_ISSUED, epoch),
                    access_token_expiry=edict.get(AppSSO.ESI_ACCESS_TOKEN_EXPIRY, epoch),
                    refresh_token=edict.get(AppSSO.ESI_REFRESH_TOKEN, ''),
                    access_token=edict.get(AppSSO.ESI_ACCESS_TOKEN, '')
                ))

                await session.commit()
            return True
        except Exception as ex:
            otel_add_exception(ex)
        return False


class AppSSO:

    CONNFIGURATION_URL: typing.Final = 'https://login.eveonline.com/.well-known/oauth-authorization-server'

    JWT_ISSUERS: typing.Final = ["login.eveonline.com", "https://login.eveonline.com"]
    JWT_AUDIENCE: typing.Final = "EVE Online"

    APP_SESSION_TYPE: typing.Final = "session_type"

    ESI_CHARACTER_NAME: typing.Final = "character_name"
    ESI_CHARACTER_BIRTHDAY: typing.Final = "character_birthday"

    ESI_CHARACTER_ID: typing.Final = "character_id"
    ESI_CORPORATION_ID: typing.Final = "corporation_id"
    ESI_ALLIANCE_ID: typing.Final = "alliance_id"

    ESI_CHARACTER_IS_DIRECTOR_ROLE: typing.Final = "is_director_role"
    ESI_CHARACTER_IS_ACCOUNTANT_ROLE: typing.Final = "is_accountant_role"
    ESI_CHARACTER_IS_STATION_MANAGER_ROLE: typing.Final = "is_station_manager_role"

    ESI_ACCESS_TOKEN_ISSUED: typing.Final = "access_token_issued"
    ESI_ACCESS_TOKEN_EXPIRY: typing.Final = "access_token_expiry"

    ESI_SCOPES: typing.Final = "scopes"
    ESI_ACCESS_TOKEN: typing.Final = "access_token"
    ESI_REFRESH_TOKEN: typing.Final = "refresh_token"

    APP_SESSION_ID: typing.Final = "session_id"

    ERROR_SLEEP_TIME: typing.Final = 11
    ERROR_RETRY_COUNT: typing.Final = 7

    @otel
    def __init__(self,
                 app: quart.Quart,
                 db: AppDatabase,
                 outbound: asyncio.Queue,
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
        self.outbound: typing.Final = outbound
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
            # self.refresh_token_task = None

            app.add_url_rule(self.callback_route, self.callback_endpoint, view_func=self.esi_sso_callback, methods=["GET"])
            app.add_url_rule(self.logout_route, self.logout_endpoint, view_func=self.esi_sso_logout, methods=["GET"])
            app.add_url_rule(self.login_route, self.login_endpoint, view_func=self.esi_sso_login, methods=["GET"], defaults={'variant': 'user'})
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
                    if response.status in [400, 403]:
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
                    if response.status in [400, 403]:
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
        refresh_buffer: typing.Final = datetime.timedelta(seconds=60)
        refresh_interval: typing.Final = datetime.timedelta(seconds=300)
        while True:
            now: typing.Final = datetime.datetime.now(tz=datetime.timezone.utc)

            refresh_obj: AppTables.PeriodicCredentials = None
            try:
                async with await self.db.sessionmaker() as session:
                    session: sqlalchemy.ext.asyncio.AsyncSession

                    query = (
                        sqlalchemy.select(AppTables.PeriodicCredentials)
                        .where(AppTables.PeriodicCredentials.is_enabled.is_(True))
                        .order_by(sqlalchemy.asc(AppTables.PeriodicCredentials.access_token_expiry))
                        .limit(1)
                    )
                    query_results: sqlalchemy.engine.Result = await session.execute(query)
                    refresh_obj: AppTables.PeriodicCredentials = query_results.scalar_one_or_none()

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

            failure_edict: typing.Final = {
                AppSSO.APP_SESSION_ID: refresh_obj.session_id,
                AppSSO.ESI_CHARACTER_ID: refresh_obj.character_id,
                AppSSO.ESI_CORPORATION_ID: refresh_obj.corporation_id,
                AppSSO.ESI_CHARACTER_IS_DIRECTOR_ROLE: refresh_obj.is_director_role,
                AppSSO.ESI_CHARACTER_IS_ACCOUNTANT_ROLE: refresh_obj.is_accountant_role,
                AppSSO.ESI_CHARACTER_IS_STATION_MANAGER_ROLE: refresh_obj.is_station_manager_role,
                AppSSO.ESI_ACCESS_TOKEN_ISSUED: refresh_obj.access_token_issued,
                AppSSO.ESI_ACCESS_TOKEN_EXPIRY: refresh_obj.access_token_expiry,
                AppSSO.ESI_ACCESS_TOKEN: refresh_obj.access_token,
                AppSSO.ESI_REFRESH_TOKEN: refresh_obj.refresh_token,
            }

            character_id: typing.Final = refresh_obj.character_id
            session_id: typing.Final = refresh_obj.session_id
            refresh_token: typing.Final = refresh_obj.refresh_token

            edict = await self.esi_sso_refresh(session_id, refresh_token)
            if edict is None:
                edict = failure_edict

            await AppSSOFunctions.update_credentials(self.db, character_id, edict)

            # if self.outbound:
            #     await self.outbound.put(SSOTokenRefreshEvent(character_id=character_id, session_id=session_id))

            await AppSSOFunctions.authlog(self.db, character_id, session_id, AppAuthType.REFRESH)

    @otel
    async def esi_decode_token(self, session_id: str, token_response: dict) -> dict | None:

        required_response_keys: typing.Final = ["access_token", "token_type", "refresh_token"]
        if not all(map(lambda x: bool(token_response.get(x)), required_response_keys)):
            return None

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
                return None

        return await self.esi_unpack_token_data(session_id, token_response, decoded_acess_token)

    @otel
    async def esi_unpack_token_data(self, session_id: str, token_response: dict, decoded_acess_token: dict) -> dict:
        now: typing.Final = datetime.datetime.now(tz=datetime.timezone.utc)

        token_response = token_response or dict()
        decoded_acess_token = decoded_acess_token or dict()

        character_id: typing.Final = int(decoded_acess_token.get('sub', '0').split(':')[-1])
        if not character_id > 0:
            return None

        access_token: typing.Final = token_response.get('access_token', None)
        refresh_token: typing.Final = token_response.get('refresh_token', None)
        if access_token is None or refresh_token is None:
            return None

        scopes = decoded_acess_token.get('scp', [])
        if type(scopes) == str:
            scopes = [scopes]

        character_result = dict()
        character_roles_result = dict()
        session_headers: typing.Final = {
            "Authorization": f"Bearer {access_token}"
        }
        async with aiohttp.ClientSession(headers=session_headers) as http_session:
            request_params: typing.Final = {
                "datasource": "tranquility",
                "language": "en"
            }

            task_list: typing.Final = list()
            task_list.append(self._get_url(http_session, f"https://esi.evetech.net/latest/characters/{character_id}/", request_params))
            if "esi-characters.read_corporation_roles.v1" in scopes:
                task_list.append(self._get_url(http_session, f"https://esi.evetech.net/latest/characters/{character_id}/roles/", request_params))

            for result in await asyncio.gather(*task_list):
                if result is None:
                    continue
                result: dict = result
                if bool(result.get('roles', False)):
                    character_roles_result |= result
                elif bool(result.get('name', False)):
                    character_result |= result


        conversions: typing.Final = {
            'birthday': lambda x: dateutil.parser.parse(str(x)).replace(tzinfo=datetime.timezone.utc)
        }
        for k, v in conversions.items():
            if k in character_result.keys():
                character_result[k] = v(character_result[k])

        return {
            AppSSO.APP_SESSION_ID: session_id,
            AppSSO.ESI_CHARACTER_ID: character_id,
            AppSSO.ESI_CORPORATION_ID: character_result.get('corporation_id', 0),
            AppSSO.ESI_ALLIANCE_ID: character_result.get('alliance_id', 0),
            AppSSO.ESI_CHARACTER_IS_DIRECTOR_ROLE: bool('Director' in character_roles_result.get('roles', [])),
            AppSSO.ESI_CHARACTER_IS_ACCOUNTANT_ROLE: bool('Accountant' in character_roles_result.get('roles', [])),
            AppSSO.ESI_CHARACTER_IS_STATION_MANAGER_ROLE: bool('Station_Manager' in character_roles_result.get('roles', [])),
            AppSSO.ESI_ACCESS_TOKEN_ISSUED: datetime.datetime.fromtimestamp(decoded_acess_token.get('iat', int(now.timestamp())), tz=datetime.timezone.utc),
            AppSSO.ESI_ACCESS_TOKEN_EXPIRY: datetime.datetime.fromtimestamp(decoded_acess_token.get('exp', int(now.timestamp())), tz=datetime.timezone.utc),
            AppSSO.ESI_REFRESH_TOKEN: refresh_token,
            AppSSO.ESI_ACCESS_TOKEN: access_token,
            AppSSO.ESI_SCOPES: scopes,
            AppSSO.ESI_CHARACTER_NAME: character_result.get('name', None),
            AppSSO.ESI_CHARACTER_BIRTHDAY: character_result.get('birthday', None),
        }

    @otel
    async def esi_sso_login(self, variant: str) -> quart.redirect:
        client_session: typing.Final = quart.session

        client_session[AppSSO.APP_SESSION_ID] = uuid.uuid4().hex

        login_scopes = ['publicData']
        if variant == 'user':
            login_scopes = ['publicData']
        elif variant == 'contributor':
            login_scopes = self.scopes

        if len(login_scopes) == 1 and 'publicData' in login_scopes:
            client_session[AppSSO.APP_SESSION_TYPE] = "USER"
        else:
            client_session[AppSSO.APP_SESSION_TYPE] = "CONTRIBUTOR"

        client_session[AppSSO.ESI_SCOPES] = login_scopes

        redirect_params: typing.Final = [
            'response_type=code',
            f'redirect_uri={self.callback_url}',
            f'client_id={self.client_id}',
            f'scope={urllib.parse.quote(" ".join(login_scopes))}',
            f'state={client_session[AppSSO.APP_SESSION_ID]}'
        ]

        redirect_url: typing.Final = f"{self.configuration['authorization_endpoint']}?{'&'.join(redirect_params)}"

        return quart.redirect(redirect_url)

    @otel
    async def esi_sso_logout(self) -> quart.redirect:
        client_session: typing.Final = quart.session

        character_id: typing.Final = client_session.get(AppSSO.ESI_CHARACTER_ID, 0)
        session_id: typing.Final = client_session.get(AppSSO.APP_SESSION_ID, '')

        if character_id > 0:

            await AppSSOFunctions.remove_credentials(self.db, character_id)

            if self.outbound:
                await self.outbound.put(SSOLogoutEvent(character_id=character_id, session_id=session_id))

            await AppSSOFunctions.authlog(self.db, character_id, session_id, AppAuthType.LOGOUT)

        client_session.clear()

        return quart.redirect("/")

    @otel
    async def esi_sso_callback(self) -> quart.redirect:

        client_session: typing.Final = quart.session

        session_id: typing.Final = client_session.get(AppSSO.APP_SESSION_ID, '')

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

        edict: typing.Final = await self.esi_decode_token(session_id, token_response)
        if edict is None:
            quart.abort(500, "invalid token_response")

        edict: dict
        character_id: typing.Final = edict.get(AppSSO.ESI_CHARACTER_ID, 0)

        await AppSSOFunctions.update_credentials(self.db, character_id, edict)
        await self.esi_update_client_session(client_session, character_id, edict)

        login_type = AppAuthType.LOGIN_USER
        if len(client_session.get(AppSSO.ESI_SCOPES, [])) > 1:
            login_type = AppAuthType.LOGIN_CONTRIBUTOR

        if self.outbound:
            await self.outbound.put(SSOLoginEvent(character_id=character_id, session_id=session_id, login_type=login_type.name))

        await AppSSOFunctions.authlog(self.db, character_id, session_id, login_type)

        return quart.redirect("/")

    @otel
    async def esi_sso_refresh(self, session_id: str, refresh_token: str) -> dict | None:

        token_response = dict()

        post_session_headers: typing.Final = {
            "Host": self.configuration["issuer"],
        }

        async with aiohttp.ClientSession(headers=post_session_headers) as http_session:
            post_token_url = self.configuration['token_endpoint']
            post_body: typing.Final = {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": self.client_id
            }
            token_response = await self._post_url(http_session, post_token_url, post_body)

        return await self.esi_decode_token(session_id, token_response)

    @otel
    async def esi_update_client_session(self, client_session: collections.abc.MutableMapping, character_id: int, edict: dict):

        edict = edict or dict()
        if len(edict) == 0 or not character_id > 0:
            client_session.clear()
            return

        # This presumes the client session keys are the same as the column names in the
        # in the PeriodicCredentials and the same as the dictionary keys.

        client_session_keys = [
            AppSSO.ESI_CHARACTER_ID,
            AppSSO.ESI_CORPORATION_ID,
            AppSSO.ESI_ALLIANCE_ID,
            AppSSO.ESI_CHARACTER_IS_STATION_MANAGER_ROLE,
            AppSSO.ESI_CHARACTER_IS_ACCOUNTANT_ROLE,
            AppSSO.ESI_SCOPES,
            AppSSO.ESI_ACCESS_TOKEN]

        for k in client_session_keys:
            v = edict.get(k)
            if v is not None:
                client_session[k] = v
            else:
                raise Exception(f"{k}")
