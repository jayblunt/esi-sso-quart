import asyncio
import base64
import collections.abc
import dataclasses
import datetime
import http
import inspect
import secrets
import typing
import urllib.parse
import uuid

import aiohttp
import aiohttp.client_exceptions
import dateutil.parser
import hashlib
import jose.exceptions
import jose.jwt
import quart
import sqlalchemy
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql

from support.telemetry import otel, otel_add_exception

from .constants import AppConstants
from .db import AppAuthType, AppDatabase, AppTables
from .esi import AppESI, AppESIResult
from .events import SSOLoginEvent, SSOLogoutEvent, SSOTokenRefreshEvent


@dataclasses.dataclass(frozen=True)
class AppSSORecord:
    session_id: str
    character_id: int
    corporation_id: int
    alliance_id: int
    character_name: str
    character_birthday: datetime.datetime
    is_director_role: bool
    is_accountant_role: bool
    is_station_manager_role: bool
    scopes: list[str]
    access_token: str
    access_token_iat: datetime.datetime
    access_token_exp: datetime.datetime
    refresh_token: str


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

    # @staticmethod
    # async def debuglog(db: AppDatabase, payload: dict[str, typing.Any]) -> bool:
    #     try:
    #         async with await db.sessionmaker() as session:
    #             session.begin()
    #             session.add(AppTables.SSODebugLog(json=payload))
    #             await session.commit()
    #         return True
    #     except Exception as ex:
    #         otel_add_exception(ex)
    #     return False

    @staticmethod
    async def update_credentials(db: AppDatabase, character_id: int, sso_record: AppSSORecord | None) -> bool:
        if not character_id > 0:
            return False

        try:
            async with await db.sessionmaker() as session:
                session: sqlalchemy.ext.asyncio.AsyncSession

                session.begin()

                if sso_record is not None:

                    query = (
                        sqlalchemy.delete(AppTables.Character)
                        .where(AppTables.Character.character_id == character_id)
                    )
                    await session.execute(query)

                    session.add(AppTables.Character(
                        character_id=sso_record.character_id,
                        corporation_id=sso_record.corporation_id,
                        alliance_id=sso_record.alliance_id,
                        birthday=sso_record.character_birthday,
                        name=sso_record.character_name
                    ))

                query = (
                    sqlalchemy.delete(AppTables.PeriodicCredentials)
                    .where(AppTables.PeriodicCredentials.character_id == character_id)
                )
                await session.execute(query)

                if sso_record is not None:

                    previous_is_permitted = sso_record.is_station_manager_role
                    previous_is_enabled = previous_is_permitted

                    obj = AppTables.PeriodicCredentials(
                        character_id=sso_record.character_id,
                        corporation_id=sso_record.corporation_id,
                        is_permitted=previous_is_permitted,
                        is_enabled=previous_is_enabled,
                        is_director_role=sso_record.is_director_role,
                        is_accountant_role=sso_record.is_accountant_role,
                        is_station_manager_role=sso_record.is_station_manager_role,
                        session_id=sso_record.session_id,
                        access_token_issued=sso_record.access_token_iat,
                        access_token_expiry=sso_record.access_token_exp,
                        refresh_token=sso_record.refresh_token,
                        access_token=sso_record.access_token
                    )

                    session.add(obj)

                await session.commit()
            return True
        except Exception as ex:
            otel_add_exception(ex)
        return False


class AppSSO:

    CONNFIGURATION_URL: typing.Final = 'https://login.eveonline.com/.well-known/oauth-authorization-server'

    JWT_AUDIENCE: typing.Final = "EVE Online"

    APP_SESSION_TYPE: typing.Final = "session_type"

    ESI_CHARACTER_ID: typing.Final = "character_id"
    ESI_CORPORATION_ID: typing.Final = "corporation_id"
    ESI_ALLIANCE_ID: typing.Final = "alliance_id"

    ESI_CHARACTER_IS_DIRECTOR_ROLE: typing.Final = "is_director_role"
    ESI_CHARACTER_IS_ACCOUNTANT_ROLE: typing.Final = "is_accountant_role"
    ESI_CHARACTER_IS_STATION_MANAGER_ROLE: typing.Final = "is_station_manager_role"

    ESI_SCOPES: typing.Final = "scopes"
    ESI_ACCESS_TOKEN: typing.Final = "access_token"
    ESI_REFRESH_TOKEN: typing.Final = "refresh_token"

    APP_SESSION_ID: typing.Final = "session_id"
    APP_PCKE_VERIFIER: typing.Final = "pcke_verifier"
    REQUEST_PATH: typing.Final = "request.path"

    @otel
    def __init__(self,
                 app: quart.Quart,
                 esi: AppESI,
                 db: AppDatabase,
                 eventqueue: asyncio.Queue,
                 client_id: str,
                 configuration_url: str | None = None,
                 scopes: list[str] = ['publicData'],
                 login_route: str = '/sso/login',
                 logout_route: str = '/sso/logout',
                 callback_route: str = '/sso/callback') -> None:

        self.app: typing.Final = app
        self.esi: typing.Final = esi
        self.db: typing.Final = db
        self.eventqueue: typing.Final = eventqueue
        self.logger: typing.Final = app.logger

        self.client_id: typing.Final = client_id

        self.configuration_url: typing.Final = configuration_url or self.CONNFIGURATION_URL
        self.scopes: typing.Final = scopes

        self.login_route: typing.Final = login_route
        self.logout_route: typing.Final = logout_route
        self.callback_route: typing.Final = callback_route

        self.request_params: typing.Final = dict()
        self.configuration: dict = dict()
        self.jwks_uri: str
        self.jwks: list[dict] = list()
        self.refresh_jwks_task: asyncio.Task | None = None

        self.refresh_token_task: asyncio.Task | None = None

        @app.before_serving
        @otel
        async def _esi_sso_setup() -> None:
            self.configuration = await self._get_json(self.configuration_url)

            required_configuration_keys = ["token_endpoint", "authorization_endpoint", "issuer", "jwks_uri"]
            if not all(map(lambda x: bool(self.configuration.get(x)), required_configuration_keys)):
                raise Exception(f"{inspect.currentframe().f_code.co_name}: configuration at {self.configuration_url} is invalid")

            self.jwks_uri = self.configuration["jwks_uri"]
            self.jwks = await self._get_jwks(self.jwks_uri)
            self.refresh_jwks_task = asyncio.create_task(self._refresh_jwks_task(), name=self._refresh_jwks_task.__name__)

            self.refresh_token_task = asyncio.create_task(self._refresh_token_task(), name=self._refresh_token_task.__name__)
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
    async def _get_json(self, url: str, esi_access_token: str = '') -> dict:
        session_headers: typing.Final = dict()
        if len(esi_access_token) > 0:
            session_headers["Authorization"] = f"Bearer {esi_access_token}"

        async with aiohttp.ClientSession(headers=session_headers) as http_session:
            esi_result = await self.esi.get(http_session, url, request_params=self.request_params)
            if esi_result.status in [http.HTTPStatus.OK, http.HTTPStatus.NOT_MODIFIED] and esi_result.data is not None:
                return esi_result.data

        return dict()

    @otel
    async def _get_jwks(self, url: str) -> list:
        payload: typing.Final = await self._get_json(url)
        return payload.get("keys", [])

    async def _refresh_jwks_task(self) -> None:
        while True:
            await asyncio.sleep(600)
            try:
                new_jwks: typing.Final = await self._get_jwks(self.jwks_uri)
                if len(new_jwks) > 0:
                    self.jwks = new_jwks
            except Exception as ex:
                otel_add_exception(ex)
                self.app.logger.error(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex=}")
            except asyncio.CancelledError as ex:
                otel_add_exception(ex)
                self.app.logger.error(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex=}")
                break

    async def _refresh_token_task(self) -> None:
        refresh_buffer: typing.Final = datetime.timedelta(seconds=30)
        refresh_interval: typing.Final = datetime.timedelta(seconds=300)
        while True:
            now: typing.Final = datetime.datetime.now(tz=datetime.UTC)

            obj: AppTables.PeriodicCredentials | None = None
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
                    obj = query_results.scalar_one_or_none()

            except Exception as ex:
                otel_add_exception(ex)
                self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex=}")
                await asyncio.sleep(refresh_interval.total_seconds())
                continue

            if obj is None:
                self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: no permitted credentials")
                await asyncio.sleep(refresh_interval.total_seconds())
                continue

            if not isinstance(obj, AppTables.PeriodicCredentials):
                self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: no permitted credentials")
                await asyncio.sleep(refresh_interval.total_seconds())
                continue

            if obj.access_token_expiry > now + refresh_buffer:
                remaining_interval: datetime.timedelta = (obj.access_token_expiry) - (now + refresh_buffer)
                remaining_sleep_interval = min(refresh_interval.total_seconds(), remaining_interval.total_seconds())
                await asyncio.sleep(remaining_sleep_interval)
                continue

            esi_status = await self.esi.status()
            if esi_status is False:
                self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: esi is not really available")
                await asyncio.sleep(refresh_interval.total_seconds())
                continue

            character_id: typing.Final = int(obj.character_id)
            session_id: typing.Final = obj.session_id
            refresh_token: typing.Final = obj.refresh_token

            self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: refresh {obj.character_id=} / {obj.corporation_id=}")
            sso_record = await self.esi_sso_refresh(session_id, refresh_token)
            await AppSSOFunctions.update_credentials(self.db, character_id, sso_record)
            if sso_record:
                await AppSSOFunctions.authlog(self.db, character_id, session_id, AppAuthType.REFRESH)

                # if self.eventqueue:
                #     await self.eventqueue.put(SSOTokenRefreshEvent(character_id=character_id, session_id=session_id))

            else:
                await AppSSOFunctions.authlog(self.db, character_id, session_id, AppAuthType.REFRESH_FAILURE)

    @otel
    async def esi_decode_token(self, session_id: str, token_response: dict) -> dict:

        token_response = token_response or dict()

        required_response_keys: typing.Final = ["access_token", "token_type", "refresh_token"]
        if not all(map(lambda x: bool(token_response.get(x)), required_response_keys)):
            return dict()

        jwt_unverified_header: typing.Final = jose.jwt.get_unverified_header(token_response["access_token"])
        jwt_key = None
        for jwk_candidate in self.jwks:
            if not type(jwk_candidate) is dict:
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
                decoded_acess_token = jose.jwt.decode(token_response["access_token"], key=jwt_key, issuer=self.configuration.get('issuer'), audience=self.JWT_AUDIENCE)
            except jose.exceptions.JWTError as ex:
                otel_add_exception(ex)
                self.logger.error(f"{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {ex=}")
                return dict()

        # if decoded_acess_token is not None:
        #     await AppSSOFunctions.debuglog(self.db, decoded_acess_token)

        return decoded_acess_token

    @otel
    async def esi_unpack_token_response(self, session_id: str, token_response: dict) -> AppSSORecord:
        now: typing.Final = datetime.datetime.now(tz=datetime.UTC)

        decoded_acess_token = await self.esi_decode_token(session_id, token_response)
        if decoded_acess_token is None:
            return None

        character_id: typing.Final = int(decoded_acess_token.get('sub', '0').split(':')[-1])
        if not character_id > 0:
            return None

        access_token: typing.Final = token_response.get('access_token')
        refresh_token: typing.Final = token_response.get('refresh_token')
        if access_token is None or refresh_token is None:
            return None

        scopes = decoded_acess_token.get('scp', [])
        if type(scopes) is str:
            scopes = [scopes]

        character_result = dict()
        character_roles_result = dict()
        session_headers: typing.Final = {
            "Authorization": f"Bearer {access_token}"
        }
        async with aiohttp.ClientSession(headers=session_headers, connector=aiohttp.TCPConnector(limit_per_host=AppConstants.ESI_LIMIT_PER_HOST)) as http_session:
            request_params: typing.Final = {
                "datasource": "tranquility",
                "language": "en"
            }

            task_list: typing.Final = list()
            task_list.append(self.esi.get(http_session, f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/characters/{character_id}/", request_params=self.request_params | request_params))
            if "esi-characters.read_corporation_roles.v1" in scopes:
                task_list.append(self.esi.get(http_session, f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/characters/{character_id}/roles/", request_params=self.request_params | request_params))

            for esi_result in await asyncio.gather(*task_list):
                esi_result: AppESIResult

                if not esi_result:
                    continue

                if esi_result.status not in [http.HTTPStatus.OK, http.HTTPStatus.NOT_MODIFIED] or esi_result.data is None:
                    continue
                if len(esi_result.data) == 0:
                    continue
                if bool(esi_result.data.get('roles', False)):
                    character_roles_result |= esi_result.data
                elif bool(esi_result.data.get('name', False)):
                    character_result |= esi_result.data

        conversions: typing.Final = {
            'birthday': lambda x: dateutil.parser.parse(str(x)).replace(tzinfo=datetime.UTC)
        }
        for k, v in conversions.items():
            if k in character_result.keys():
                character_result[k] = v(character_result[k])

        epoch: typing.Final = datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
        return AppSSORecord(
            session_id=session_id,
            character_id=character_id,
            corporation_id=character_result.get('corporation_id', 0),
            alliance_id=character_result.get('alliance_id', 0),
            character_name=character_result.get('name', None),
            character_birthday=character_result.get('birthday', epoch),
            is_director_role=bool('Director' in character_roles_result.get('roles', [])),
            is_accountant_role=bool('Accountant' in character_roles_result.get('roles', [])),
            is_station_manager_role=bool('Station_Manager' in character_roles_result.get('roles', [])),
            scopes=character_roles_result.get('roles', []),
            access_token=access_token,
            access_token_iat=datetime.datetime.fromtimestamp(decoded_acess_token.get('iat', int(now.timestamp())), tz=datetime.UTC),
            access_token_exp=datetime.datetime.fromtimestamp(decoded_acess_token.get('exp', int(now.timestamp())), tz=datetime.UTC),
            refresh_token=refresh_token
        )

    @otel
    async def esi_sso_login(self, variant: str) -> quart.ResponseReturnValue:

        """
        Handle the login process - the redirect to ESI with our client information
        and the scopes that we want to request.

        :param: variant is the login type. ``contributor`` logins request more scopes
        than regular ``user`` logins.
        """

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

        pcke_verifier: typing.Final = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode().replace("=", "")
        client_session[AppSSO.APP_PCKE_VERIFIER] = pcke_verifier

        sha256: typing.Final = hashlib.sha256()
        sha256.update(pcke_verifier.encode())
        hash_digest: typing.Final = sha256.digest()
        pcke_challenge: typing.Final = base64.urlsafe_b64encode(hash_digest).decode().replace("=", "")

        redirect_params: typing.Final = {
            'response_type': 'code',
            'client_id': self.client_id,
            'redirect_uri': self.callback_url,
            'scope': " ".join(login_scopes),
            'state': client_session[AppSSO.APP_SESSION_ID],
            'code_challenge': pcke_challenge,
            'code_challenge_method': 'S256',
        }

        redirect_url: typing.Final = f"{self.configuration['authorization_endpoint']}?{urllib.parse.urlencode(redirect_params)}"

        return quart.redirect(redirect_url)

    @otel
    async def esi_sso_logout(self) -> quart.ResponseReturnValue:

        """
        Handle the logout process - clear the sesssion cooke, remove any credentials
        we have in the db (for ``contributor`` logins), and redirect back to the main site.
        """

        client_session: typing.Final = quart.session

        character_id: typing.Final = client_session.get(AppSSO.ESI_CHARACTER_ID, 0)
        session_id: typing.Final = client_session.get(AppSSO.APP_SESSION_ID, '')

        if character_id > 0:

            await AppSSOFunctions.update_credentials(self.db, character_id, None)

            await AppSSOFunctions.authlog(self.db, character_id, session_id, AppAuthType.LOGOUT)

            if self.eventqueue:
                await self.eventqueue.put(SSOLogoutEvent(character_id=character_id, session_id=session_id))

        client_session.clear()

        return quart.redirect("/")

    @otel
    async def esi_sso_callback(self) -> quart.ResponseReturnValue:

        """
        Handle the callback from ESI. The route for this has to be the one
        configured on the ESI application page.

        We get an ``Authorization code`` from ESI. We POST to an ESI enpoint with our
        application credentials to get an ``access_token`` and ``refresh_token`` from ESI.
        """

        client_session: typing.Final = quart.session

        session_id: typing.Final = client_session.get(AppSSO.APP_SESSION_ID, quart.request.args["state"])

        required_callback_keys: typing.Final = ["code", "state"]
        if not all(map(lambda x: bool(quart.request.args.get(x)), required_callback_keys)):
            quart.abort(http.HTTPStatus.BAD_REQUEST, f"invalid call to {self.callback_route}")

        if quart.request.args["state"] != session_id:
            quart.abort(http.HTTPStatus.BAD_REQUEST, f"invalid session state in {self.callback_route}")

        post_token_url = self.configuration.get('token_endpoint', '')
        post_session_headers: typing.Final = {
            "Host": urllib.parse.urlparse(post_token_url).netloc,
        }

        token_response = dict()
        async with aiohttp.ClientSession(headers=post_session_headers) as http_session:
            post_body: typing.Final = {
                "grant_type": "authorization_code",
                'client_id': self.client_id,
                'redirect_uri': self.callback_url,
                "code": quart.request.args['code'],
                'code_verifier': client_session[AppSSO.APP_PCKE_VERIFIER]
            }

            post_result: typing.Final = await self.esi.post(http_session, post_token_url, post_body)
            if post_result.status == http.HTTPStatus.OK:
                token_response = post_result.data

        sso_record: typing.Final = await self.esi_unpack_token_response(session_id, token_response)
        if sso_record is None:
            quart.abort(http.HTTPStatus.BAD_GATEWAY, "invalid token_response")

        character_id: typing.Final = sso_record.character_id

        await AppSSOFunctions.update_credentials(self.db, character_id, sso_record)
        await self.esi_update_client_session(client_session, character_id, sso_record)

        login_type = AppAuthType.LOGIN_USER
        if client_session[AppSSO.APP_SESSION_TYPE] == "CONTRIBUTOR":
            login_type = AppAuthType.LOGIN_CONTRIBUTOR

        await AppSSOFunctions.authlog(self.db, character_id, session_id, login_type)

        if self.eventqueue:
            await self.eventqueue.put(SSOLoginEvent(character_id=character_id, session_id=session_id, login_type=login_type.name))

        return quart.redirect(quart.session.get(AppSSO.REQUEST_PATH, "/"))

    @otel
    async def esi_sso_refresh(self, session_id: str, refresh_token: str) -> AppSSORecord:

        """
        Handle the ``access_token`` refresh - used for ``contributor`` logins.
        """

        post_token_url: typing.Final = self.configuration['token_endpoint']

        post_session_headers: typing.Final = {
            "Host": urllib.parse.urlparse(post_token_url).netloc
        }

        token_response = dict()
        async with aiohttp.ClientSession(headers=post_session_headers) as http_session:
            post_body: typing.Final = {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": self.client_id
            }
            post_result: typing.Final = await self.esi.post(http_session, post_token_url, post_body)
            if post_result.status == http.HTTPStatus.OK:
                token_response = post_result.data

        return await self.esi_unpack_token_response(session_id, token_response)

    @otel
    async def esi_update_client_session(self, client_session: collections.abc.MutableMapping, character_id: int, sso_record: AppSSORecord | None):

        if sso_record is None or not character_id > 0:
            client_session.clear()
            return

        # This presumes the client session keys are the same as the column names in the
        # in the PeriodicCredentials and the same as the dictionary keys.

        client_session.update({
            AppSSO.ESI_CHARACTER_ID: sso_record.character_id,
            AppSSO.ESI_CORPORATION_ID: sso_record.corporation_id,
            AppSSO.ESI_ALLIANCE_ID: sso_record.alliance_id,
            AppSSO.ESI_CHARACTER_IS_ACCOUNTANT_ROLE: sso_record.is_accountant_role,
            AppSSO.ESI_CHARACTER_IS_DIRECTOR_ROLE: sso_record.is_director_role,
            AppSSO.ESI_CHARACTER_IS_STATION_MANAGER_ROLE: sso_record.is_station_manager_role,
            AppSSO.ESI_SCOPES: sso_record.scopes,
            AppSSO.ESI_ACCESS_TOKEN: sso_record.access_token,
            AppSSO.ESI_REFRESH_TOKEN: sso_record.refresh_token
        })
