import asyncio
import base64
import dataclasses
import datetime
import hashlib
import http
import inspect
import logging
import secrets
import typing
import urllib.parse
import uuid

import aiohttp
import aiohttp.client_exceptions
import jose.exceptions
import jose.jwt
import quart
import sqlalchemy
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql

from support.telemetry import otel, otel_add_exception

from .constants import AppSessionKeys
from .db import AppDatabase, AppTables, OAuthType
from .esi import AppESI, AppESIResult


class OAuthProvider:

    @property
    def url(self) -> str:
        return None

    @property
    def document(self) -> dict:
        return dict()

    @property
    def authorization_endpoint(self) -> str:
        return self.document.get('authorization_endpoint')

    @property
    def token_endpoint(self) -> str:
        return self.document.get('token_endpoint')

    @property
    def revocation_endpoint(self) -> str:
        return self.document.get('revocation_endpoint')

    @property
    def audience(self) -> str:
        return ''

    @otel
    async def decode_access_token(self, access_token: str, jwks: typing.Optional[list[str]] = None) -> dict:
        if jwks is None:
            return None

        if not len(jwks) > 0:
            return None

        jwt_unverified_header: typing.Final = await asyncio.to_thread(jose.jwt.get_unverified_header, access_token)
        if not len(jwt_unverified_header) > 0:
            return None

        jwt_key = None
        for jwk_candidate in jwks:
            if not isinstance(jwk_candidate, dict):
                continue

            jwt_key_match = True
            for header_key in set(jwt_unverified_header.keys()).intersection({"kid", "alg"}):
                if jwt_unverified_header.get(header_key) != jwk_candidate.get(header_key):
                    jwt_key_match = False
                    break

            if jwt_key_match:
                jwt_key = jwk_candidate
                break

        decoded_token = None
        if jwt_key:
            try:
                decoded_token = await asyncio.to_thread(jose.jwt.decode, access_token, key=jwt_key, issuer=self.document.get('issuer'), audience=self.audience)
            except jose.exceptions.JWTError as ex:
                pass

        return decoded_token


class OAuthPCKE:

    challenge: str
    verifier: str
    method: str = 'S256'

    def __init__(self, challenge: typing.Optional[str] = None):
        self.challenge = challenge
        if challenge is None:
            sha256: typing.Final = hashlib.sha256()
            self.verifier = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode().replace("=", "")
            sha256.update(self.verifier.encode())
            self.challenge = base64.urlsafe_b64encode(sha256.digest()).decode().replace("=", "")


@dataclasses.dataclass(frozen=True)
class OAuthRecord:
    owner: str
    character_id: int
    session_active: bool
    session_id: str
    session_scopes: str
    refresh_token: str
    refresh_timestamp: datetime.datetime
    access_token_iat: datetime.datetime
    access_token_exp: datetime.datetime
    access_token: str


class OAuthHookProvider:

    async def on_event(self, oauthevent: OAuthType, oauthrecord: OAuthRecord, /) -> None:
        pass


class OAuthStorageProvider:

    db: AppDatabase
    eventqueue: asyncio.Queue
    logger: logging.Logger

    def __init__(self,
                 db: AppDatabase,
                 eventqueue: asyncio.Queue,
                 logger: logging.Logger) -> None:

        self.db: typing.Final = db
        self.eventqueue: typing.Final = eventqueue
        self.logger: typing.Final = logger

    @otel
    async def put_ssolog(self, owner: str, character_id: int, session_id: str, auth_type: OAuthType) -> bool:
        try:
            async with await self.db.sessionmaker() as session, session.begin():
                session: sqlalchemy.ext.asyncio.AsyncSession
                session.add(AppTables.OAuthLog(
                    owner=owner,
                    character_id=character_id,
                    session_id=session_id,
                    auth_type=auth_type
                ))
                await session.commit()
        except Exception as ex:
            otel_add_exception(ex)
            return False
        else:
            return True

    @otel
    async def put_ssorecord(self, oauth: OAuthRecord) -> bool:

        try:
            async with await self.db.sessionmaker() as session, session.begin():
                session: sqlalchemy.ext.asyncio.AsyncSession
                query = (
                    sqlalchemy.select(AppTables.OAuthSession)
                    .where(sqlalchemy.and_(
                        AppTables.OAuthSession.owner == oauth.owner,
                        AppTables.OAuthSession.character_id == oauth.character_id
                    ))
                )
                query_result = await session.execute(query)
                query_obj = query_result.scalar_one_or_none()
                if query_obj:
                    await session.delete(query_obj)
                session.add(AppTables.OAuthSession(
                    owner=oauth.owner,
                    character_id=oauth.character_id,
                    session_active=oauth.session_active,
                    session_id=oauth.session_id,
                    session_scopes=oauth.session_scopes,
                    refresh_token=oauth.refresh_token,
                    refresh_timestamp=oauth.refresh_timestamp,
                    access_token_iat=oauth.access_token_iat,
                    access_token_exp=oauth.access_token_exp,
                    access_token=oauth.access_token
                ))

                # XXX TEMP HACK
                query = (
                    sqlalchemy.delete(AppTables.OAuthSession)
                    .where(sqlalchemy.and_(
                        AppTables.OAuthSession.owner == str(oauth.character_id),
                        AppTables.OAuthSession.character_id == oauth.character_id
                    ))
                )
                query_result = await session.execute(query)

                await session.commit()
        except Exception as ex:
            otel_add_exception(ex)
            return False
        else:
            return True

    @otel
    async def first_ssorecord(self) -> OAuthRecord:
        try:
            async with await self.db.sessionmaker() as session, session.begin():
                session: sqlalchemy.ext.asyncio.AsyncSession
                query = (
                    sqlalchemy.select(AppTables.OAuthSession.session_id)
                    .where(sqlalchemy.and_(
                        AppTables.OAuthSession.session_active == sqlalchemy.sql.expression.true(),
                        AppTables.OAuthSession.refresh_timestamp.isnot(None)
                    ))
                    .order_by(sqlalchemy.asc(AppTables.OAuthSession.refresh_timestamp))
                    .limit(1)
                )
                query_result = await session.execute(query)
                query_session_id = query_result.scalar_one_or_none()
                if query_session_id:
                    return await self.get_authrecord(query_session_id)
        except Exception as ex:
            otel_add_exception(ex)

        return None

    @otel
    async def get_authrecord(self, session_id: str) -> OAuthRecord:
        try:
            async with await self.db.sessionmaker() as session, session.begin():
                session: sqlalchemy.ext.asyncio.AsyncSession
                query = (
                    sqlalchemy.select(AppTables.OAuthSession)
                    .where(AppTables.OAuthSession.session_id == session_id)
                )
                query_result = await session.execute(query)
                query_obj = query_result.scalar_one_or_none()
                if query_obj:
                    return OAuthRecord(
                        owner=query_obj.owner,
                        character_id=query_obj.character_id,
                        session_active=query_obj.session_active,
                        session_id=query_obj.session_id,
                        session_scopes=query_obj.session_scopes,
                        refresh_token=query_obj.refresh_token,
                        refresh_timestamp=query_obj.access_token_exp,
                        access_token_iat=query_obj.access_token_iat,
                        access_token_exp=query_obj.access_token_exp,
                        access_token=query_obj.access_token
                    )
        except Exception as ex:
            otel_add_exception(ex)

        return None


class AppSSO:

    app: quart.Quart
    esi: AppESI
    db: AppDatabase
    eventqueue: asyncio.Queue
    client_id: str
    provider: OAuthProvider
    hook_provider: OAuthHookProvider
    storage: OAuthStorageProvider
    client_scopes: list[str]
    login_route: str
    logout_route: str
    callback_route: str

    @otel
    def __init__(self,
                 app: quart.Quart,
                 esi: AppESI,
                 db: AppDatabase,
                 eventqueue: asyncio.Queue,
                 client_id: str,
                 client_scopes: list[str] = ['publicData'],
                 provider: OAuthProvider | None = None,
                 hook_provider: OAuthHookProvider | None = None,
                 login_route: str = '/sso/login',
                 logout_route: str = '/sso/logout',
                 callback_route: str = '/sso/callback') -> None:

        self.app: typing.Final = app
        self.esi: typing.Final = esi
        self.db: typing.Final = db
        self.eventqueue: typing.Final = eventqueue
        self.logger: typing.Final = app.logger

        self.client_id: typing.Final = client_id
        self.client_scopes: typing.Final = client_scopes

        self.provider: typing.Final = provider or OAuthProvider()
        self.hook_provider: typing.Final = hook_provider or OAuthHookProvider()
        self.storage: typing.Final = OAuthStorageProvider(self.db, self.eventqueue, self.logger)

        self.login_route: typing.Final = login_route
        self.logout_route: typing.Final = logout_route
        self.callback_route: typing.Final = callback_route

        self.jwks_uri: str | None = None
        self.jwks: list[dict] = list()
        self.refresh_jwks_task: asyncio.Task | None = None

        self.refresh_token_task: asyncio.Task | None = None

        @app.before_serving
        @otel
        async def _esi_sso_setup() -> None:

            if not all([self.provider.authorization_endpoint, self.provider.token_endpoint]):
                raise Exception(f"{inspect.currentframe().f_code.co_name}: invalid provider {self.provider=}")

            self.jwks_uri = self.provider.document.get('jwks_uri')
            if self.jwks_uri:
                self.jwks = await self._get_jwks(self.jwks_uri)
                self.refresh_jwks_task = asyncio.create_task(self._refresh_jwks_task(), name=self._refresh_jwks_task.__name__)

            self.refresh_token_task = asyncio.create_task(self._refresh_token_task(), name=self._refresh_token_task.__name__)

            app.add_url_rule(self.callback_route, self.callback_endpoint, view_func=self.oauth_callback, methods=["GET"])
            app.add_url_rule(self.logout_route, self.logout_endpoint, view_func=self.oauth_logout, methods=["GET"])
            app.add_url_rule(self.login_route, self.login_endpoint, view_func=self.oauth_login, methods=["GET"], defaults={'variant': 'user'})
            app.add_url_rule(f"{self.login_route}/<string:variant>", self.login_endpoint, view_func=self.oauth_login, methods=["GET"])

        @app.after_serving
        @otel
        async def _esi_sso_teardown() -> None:
            for task in filter(None, [self.refresh_jwks_task, self.refresh_token_task]):
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
            session_headers.update({"Authorization": f"Bearer {esi_access_token}"})

        async with aiohttp.ClientSession(headers=session_headers) as http_session:
            esi_result = await self.esi.get(http_session, url)
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
        token_refresh_buffer: typing.Final = datetime.timedelta(seconds=30)
        token_refresh_interval: typing.Final = datetime.timedelta(seconds=300)
        while True:
            now: typing.Final = datetime.datetime.now(tz=datetime.UTC)

            oauthrecord = await self.storage.first_ssorecord()
            if oauthrecord is None:
                self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: no active credentials")
                await asyncio.sleep(token_refresh_interval.total_seconds())
                continue

            if oauthrecord.access_token_exp > (now + token_refresh_buffer):
                remaining_interval: datetime.timedelta = (oauthrecord.access_token_exp) - (now + token_refresh_buffer)
                remaining_sleep_interval = min(token_refresh_interval.total_seconds(), remaining_interval.total_seconds())
                self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {oauthrecord.character_id} refresh in {int(remaining_interval.total_seconds())}, sleeping {int(remaining_sleep_interval)}")
                await asyncio.sleep(remaining_sleep_interval)
                continue

            esi_status = await self.esi.status()
            if esi_status is False:
                self.logger.warning(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: esi is not really available")
                await asyncio.sleep(token_refresh_interval.total_seconds())
                continue

            self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: refresh {oauthrecord.character_id=}")
            refresh_result: typing.Final = await self.oauth_refresh(oauthrecord.session_id, oauthrecord.refresh_token)

            updated_authrecord = None
            updated_authevent = None
            if refresh_result.status == http.HTTPStatus.OK:
                updated_authevent = OAuthType.REFRESH
                updated_authrecord = await self.oauth_decode_token_response(oauthrecord.session_id, refresh_result.data)

            # 4xx: fail
            elif refresh_result.status in [http.HTTPStatus.UNAUTHORIZED, http.HTTPStatus.FORBIDDEN]:
                updated_authevent = OAuthType.REFRESH_FAILURE
                self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: refresh {oauthrecord.character_id=}, {refresh_result=}")
                updated_authrecord = dataclasses.replace(oauthrecord, session_active=False)

            # Not a 4xx: schedule re-try
            else:
                next_refresh_time = now + token_refresh_interval
                updated_authrecord = dataclasses.replace(oauthrecord, refresh_timestamp=next_refresh_time)
                self.logger.warning(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: refresh {oauthrecord.character_id=}, {refresh_result=}, {updated_authrecord.refresh_timestamp=}")

            tasks = [self.storage.put_ssorecord(updated_authrecord)]
            if updated_authevent:
                tasks.append(self.storage.put_ssolog(updated_authrecord.owner, updated_authrecord.character_id, updated_authrecord.session_id, updated_authevent))
            await asyncio.gather(*tasks)

            if updated_authevent:
                await self.hook_provider.on_event(updated_authevent, updated_authrecord)

    @otel
    async def oauth_decode_token_response(self, session_id: str, token_response: dict) -> OAuthRecord:
        token_response = token_response or dict()

        now: typing.Final = datetime.datetime.now(tz=datetime.UTC)

        required_response_keys: typing.Final = ["access_token", "token_type", "refresh_token"]
        if not all(map(lambda x: bool(token_response.get(x)), required_response_keys)):
            return None

        access_token: typing.Final = token_response.get('access_token')
        refresh_token: typing.Final = token_response.get('refresh_token')

        decoded_acess_token = await self.provider.decode_access_token(access_token, self.jwks)
        if decoded_acess_token is None:
            decoded_acess_token = dict()

        access_token_iat: typing.Final = datetime.datetime.fromtimestamp(decoded_acess_token.get('iat', int(now.timestamp())), tz=datetime.UTC)
        access_token_exp: typing.Final = datetime.datetime.fromtimestamp(decoded_acess_token.get('exp', int(now.timestamp())), tz=datetime.UTC)
        if not all([access_token, refresh_token, access_token_iat, access_token_exp]):
            return None

        session_scopes = decoded_acess_token.get('scp', decoded_acess_token.get('scope', ''))
        if isinstance(session_scopes, list):
            session_scopes = " ".join(map(lambda x: str(x).strip(), session_scopes))
        session_scopes = session_scopes.strip()

        owner = decoded_acess_token.get('owner', '')
        if isinstance(owner, bytes):
            owner = owner.decode()

        character_id: typing.Final = int(decoded_acess_token.get('sub', '0').split(':')[-1])
        if not character_id > 0:
            return None

        return OAuthRecord(
            owner=owner,
            character_id=character_id,
            session_active=True,
            session_id=session_id,
            session_scopes=session_scopes,
            refresh_token=refresh_token,
            refresh_timestamp=access_token_exp,
            access_token_iat=access_token_iat,
            access_token_exp=access_token_exp,
            access_token=access_token)

    @otel
    async def oauth_login(self, variant: str) -> quart.ResponseReturnValue:

        """
        Handle the login process - the redirect to ESI with our client information
        and the scopes that we want to request.

        :param: variant is the login type. ``contributor`` logins request more scopes
        than regular ``user`` logins.
        """

        client_session: typing.Final = quart.session

        client_session[AppSessionKeys.KEY_APP_SESSION_ID] = uuid.uuid4().hex

        login_scopes = ['publicData']
        if variant == 'user':
            login_scopes = ['publicData']
        elif variant == 'contributor':
            login_scopes = self.client_scopes

        if len(login_scopes) == 1 and 'publicData' in login_scopes:
            client_session[AppSessionKeys.KEY_APP_SESSION_TYPE] = "USER"
        else:
            client_session[AppSessionKeys.KEY_APP_SESSION_TYPE] = "CONTRIBUTOR"

        pcke: typing.Final = OAuthPCKE()
        client_session[AppSessionKeys.KEY_OAUTH_PCKE_VERIFIER] = pcke.verifier

        redirect_params: typing.Final = {
            'response_type': 'code',
            'client_id': self.client_id,
            'redirect_uri': self.callback_url,
            'scope': " ".join(login_scopes),
            'state': client_session[AppSessionKeys.KEY_APP_SESSION_ID],
            'code_challenge': pcke.challenge,
            'code_challenge_method': pcke.method
        }

        # redirect_url: typing.Final = f"{self.provider.authorization_endpoint}?{urllib.parse.urlencode(redirect_params)}"
        redirect_url = urllib.parse.urlparse(self.provider.authorization_endpoint)._replace(query=urllib.parse.urlencode(redirect_params)).geturl()

        return quart.redirect(redirect_url)

    @otel
    async def oauth_logout(self) -> quart.ResponseReturnValue:

        """
        Handle the logout process - clear the sesssion cooke, remove any credentials
        we have in the db (for ``contributor`` logins), and redirect back to the main site.
        """

        client_session: typing.Final = quart.session

        session_id: typing.Final = client_session.get(AppSessionKeys.KEY_APP_SESSION_ID, '')

        oauthrecord = await self.storage.get_authrecord(session_id)
        if oauthrecord:
            oauthrecord = dataclasses.replace(oauthrecord, session_active=False)
            await asyncio.gather(
                self.storage.put_ssolog(oauthrecord.owner, oauthrecord.character_id, oauthrecord.session_id, OAuthType.LOGOUT),
                self.storage.put_ssorecord(oauthrecord)
            )
            await self.hook_provider.on_event(OAuthType.LOGOUT, oauthrecord)

        client_session.clear()

        return quart.redirect(quart.session.get(AppSessionKeys.KEY_APP_REQUEST_PATH, "/"))

    @otel
    async def oauth_callback(self) -> quart.ResponseReturnValue:

        """
        Handle the callback from ESI. The route for this has to be the one
        configured on the ESI application page.

        We get an ``Authorization code`` from ESI. We POST to an ESI enpoint with our
        application credentials to get an ``access_token`` and ``refresh_token`` from ESI.
        """

        client_session: typing.Final = quart.session

        session_id: typing.Final = client_session.get(AppSessionKeys.KEY_APP_SESSION_ID, quart.request.args["state"])

        required_callback_keys: typing.Final = ["code", "state"]
        if not all(map(lambda x: bool(quart.request.args.get(x)), required_callback_keys)):
            quart.abort(http.HTTPStatus.BAD_REQUEST, f"invalid call to {self.callback_route}")

        if quart.request.args["state"] != session_id:
            quart.abort(http.HTTPStatus.BAD_REQUEST, f"invalid session state in {self.callback_route}")

        pcke_verifier: typing.Final = client_session.get(AppSessionKeys.KEY_OAUTH_PCKE_VERIFIER)
        if pcke_verifier is None:
            quart.abort(http.HTTPStatus.BAD_REQUEST, f"invalid session state in {self.callback_route}")
        del client_session[AppSessionKeys.KEY_OAUTH_PCKE_VERIFIER]

        async with aiohttp.ClientSession() as http_session:
            post_request_headers: typing.Final = {"Host": urllib.parse.urlparse(self.provider.token_endpoint).netloc}
            post_body: typing.Final = {
                "grant_type": "authorization_code",
                'client_id': self.client_id,
                'redirect_uri': self.callback_url,
                "code": quart.request.args['code'],
                'code_verifier': pcke_verifier
            }

            token_result = await self.esi.post(http_session, self.provider.token_endpoint, post_body, request_headers=post_request_headers)

        if token_result is None:
            return http.HTTPStatus.SERVICE_UNAVAILABLE
        elif token_result.status != http.HTTPStatus.OK:
            return token_result.status

        oauthrecord = await self.oauth_decode_token_response(session_id, token_result.data)
        if oauthrecord is None:
            quart.abort(http.HTTPStatus.BAD_GATEWAY, "invalid token_response")

        authevent = OAuthType.LOGIN_USER
        if client_session[AppSessionKeys.KEY_APP_SESSION_TYPE] == "CONTRIBUTOR":
            authevent = OAuthType.LOGIN_CONTRIBUTOR

        client_session[AppSessionKeys.KEY_ESI_CHARACTER_ID] = oauthrecord.character_id
        client_session[AppSessionKeys.KEY_APP_SESSION_ID] = oauthrecord.session_id

        await asyncio.gather(
            self.storage.put_ssolog(oauthrecord.owner, oauthrecord.character_id, oauthrecord.session_id, authevent),
            self.storage.put_ssorecord(oauthrecord)
        )

        self.logger.info(f'{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: --- FIVE ---')

        await self.hook_provider.on_event(authevent, oauthrecord)

        self.logger.info(f'{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: --- SIX ---')

        return quart.redirect(quart.session.get(AppSessionKeys.KEY_APP_REQUEST_PATH, "/"))

    @otel
    async def oauth_refresh(self, session_id: str, refresh_token: str) -> AppESIResult:

        async with aiohttp.ClientSession() as http_session:
            post_request_headers: typing.Final = {"Host": urllib.parse.urlparse(self.provider.token_endpoint).netloc}
            post_body: typing.Final = {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": self.client_id
            }
            return await self.esi.post(http_session, self.provider.token_endpoint, post_body, request_headers=post_request_headers)

        return None
