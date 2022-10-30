import asyncio
import base64
import collections.abc
import datetime
import inspect
import logging
import urllib.parse
import uuid
import zoneinfo
from typing import Final, Optional

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
from db import EveAccessType, EveAuthType, EveDatabase, EveTables
from telemetry import otel, otel_add_event, otel_add_error, otel_add_exception


class EveSSO:

    CONNFIGURATION_URL: Final = 'https://login.eveonline.com/.well-known/oauth-authorization-server'

    JWT_ISSUERS: Final = ["login.eveonline.com", "https://login.eveonline.com"]
    JWT_AUDIENCE: Final = "EVE Online"

    APP_SESSION_ID: Final = "app_session_id"
    APP_SESSION_SCOPES: Final = "app_session_scopes"
    APP_SESSION_TYPE: Final = "app_session_type"

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

        self.app: Final = app
        self.db: Final = db
        self.logger: Final = app.logger

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
        self.refresh_jwks_task: asyncio.Task = None

        self.refresh_token_task: asyncio.Task = None

        @app.before_serving
        @otel
        async def _esi_sso_setup():
            self.configuration = await self._get_json(self.configuration_url)

            required_configuration_keys = ["token_endpoint", "authorization_endpoint", "issuer", "jwks_uri"]
            if not all(map(lambda x: bool(self.configuration.get(x)), required_configuration_keys)):
                raise Exception(f"{inspect.currentframe().f_code.co_name}: configuration at {self.configuration_url} is invalid")

            self.jwks_uri = self.configuration["jwks_uri"]
            self.jwks = await self._get_jwks(self.jwks_uri)
            self.refresh_jwks_task = asyncio.create_task(self._refresh_jwks_task())

            self.refresh_token_task = asyncio.create_task(self._refresh_token_task())

            app.add_url_rule(self.callback_route, self.callback_endpoint, view_func=self.esi_sso_callback, methods=["GET"])
            app.add_url_rule(self.logout_route, self.logout_endpoint, view_func=self.esi_sso_logout, methods=["GET"])
            app.add_url_rule(self.login_route, self.login_endpoint, view_func=self.esi_sso_login, methods=["GET"], defaults={'variant': 'default'})
            app.add_url_rule(f"{self.login_route}/<string:variant>", self.login_endpoint, view_func=self.esi_sso_login, methods=["GET"])

        @app.after_serving
        @otel
        async def _esi_sso_teardown():
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
        session_headers: Final = dict()
        if len(esi_access_token) > 0:
            session_headers["Authorization"] = f"Bearer {esi_access_token}"

        json = None
        async with aiohttp.ClientSession(headers=session_headers) as http_session:
            async with await http_session.get(url) as response:
                if response.status in [200]:
                    json = dict(await response.json())
                else:
                    otel_add_error(f"{response.url} -> {response.status}")
                    self.logger.warning("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"{response.url} -> {response.status}"))

        return json

    @otel
    async def _get_jwks(self, url: str) -> list[dict]:
        payload: Final = await self._get_json(url)
        return payload.get("keys", [])

    @otel
    async def _refresh_jwks_task(self) -> None:
        while True:
            await asyncio.sleep(300)
            try:
                new_jwks: Final = await self._get_jwks(self.jwks_uri)
                if new_jwks is not None:
                    self.jwks = new_jwks
            except Exception as ex:
                otel_add_exception(ex)
                self.app.logger.error(f"{inspect.currentframe().f_code.co_name}: {ex}")

    @otel
    async def _refresh_token_task(self) -> None:
        refresh_buffer: Final = datetime.timedelta(seconds=15)
        refresh_interval: Final = datetime.timedelta(seconds=60) - refresh_buffer
        while True:
            now: Final = datetime.datetime.now(tz=datetime.timezone.utc)

            refresh_obj: EveTables.PeriodicCredentials = None
            async with await self.db.sessionmaker() as session, session.begin():
                query = sqlalchemy.select(EveTables.PeriodicCredentials).where(EveTables.PeriodicCredentials.is_permitted.is_(True)).order_by(sqlalchemy.asc(EveTables.PeriodicCredentials.access_token_exiry)).limit(1)
                results = await session.execute(query)
                obj_set = {x for x in results.scalars()}
                if len(obj_set) > 0:
                    refresh_obj = obj_set.pop()

            if refresh_obj is None:
                await asyncio.sleep(refresh_interval.total_seconds())
                continue

            if refresh_obj.access_token_exiry - refresh_interval > now:
                remaining_interval: Final = (refresh_obj.access_token_exiry + refresh_interval + refresh_buffer) - now
                await asyncio.sleep(int(min(refresh_interval.total_seconds(), remaining_interval.total_seconds())))
                continue

            refresh_character_id: Final = refresh_obj.character_id

            client_session = dict({
                EveSSO.ESI_ACCESS_TOKEN_ISSUED: int(refresh_obj.access_token_issued.timestamp()),
                EveSSO.ESI_ACCESS_TOKEN_EXPIRY: int(refresh_obj.access_token_exiry.timestamp()),
                EveSSO.ESI_CHARACTER_ID: refresh_obj.character_id,
                EveSSO.ESI_CORPORATION_ID: refresh_obj.corporation_id,
                EveSSO.APP_SESSION_ID: refresh_obj.session_id,
                EveSSO.ESI_REFRESH_TOKEN: refresh_obj.refresh_token,
            })

            async with await self.db.sessionmaker() as session, session.begin():
                query = await session.execute(
                    sqlalchemy.select(EveTables.PeriodicCredentials)
                    .where(EveTables.PeriodicCredentials.character_id == refresh_character_id))
                update_character_obj_set: Final = {x for x in query.scalars()}

                refresh_character_obj: EveTables.PeriodicCredentials = None
                if len(update_character_obj_set) == 1:
                    refresh_character_obj = update_character_obj_set.pop()

                if refresh_character_obj is None:
                    await asyncio.sleep(int(refresh_interval.total_seconds()))
                    continue

                if await self.esi_sso_refresh(client_session):

                    update_dict = {
                        "access_token_exiry": datetime.datetime.fromtimestamp(client_session.get(EveSSO.ESI_ACCESS_TOKEN_EXPIRY, 0), tz=datetime.timezone.utc),
                        "access_token_issued": datetime.datetime.fromtimestamp(client_session.get(EveSSO.ESI_ACCESS_TOKEN_ISSUED, 0), tz=datetime.timezone.utc),
                        "access_token": client_session.get(EveSSO.ESI_ACCESS_TOKEN, ''),
                        "refresh_token": client_session.get(EveSSO.ESI_REFRESH_TOKEN, '')
                    }

                    for k, v in update_dict.items():
                        if hasattr(refresh_character_obj, k):
                            setattr(refresh_character_obj, k, v)

                    self.logger.info("- {}.{}: {} {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  refresh_obj.character_id, "token refreshed"))
                else:
                    otel_add_error("token refreshed failed")
                    self.logger.error("- {}.{}: {} {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  refresh_obj.character_id, "token refreshed failed"))

                    refresh_character_obj.is_enabled = False
                    refresh_character_obj.is_permitted = False
                await session.commit()

            async with await self.db.sessionmaker() as session, session.begin():
                authlog_obj = EveTables.AuthLog(character_id=client_session.get(EveSSO.ESI_CHARACTER_ID, 0), session_id=client_session.get(EveSSO.APP_SESSION_ID, ''), auth_type=EveAuthType.REFRESH)
                session.add(authlog_obj)
                await session.commit()

    @otel
    async def esi_decode_token(self, client_session: collections.abc.MutableMapping, token_response: dict) -> dict:

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
                otel_add_exception(ex)
                self.logger.error(f"{inspect.currentframe().f_code.co_name}: {ex}")

        return decoded_acess_token or dict()

    @otel
    async def esi_sso_login(self, variant: str) -> quart.redirect:
        client_session: Final = quart.session

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

        redirect_params: Final = [
            'response_type=code',
            f'redirect_uri={self.callback_url}',
            f'client_id={self.client_id}',
            f'scope={urllib.parse.quote(" ".join(login_scopes))}',
            f'state={client_session[EveSSO.APP_SESSION_ID]}'
        ]

        redirect_url: Final = f"{self.configuration['authorization_endpoint']}?{'&'.join(redirect_params)}"

        return quart.redirect(redirect_url)

    @otel
    async def esi_sso_logout(self) -> quart.redirect:
        client_session: Final = quart.session

        character_id: Final = client_session.get(EveSSO.ESI_CHARACTER_ID, 0)
        session_id: Final = client_session.get(EveSSO.APP_SESSION_ID, '')

        if character_id > 0 and len(session_id) > 0:
            # Disable periodic on explicit logout
            async with await self.db.sessionmaker() as session, session.begin():
                character_query = sqlalchemy.select(EveTables.PeriodicCredentials).where(
                    EveTables.PeriodicCredentials.character_id == character_id)
                character_query_results = await session.execute(character_query)
                character_set = {x for x in character_query_results.scalars()}
                obj = None
                if len(character_set) > 0:
                    obj: EveTables.PeriodicCredentials = character_set.pop()
                    obj.is_enabled = False
                    obj.is_permitted = False
                    await session.commit()

            async with await self.db.sessionmaker() as session, session.begin():
                authlog_obj = EveTables.AuthLog(character_id=character_id, session_id=session_id, auth_type=EveAuthType.LOGOUT)
                session.add(authlog_obj)
                await session.commit()

        otel_add_event("logout", {"session_id": session_id})

        client_session.clear()

        return quart.redirect("/")

    @otel
    async def esi_sso_callback(self) -> quart.redirect:

        client_session: Final = quart.session

        session_id: Final = client_session.get(EveSSO.APP_SESSION_ID, '')

        required_callback_keys: Final = ["code", "state"]
        if not all(map(lambda x: bool(quart.request.args.get(x)), required_callback_keys)):
            quart.abort(500, f"invalid call to {self.callback_route}")

        if quart.request.args["state"] != session_id:
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
            async with await http_session.post(post_token_url, data=post_body) as response:
                if response.status in [200]:
                    token_response = dict(await response.json())
                else:
                    otel_add_error(f"{response.url} -> {response.status}")
                    self.logger.warning("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"{response.url} -> {response.status}"))

        required_response_keys: Final = ["access_token", "token_type", "refresh_token"]
        if not all(map(lambda x: bool(token_response.get(x)), required_response_keys)):
            quart.abort(500, f"invalid response to {post_token_url}")

        decoded_acess_token: Final = await self.esi_decode_token(client_session, token_response)
        if decoded_acess_token is None:
            quart.abort(500, "invalid jwt in token_response")

        await self.esi_update_client_session(client_session, token_response, decoded_acess_token)

        character_id: Final = client_session.get(EveSSO.ESI_CHARACTER_ID, 0)

        async with await self.db.sessionmaker() as session, session.begin():

            login_auth_type = EveAuthType.LOGIN_USER
            if len(client_session.get(EveSSO.APP_SESSION_SCOPES, [])) > 1:
                login_auth_type = EveAuthType.LOGIN_CONTRIBUTOR

            authlog_obj = EveTables.AuthLog(character_id=character_id, session_id=session_id, auth_type=login_auth_type)
            session.add(authlog_obj)
            await session.commit()

        otel_add_event("login", {"session_id": session_id})

        return quart.redirect("/")

    @otel
    async def esi_sso_refresh(self, client_session: collections.abc.MutableMapping) -> None:

        now: Final = datetime.datetime.now(tz=datetime.timezone.utc)

        if now < datetime.datetime.fromtimestamp(client_session.get(EveSSO.ESI_ACCESS_TOKEN_ISSUED, 0), tz=datetime.timezone.utc):
            return

        if (now + datetime.timedelta(minutes=5)) < datetime.datetime.fromtimestamp(client_session.get(EveSSO.ESI_ACCESS_TOKEN_EXPIRY, 0), tz=datetime.timezone.utc):
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
            async with await http_session.post(post_token_url, data=post_body) as response:
                if response.status in [200]:
                    token_response = dict(await response.json())
                else:
                    otel_add_error(f"{response.url} -> {response.status}")
                    self.logger.warning("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"{response.url} -> {response.status}"))

        required_response_keys: Final = ["access_token", "token_type", "refresh_token"]
        if all(map(lambda x: bool(token_response.get(x)), required_response_keys)):
            decoded_acess_token = await self.esi_decode_token(client_session, token_response)

        if len(decoded_acess_token) > 0:
            await self.esi_update_client_session(client_session, token_response, decoded_acess_token)

        return len(decoded_acess_token) > 0

    @otel
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

        now: Final = datetime.datetime.now(tz=datetime.timezone.utc)

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
        if not corporation_id > 0:
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
                        async with await http_session.get(url, params=request_params) as response:
                            if response.status in [200]:
                                return await response.json()
                            else:
                                attempts_remaining -= 1
                                otel_add_error(f"{response.url} -> {response.status}")
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

                characters_result = None
                characters_roles_result = dict()

                for gather_result in await asyncio.gather(*task_list):
                    result: Final[dict] = gather_result
                    if bool(result.get('roles', False)):
                        characters_roles_result = result
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

                    async with await self.db.sessionmaker() as session, session.begin():
                        character_query = sqlalchemy.select(EveTables.Character).where(
                            EveTables.Character.character_id == character_id)
                        character_query_results = await session.execute(character_query)
                        character_set = {x for x in character_query_results.scalars()}

                        if len(character_set) > 0:
                            [await session.delete(x) for x in character_set]

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

                        obj = EveTables.Character(**edict)
                        if obj is not None:
                            session.add(obj)
                            await session.commit()

                # Setup / Update the periodic credentials record
                async with await self.db.sessionmaker() as session, session.begin():
                    periodic_credentials = {
                        "character_id": character_id,
                        "corporation_id": corporation_id,
                        "session_id": client_session.get(EveSSO.APP_SESSION_ID, ''),
                        "access_token_issued": datetime.datetime.fromtimestamp(client_session.get(EveSSO.ESI_ACCESS_TOKEN_ISSUED, 0), tz=datetime.timezone.utc),
                        "access_token_exiry": datetime.datetime.fromtimestamp(client_session.get(EveSSO.ESI_ACCESS_TOKEN_EXPIRY, 0), tz=datetime.timezone.utc),
                        "access_token": client_session.get(EveSSO.ESI_ACCESS_TOKEN, ''),
                        "refresh_token": client_session.get(EveSSO.ESI_REFRESH_TOKEN, ''),
                        "is_station_manager_role": bool(client_session.get(EveSSO.ESI_CHARACTER_HAS_STATION_MANAGER_ROLE, False)),
                        "is_director_role": bool(client_session.get(EveSSO.ESI_CHARACTER_HAS_DIRECTOR_ROLE, False)),
                        "is_accountant_role": bool(client_session.get(EveSSO.ESI_CHARACTER_HAS_ACCOUNTANT_ROLE, False)),
                        "is_permitted": bool(client_session.get(EveSSO.ESI_CHARACTER_HAS_STATION_MANAGER_ROLE, False)),
                        "is_enabled": bool(client_session.get(EveSSO.ESI_CHARACTER_HAS_STATION_MANAGER_ROLE, False)),
                    }

                    character_query = sqlalchemy.select(EveTables.PeriodicCredentials).where(
                        EveTables.PeriodicCredentials.character_id == character_id)
                    character_query_results = await session.execute(character_query)
                    character_set = {x for x in character_query_results.scalars()}
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
                        session.add(obj)
                        await session.commit()
