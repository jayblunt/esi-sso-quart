import aiohttp
import asyncio
import logging
import dataclasses
import datetime
import dateutil.parser
import inspect
import sqlalchemy
import sqlalchemy.ext.asyncio
import typing
import http
from .events import SSOLoginEvent, SSOLogoutEvent, SSOTokenRefreshEvent
from .db import AppDatabase, AppAuthType, OAuthType, AppTables
from .constants import AppConstants
from .esi import AppESIResult, AppESI
from .sso import OAuthProvider, OAuthHookProvider, OAuthRecord
from support.telemetry import otel_add_exception, otel


class CCPSSOProvider(OAuthProvider):

    def __init__(self) -> None:
        self._json = dict()

        async def _get_json(url: str) -> dict:
            async with aiohttp.ClientSession() as session:
                result = await session.get(url)
                if result.status == http.HTTPStatus.OK:
                    return await result.json()
            return dict()

        self._json = asyncio.run(_get_json(self.url))

    @property
    def document(self) -> dict:
        return self._json

    @property
    def audience(self) -> str:
        return 'EVE Online'

    @property
    def url(self) -> str | None:
        return 'https://login.eveonline.com/.well-known/oauth-authorization-server'


@dataclasses.dataclass(frozen=True)
class AppESIRecord:
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


class AppSSOHookProvider(OAuthHookProvider):

    esi: AppESI
    db: AppDatabase
    eventqueue: asyncio.Queue
    logger: logging.Logger
    connector: aiohttp.TCPConnector

    def __init__(self,
                 esi: AppESI,
                 db: AppDatabase,
                 eventqueue: asyncio.Queue,
                 logger: logging.Logger, /) -> None:

        self.esi: typing.Final = esi
        self.db: typing.Final = db
        self.eventqueue: typing.Final = eventqueue
        self.logger: typing.Final = logger

    @otel
    async def app_authlog(self, character_id: int, session_id: str, auth_type: AppAuthType) -> bool:
        try:
            async with await self.db.sessionmaker() as session:
                session.begin()
                session.add(AppTables.AuthLog(character_id=character_id, session_id=session_id, auth_type=auth_type))
                await session.commit()
            return True
        except Exception as ex:
            otel_add_exception(ex)
        return False

    @otel
    async def app_update_credentials(self, oauthevent: OAuthType, oauthrecord: OAuthRecord, /) -> bool:
        esi_record: typing.Final = await self.oauth_update_esi_information(oauthevent, oauthrecord)

        if not oauthrecord.character_id > 0:
            return False

        try:
            async with await self.db.sessionmaker() as session:
                session: sqlalchemy.ext.asyncio.AsyncSession

                session.begin()

                if esi_record:
                    history_query = (
                        sqlalchemy.select(AppTables.CharacterHistory)
                        .where(AppTables.CharacterHistory.character_id == oauthrecord.character_id)
                        .order_by(sqlalchemy.desc(AppTables.CharacterHistory.timestamp))
                        .limit(1)
                    )
                    history_result = await session.execute(history_query)
                    history_obj = history_result.scalar_one_or_none()
                    history_changed = False

                    if history_obj is None:
                        history_changed = True
                    elif not all([history_obj.corporation_id == esi_record.corporation_id, history_obj.alliance_id == esi_record.alliance_id, history_obj.name == esi_record.character_name]):
                        history_changed = True

                    if history_changed:
                        session.add(AppTables.CharacterHistory(
                            character_id=esi_record.character_id,
                            corporation_id=esi_record.corporation_id,
                            alliance_id=esi_record.alliance_id,
                            birthday=esi_record.character_birthday,
                            name=esi_record.character_name
                        ))

                if esi_record:
                    character_query = (
                        sqlalchemy.select(AppTables.Character)
                        .where(AppTables.Character.character_id == oauthrecord.character_id)
                    )
                    character_result = await session.execute(character_query)
                    character_obj = character_result.scalar_one_or_none()
                    if character_obj:
                        character_obj.character_id = esi_record.character_id
                        character_obj.corporation_id = esi_record.corporation_id
                        character_obj.alliance_id = esi_record.alliance_id
                        character_obj.name = esi_record.character_name
                        character_obj.birthday = esi_record.character_birthday
                    else:
                        character_obj = AppTables.Character(
                            character_id=esi_record.character_id,
                            corporation_id=esi_record.corporation_id,
                            alliance_id=esi_record.alliance_id,
                            birthday=esi_record.character_birthday,
                            name=esi_record.character_name)
                    session.add(character_obj)

                if esi_record:
                    credentials_query = (
                        sqlalchemy.select(AppTables.PeriodicCredentials)
                        .where(AppTables.PeriodicCredentials.character_id == oauthrecord.character_id)
                    )
                    credentials_result = await session.execute(credentials_query)
                    credentials_obj = credentials_result.scalar_one_or_none()
                    if credentials_obj:
                        credentials_obj.corporation_id = esi_record.corporation_id
                        credentials_obj.is_permitted = esi_record.is_station_manager_role
                        credentials_obj.is_enabled = esi_record.is_station_manager_role
                        credentials_obj.is_director_role = esi_record.is_director_role
                        credentials_obj.is_accountant_role = esi_record.is_accountant_role
                        credentials_obj.is_station_manager_role = esi_record.is_station_manager_role

                        credentials_obj.access_token_issued = oauthrecord.access_token_iat
                        credentials_obj.access_token_expiry = oauthrecord.access_token_exp
                        credentials_obj.access_token = oauthrecord.access_token
                        credentials_obj.refresh_token = oauthrecord.refresh_token
                    else:
                        credentials_obj = AppTables.PeriodicCredentials(
                            session_id=esi_record.session_id,
                            character_id=esi_record.character_id,
                            corporation_id=esi_record.corporation_id,
                            is_permitted=esi_record.is_station_manager_role,
                            is_enabled=esi_record.is_station_manager_role,
                            is_director_role=esi_record.is_director_role,
                            is_accountant_role=esi_record.is_accountant_role,
                            is_station_manager_role=esi_record.is_station_manager_role,

                            access_token_issued=oauthrecord.access_token_iat,
                            access_token_expiry=oauthrecord.access_token_exp,
                            access_token=oauthrecord.access_token,
                            refresh_token=oauthrecord.refresh_token
                        )
                    session.add(credentials_obj)
                else:
                    credentials_query = (
                        sqlalchemy.update(AppTables.PeriodicCredentials)
                        .where(AppTables.PeriodicCredentials.character_id == oauthrecord.character_id)
                        .values(is_permitted=False, is_enabled=False)
                    )
                    credentials_result = await session.execute(credentials_query)

                await session.commit()
            return True
        except Exception as ex:
            otel_add_exception(ex)
        return False

    @otel
    async def app_put_contributor_record(self, oauth: OAuthRecord) -> bool:

        try:
            async with await self.db.sessionmaker() as session:
                session: sqlalchemy.ext.asyncio.AsyncSession

                query = (
                    sqlalchemy.select(AppTables.PeriodicCredentials)
                    .where(sqlalchemy.and_(
                        AppTables.PeriodicCredentials.character_id == oauth.character_id,
                        AppTables.PeriodicCredentials.session_id == oauth.session_id
                    ))
                )
                query_results: sqlalchemy.engine.Result = await session.execute(query)
                obj: AppTables.PeriodicCredentials = query_results.scalar_one_or_none()

                if obj:
                    obj.access_token_issued = oauth.access_token_iat
                    obj.access_token_expiry = oauth.access_token_exp
                    obj.refresh_token = oauth.refresh_token
                    obj.access_token = oauth.access_token
                    obj.is_enabled = all([obj.is_enabled, oauth.session_active])

                    session.begin()
                    session.add(obj)
                    await session.commit()

                    self.logger.info(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: updating contributor {obj=}")

        except Exception as ex:
            otel_add_exception(ex)

    @otel
    async def oauth_update_esi_information(self, oauthevent: OAuthType, oauthrecord: OAuthRecord, /) -> AppESIRecord:

        if OAuthType.LOGOUT == oauthevent:
            return None

        character_result = dict()
        character_roles_result = dict()
        session_headers: typing.Final = {"Authorization": f"Bearer {oauthrecord.access_token}"}
        async with aiohttp.ClientSession(headers=session_headers, connector=aiohttp.TCPConnector(limit_per_host=AppConstants.ESI_LIMIT_PER_HOST)) as http_session:
            request_params: typing.Final = {
                "datasource": "tranquility",
                "language": "en"
            }

            task_list: typing.Final = list()
            if oauthrecord.session_scopes.find("esi-characters.read_corporation_roles.v1") >= 0:
                task_list.append(self.esi.get(http_session, f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/characters/{oauthrecord.character_id}/roles/", request_params=request_params))
            task_list.append(self.esi.get(http_session, f"{AppConstants.ESI_API_ROOT}{AppConstants.ESI_API_VERSION}/characters/{oauthrecord.character_id}/", request_params=request_params))

            for esi_result in await asyncio.gather(*task_list):
                esi_result: AppESIResult

                if esi_result is None:
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
        return AppESIRecord(
            session_id=oauthrecord.session_id,
            character_id=oauthrecord.character_id,
            corporation_id=character_result.get('corporation_id', 0),
            alliance_id=character_result.get('alliance_id', 0),
            character_name=character_result.get('name', None),
            character_birthday=character_result.get('birthday', epoch),
            is_director_role=bool('Director' in character_roles_result.get('roles', [])),
            is_accountant_role=bool('Accountant' in character_roles_result.get('roles', [])),
            is_station_manager_role=bool('Station_Manager' in character_roles_result.get('roles', [])),
            scopes=character_roles_result.get('roles', []),
            access_token=oauthrecord.access_token,
            access_token_iat=oauthrecord.access_token_iat,
            access_token_exp=oauthrecord.access_token_exp,
            refresh_token=oauthrecord.refresh_token
        )

    @otel
    async def on_refresh(self, oauthevent: OAuthType, oauthrecord: OAuthRecord, /) -> None:
        await self.app_put_contributor_record(oauthrecord)
        if OAuthType.REFRESH_FAILURE == oauthevent:
            await self.app_authlog(oauthrecord.character_id, oauthrecord.session_id, AppAuthType.REFRESH_FAILURE)
        elif OAuthType.REFRESH == oauthevent:
            await self.app_update_credentials(oauthevent, oauthrecord)
            await self.app_authlog(oauthrecord.character_id, oauthrecord.session_id, AppAuthType.REFRESH)
        if self.eventqueue:
            await self.eventqueue.put(SSOTokenRefreshEvent(character_id=oauthrecord.character_id, session_id=oauthrecord.session_id))
        pass

    @otel
    async def on_login(self, oauthevent: OAuthType, oauthrecord: OAuthRecord, /) -> None:
        tasks: typing.Final = list()

        self.logger.info(f'{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: --- FIVE --- ONE ---')

        tasks.append(self.app_update_credentials(oauthevent, oauthrecord))

        app_login_type = AppAuthType.LOGIN_USER
        if OAuthType.LOGIN_CONTRIBUTOR == oauthevent:
            app_login_type = AppAuthType.LOGIN_CONTRIBUTOR
        tasks.append(self.app_authlog(oauthrecord.character_id, oauthrecord.session_id, app_login_type))

        if len(tasks) > 0:
            await asyncio.gather(*tasks)
        self.logger.info(f'{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: --- FIVE --- TWO ---')
        if self.eventqueue:
            await self.eventqueue.put(SSOLoginEvent(character_id=oauthrecord.character_id, session_id=oauthrecord.session_id, login_type=app_login_type.name))
        pass

    @otel
    async def on_logout(self, oauthevent: OAuthType, oauthrecord: OAuthRecord, /) -> None:
        await self.app_put_contributor_record(oauthrecord)
        await self.app_update_credentials(oauthevent, oauthrecord)
        await self.app_authlog(oauthrecord.character_id, oauthrecord.session_id, AppAuthType.LOGOUT)
        if self.eventqueue:
            await self.eventqueue.put(SSOLogoutEvent(character_id=oauthrecord.character_id, session_id=oauthrecord.session_id))
        pass

    @otel
    async def on_unknown(self, oauthrecord: OAuthRecord, /) -> None:
        pass

    @otel
    async def on_event(self, oauthevent: OAuthType, oauthrecord: OAuthRecord, /) -> None:
        # self.logger.warning(f'{self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {oauthevent=}, {oauthrecord=}')
        dispatch_dict = {
            OAuthType.REFRESH: self.on_refresh,
            OAuthType.REFRESH_FAILURE: self.on_refresh,
            OAuthType.LOGIN: self.on_login,
            OAuthType.LOGOUT: self.on_logout,
            OAuthType.LOGIN_CONTRIBUTOR: self.on_login,
            OAuthType.LOGIN_USER: self.on_login,
        }
        await dispatch_dict.get(oauthevent, self.on_unknown)(oauthevent, oauthrecord)
