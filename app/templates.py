import datetime
import enum
import typing

import quart
import quart.sessions

from support.telemetry import otel

from .db import AppDatabase
from .functions import AppFunctions
from .sso import AppSSO


class AppTemplateCsacheEnum(enum.Enum):

    CHARACTER_NAME = 0
    CORPORATION_NAME = 1
    SYSTEM_NAME = 2
    MOON_NAME = 3
    TYPE_NAME = 4


class AppTemplates:

    EVEDB: AppDatabase | None = None
    CACHE: typing.Final = {
        AppTemplateCsacheEnum.CHARACTER_NAME: dict(),
        AppTemplateCsacheEnum.CORPORATION_NAME: dict(),
        AppTemplateCsacheEnum.MOON_NAME: dict(),
        AppTemplateCsacheEnum.SYSTEM_NAME: dict(),
        AppTemplateCsacheEnum.TYPE_NAME: dict(),
    }

    @staticmethod
    @otel
    def _login_type(input: str) -> str:
        client_session: typing.Final = quart.session
        login_type = client_session.get(AppSSO.APP_SESSION_TYPE, "USER")
        if login_type == "CONTRIBUTOR":
            return "contributor"
        else:
            return "user"

    @staticmethod
    @otel
    async def _meta_name(input: str, name_type: AppTemplateCsacheEnum, lookup_function: typing.Callable) -> str:
        name = AppTemplates.CACHE[name_type].get(input)
        if not name:
            name = await lookup_function(AppTemplates.EVEDB, int(input))
            if name:
                AppTemplates.CACHE[name_type][input] = name
        return name

    @staticmethod
    @otel
    async def _character_name(input: str) -> str:
        return await AppTemplates._meta_name(input, AppTemplateCsacheEnum.CHARACTER_NAME, AppFunctions.get_character_name)

    @staticmethod
    @otel
    async def _corporation_name(input: str) -> str:
        return await AppTemplates._meta_name(input, AppTemplateCsacheEnum.CORPORATION_NAME, AppFunctions.get_corporation_name)

    @staticmethod
    @otel
    async def _system_name(input: str) -> str:
        return await AppTemplates._meta_name(input, AppTemplateCsacheEnum.SYSTEM_NAME, AppFunctions.get_system_name)

    @staticmethod
    @otel
    async def _moon_name(input: str) -> str:
        return await AppTemplates._meta_name(input, AppTemplateCsacheEnum.MOON_NAME, AppFunctions.get_moon_name)

    @staticmethod
    @otel
    async def _type_name(input: str) -> str:
        return await AppTemplates._meta_name(input, AppTemplateCsacheEnum.TYPE_NAME, AppFunctions.get_type_name)

    @staticmethod
    @otel
    async def _zkillboard_character(input: str) -> str:
        return f"https://zkillboard.com/character/{input}/"

    @staticmethod
    @otel
    async def _zkillboard_corporation(input: str) -> str:
        return f"https://zkillboard.com/corporation/{input}/"

    @staticmethod
    @otel
    async def _zkillboard_system(input: str) -> str:
        return f"https://zkillboard.com/system/{input}/"

    @staticmethod
    @otel
    def _structure_state(state: str) -> str:
        map: typing.Final = {
            "deploy_vulnerable": "Deploy / Vulnerable",
            "anchoring": "Anchoring",
            "anchor_vulnerable": "Anchoring / Vulnerable",
            "onlining_vulnerable": "Onlining / Vulnerable",
            "shield_vulnerable": "Shield / Vulnerable",
            "hull_reinforce": "Hull Reinforced",
            "hull_vulnerable": "Hull / Vulnerable",
            "armor_reinforce": "Armor Reinforced",
            "armor_vulnerable": "Armor / Vulnerable",
        }
        return map.get(state, "Unknown")

    @staticmethod
    @otel
    def _timestamp_age(dt: datetime.datetime) -> str:
        return "fresh"
        age_days: typing.Final = (datetime.datetime.now(datetime.timezone.utc) - dt.replace(tzinfo=datetime.timezone.utc)).days
        if age_days >= 7:
            return "stale"

    @staticmethod
    @otel
    def _datetime(dt: datetime.datetime | None) -> str:
        if dt is None:
            return ''
        return dt.replace(tzinfo=None).isoformat(sep=" ", timespec="minutes")

    @staticmethod
    @otel
    def _date(dt: datetime.datetime | None) -> str:
        if dt is None:
            return ''
        return dt.replace(tzinfo=None).date().isoformat()

    @staticmethod
    @otel
    def _percentage(n: float) -> str:
        return f"{(100 * n):.2f}%"

    @staticmethod
    @otel
    def _commafiy(n: int) -> str:
        try:
            return f"{int(n):,d}"
        except ValueError:
            return str(n)

    @staticmethod
    @otel
    def add_templates(app: quart.Quart, evedb: AppDatabase) -> None:
        AppTemplates.EVEDB = evedb
        filters: typing.Final = {
            "login_type": AppTemplates._login_type,
            "esi_character_name": AppTemplates._character_name,
            "esi_corporation_name": AppTemplates._corporation_name,
            "esi_moon_name": AppTemplates._moon_name,
            "esi_system_name": AppTemplates._system_name,
            "esi_type_name": AppTemplates._type_name,
            "zkillboard_character": AppTemplates._zkillboard_character,
            "zkillboard_corporation": AppTemplates._zkillboard_corporation,
            "zkillboard_system": AppTemplates._zkillboard_system,
            "structure_state": AppTemplates._structure_state,
            "timestamp_age": AppTemplates._timestamp_age,
            "datetime": AppTemplates._datetime,
            "date": AppTemplates._date,
            "percentage": AppTemplates._percentage,
            "commafy": AppTemplates._commafiy,
        }
        for k, v in filters.items():
            app.add_template_filter(v, k)
