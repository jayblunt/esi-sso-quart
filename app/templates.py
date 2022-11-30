import datetime
import enum
import typing

import quart
import quart.sessions

from support.telemetry import otel

from .db import EveDatabase
from .functions import AppFunctions
from .sso import EveSSO


class AppTemplateCsacheEnum(enum.Enum):

    CHARACTER_NAME = 0
    CORPORATION_NAME = 1
    MOON_NAME = 2
    TYPE_NAME = 3


class AppTemplates:

    EVEDB: EveDatabase = None
    CACHE: typing.Final = {
        AppTemplateCsacheEnum.CHARACTER_NAME: dict(),
        AppTemplateCsacheEnum.CORPORATION_NAME: dict(),
        AppTemplateCsacheEnum.MOON_NAME: dict(),
        AppTemplateCsacheEnum.TYPE_NAME: dict(),
    }

    @staticmethod
    @otel
    def _login_type(input: str) -> str:
        client_session: typing.Final = quart.session
        login_type = client_session.get(EveSSO.APP_SESSION_TYPE, "USER")
        if login_type == "CONTRIBUTOR":
            return "contributor"
        else:
            return "user"

    @staticmethod
    @otel
    async def _character_name(input: str) -> str:
        character_id = int(input)
        character_name = AppTemplates.CACHE[AppTemplateCsacheEnum.CHARACTER_NAME].get(character_id)
        if not character_name:
            character_name = await AppFunctions.get_character_name(AppTemplates.EVEDB, character_id)
            if character_name:
                AppTemplates.CACHE[AppTemplateCsacheEnum.CHARACTER_NAME][character_id] = character_name
        return character_name

    @staticmethod
    @otel
    async def _corporation_name(input: str) -> str:
        corporation_id = int(input)
        corporation_name = AppTemplates.CACHE[AppTemplateCsacheEnum.CORPORATION_NAME].get(corporation_id)
        if not corporation_name:
            corporation_name = await AppFunctions.get_corporation_name(AppTemplates.EVEDB, corporation_id)
            if corporation_name:
                AppTemplates.CACHE[AppTemplateCsacheEnum.CORPORATION_NAME][corporation_id] = corporation_name
        return corporation_name

    @staticmethod
    @otel
    async def _moon_name(input: str) -> str:
        moon_id = int(input)
        moon_name = AppTemplates.CACHE[AppTemplateCsacheEnum.MOON_NAME].get(moon_id)
        if not moon_name:
            moon_name = await AppFunctions.get_mmon_name(AppTemplates.EVEDB, moon_id)
            if moon_name:
                AppTemplates.CACHE[AppTemplateCsacheEnum.MOON_NAME][moon_id] = moon_name
        return moon_name

    @staticmethod
    @otel
    async def _type_name(input: str) -> str:
        moon_id = int(input)
        moon_name = AppTemplates.CACHE[AppTemplateCsacheEnum.TYPE_NAME].get(moon_id)
        if not moon_name:
            moon_name = await AppFunctions.get_type_name(AppTemplates.EVEDB, moon_id)
            if moon_name:
                AppTemplates.CACHE[AppTemplateCsacheEnum.TYPE_NAME][moon_id] = moon_name
        return moon_name

    @staticmethod
    @otel
    async def _zkillboard_character(input: str) -> str:
        character_id = int(input)
        return f"https://zkillboard.com/character/{character_id}/"

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
    def _datetime(dt: datetime.datetime) -> str:
        return dt.replace(tzinfo=None).isoformat(sep=" ", timespec="minutes")

    @staticmethod
    @otel
    def _date(dt: datetime.datetime) -> str:
        return dt.replace(tzinfo=None).date().isoformat()

    @staticmethod
    @otel
    def _percentage(n: float) -> str:
        return f"{(100 * n):.2f}%"

    @staticmethod
    @otel
    def add_templates(app: quart.Quart, evedb: EveDatabase) -> None:
        AppTemplates.EVEDB = evedb
        filters: typing.Final = {
            "login_type": AppTemplates._login_type,
            "character_name": AppTemplates._character_name,
            "corporation_name": AppTemplates._corporation_name,
            "moon_name": AppTemplates._moon_name,
            "type_name": AppTemplates._type_name,
            "zkillboard_character": AppTemplates._zkillboard_character,
            "structure_state": AppTemplates._structure_state,
            "timestamp_age": AppTemplates._timestamp_age,
            "datetime": AppTemplates._datetime,
            "date": AppTemplates._date,
            "percentage": AppTemplates._percentage,
        }
        for k, v in filters.items():
            app.add_template_filter(v, k)
