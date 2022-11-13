import datetime
import typing

import quart
import quart.sessions

from app_functions import AppFunctions
from sso import EveSSO
from db import EveDatabase
from telemetry import otel

_CACHE = {
    EveSSO.ESI_CHARACTER_NAME: dict()
}
_EVEDB = None


class AppTemplates:


    @staticmethod
    @otel
    def _login_type(input: str):
        client_session: typing.Final = quart.session
        login_type = client_session.get(EveSSO.APP_SESSION_TYPE, "USER")
        if login_type == "CONTRIBUTOR":
            return "contributor"
        else:
            return "user"


    @staticmethod
    @otel
    async def _character_name(input: str):
        global _CACHE, _EVEDB
        character_id = int(input)
        character_name = _CACHE[EveSSO.ESI_CHARACTER_NAME].get(character_id)
        if not character_name:
            character_name = await AppFunctions.get_character_name(_EVEDB, character_id)
            if character_name:
                _CACHE[EveSSO.ESI_CHARACTER_NAME][character_id] = character_name
        return character_name


    @staticmethod
    @otel
    async def _zkillboard(input: str):
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
        age_days: typing.Final = (datetime.datetime.now(datetime.timezone.utc) - dt.replace(tzinfo=datetime.timezone.utc)).days
        if age_days >= 3:
            return "stale"
        return "fresh"


    @staticmethod
    @otel
    def _datetime(dt: datetime.datetime) -> str:
        return dt.replace(tzinfo=None).isoformat(sep=" ", timespec="minutes")


    @staticmethod
    @otel
    def add_templates(app: quart.Quart, evedb: EveDatabase) -> None:
        global _EVEDB
        _EVEDB = evedb
        filters: typing.Final = {
            "login_type": AppTemplates._login_type,
            "character_name": AppTemplates._character_name,
            "zkillboard": AppTemplates._zkillboard,
            "structure_state": AppTemplates._structure_state,
            "timestamp_age": AppTemplates._timestamp_age,
            "datetime": AppTemplates._datetime,
        }
        for k, v in filters.items():
            app.add_template_filter(v, k)
