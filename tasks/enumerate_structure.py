import contextlib
from typing import Final, MutableSet

import aiohttp
import aiohttp.client_exceptions
from sso import EveSSO

from .task import EveTask


class EveEnumerateStructureTask(EveTask):

    async def run(self):

        session_headers = {
            "Authorization": f"Bearer {self.session.get(EveSSO.ESI_ACCESS_TOKEN)}"
        }

        common_params = {
            "datasource": "tranquility"
        }

        corporation_id: Final = self.session.get(
            EveSSO.ESI_CORPORATEION_ID, '')

        required_scopes: Final = {
            "esi-corporations.read_structures.v1", "esi-industry.read_corporation_mining.v1"}

        structure_id_set: Final[MutableSet[str]] = set()

        if all([self.session.get(EveSSO.ESI_CHARACTER_STATION_MANAGER_ROLE, False), len(required_scopes.intersection(set(self.session.get(EveSSO.ESI_TOKEN_SCOPES, [])))) == len(required_scopes)]):

            async with aiohttp.ClientSession(headers=session_headers) as client_session:

                url = f"https://esi.evetech.net/latest/corporations/{corporation_id}/structures/"
                with contextlib.suppress(aiohttp.client_exceptions.ClientResponseError):
                    async with client_session.get(url, params=common_params) as response:
                        print(f"{response.url} -> {response.status}")
                        if response.status in [200]:
                            data = dict(await response.json())
                            for structure_id in data.get('structure', []):
                                structure_id_set.add(str(structure_id))

                url = f"https://esi.evetech.net/latest/corporation/{corporation_id}/mining/extractions/"
                with contextlib.suppress(aiohttp.client_exceptions.ClientResponseError):
                    async with client_session.get(url, params=common_params) as response:
                        print(f"{response.url} -> {response.status}")
                        if response.status in [200]:
                            data = dict(await response.json())
                            print(data)
