import contextlib
from typing import Final, MutableSet

import aiohttp
import aiohttp.client_exceptions
import quart
from sso import EveSSO

from .task import EveTask


class EveStructureSearchTask(EveTask):

    async def run(self):

        session_headers = {
            "Authorization": f"Bearer {self.session.get(EveSSO.ESI_ACCESS_TOKEN, '')}"
        }

        common_params = {
            "datasource": "tranquility"
        }

        character_id: Final = self.session.get(EveSSO.ESI_CHARACTER_ID, '')

        system_list: Final = ["RF-GGF", "BMNV-P",
                              "31-MLU", "LSC$-P", "9GYL-O", "A9D-R0"]

        structure_id_set: Final[MutableSet[str]] = set()

        if len(system_list) and "esi-search.search_structures.v1" in self.session.get(EveSSO.ESI_TOKEN_SCOPES, []):
            system_list.sort()
            async with aiohttp.ClientSession(headers=session_headers) as client_session:

                for system_name in system_list:
                    url = f"https://esi.evetech.net/latest/characters/{character_id}/search/"
                    url_params = {**common_params, **{
                        "categories": "structure",
                        "language": "en",
                        "strict": "false",
                        "search": system_name
                    }}
                    with contextlib.suppress(aiohttp.client_exceptions.ClientResponseError):
                        async with client_session.get(url, params=url_params) as response:
                            print(f"{response.url} -> {response.status}")
                            # print(f"{response.headers}")
                            if response.status in [200]:
                                data = dict(await response.json())
                                for structure_id in data.get('structure', []):
                                    structure_id_set.add(str(structure_id))

        if len(structure_id_set) and "esi-universe.read_structures.v1" in self.session.get(EveSSO.ESI_TOKEN_SCOPES, []):
            async with aiohttp.ClientSession(headers=session_headers) as client_session:

                print(f"structure_list: {structure_id_set}")
                for structure_id in structure_id_set:
                    url = f"https://esi.evetech.net/latest/universe/structures/{structure_id}/"
                    with contextlib.suppress(aiohttp.client_exceptions.ClientResponseError):
                        async with client_session.get(url, params=common_params) as response:
                            print(f"{response.url} -> {response.status}")
                            # print(f"{response.headers}")
                            if response.status in [200]:
                                data = await response.json()
                                print(
                                    f"{structure_id}: {quart.json.dumps(data, ensure_ascii=True, indent=4)}")

        await super().run()
