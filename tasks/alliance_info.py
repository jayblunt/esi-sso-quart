import contextlib
from typing import Final, MutableSet

import aiohttp
import aiohttp.client_exceptions
from sso import EveSSO
import quart

from .task import EveTask


class EveAllianceInfoTask(EveTask):

    async def run(self):

        session_headers = {
            "Authorization": f"Bearer {self.session.get(EveSSO.ESI_ACCESS_TOKEN, '')}"
        }

        common_params = {
            "datasource": "tranquility"
        }

        corporation_id_set: Final[MutableSet[str]] = set()

        async with aiohttp.ClientSession(headers=session_headers) as client_session:

            alliance_id: Final = self.session.get(EveSSO.ESI_ALLIANCE_ID, None)
            if alliance_id is None:
                corporation_id = self.session.get(
                    EveSSO.ESI_CORPORATEION_ID, '')
                corporation_id_set.add(str(corporation_id))
            else:

                url = f"https://esi.evetech.net/latest/alliances/{alliance_id}/corporations/"
                with contextlib.suppress(aiohttp.client_exceptions.ClientResponseError):
                    async with client_session.get(url, params=common_params) as response:
                        print(f"{response.url} -> {response.status}")
                        if response.status in [200]:
                            for corporation_id in list(await response.json()):
                                corporation_id_set.add(str(corporation_id))

        if len(corporation_id_set):
            print(f"corporation_list: {corporation_id_set}")
            async with aiohttp.ClientSession(headers=session_headers) as client_session:

                for corporation_id in corporation_id_set:

                    url = f"https://esi.evetech.net/latest/corporations/{corporation_id}"
                    with contextlib.suppress(aiohttp.client_exceptions.ClientResponseError):
                        async with client_session.get(url, params=common_params) as response:
                            print(f"{response.url} -> {response.status}")
                            if response.status in [200]:
                                results = dict(await response.json())
                                print(
                                    f"{corporation_id}: {quart.json.dumps(results, ensure_ascii=True, indent=4)}")

                    if "esi-corporations.read_structures.v1" in self.session.get(EveSSO.ESI_ACCESS_TOKEN, []):
                        url = f"https://esi.evetech.net/latest/corporations/{corporation_id}/structures"
                        with contextlib.suppress(aiohttp.client_exceptions.ClientResponseError):
                            async with client_session.get(url, params=common_params) as response:
                                print(f"{response.url} -> {response.status}")
                                if response.status in [200]:
                                    results = await response.json()
                                    print(
                                        f"{corporation_id}: {quart.json.dumps(results, ensure_ascii=True, indent=4)}")

        await super().run()
