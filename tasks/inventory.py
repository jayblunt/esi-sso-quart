from typing import Final

import aiohttp
import aiohttp.client_exceptions
from sso import EveSSO

from .task import EveTask


class EveInventoryTask(EveTask):

    async def run(self):

        if "esi-assets.read_assets.v1" in self.session.get(EveSSO.ESI_TOKEN_SCOPES, []):
            session_headers = {
                "Authorization": f"Bearer {self.session.get(EveSSO.ESI_ACCESS_TOKEN, '')}"
            }
            common_params = {
                "datasource": "tranquility"
            }

            character_id: Final = self.session.get(EveSSO.ESI_CHARACTER_ID)
            inventory_list: Final = list()


            maxpageno: int = 1

            url = f"https://esi.evetech.net/latest/characters/{character_id}/assets/"

            async with aiohttp.ClientSession(headers=session_headers) as client_session:
                async with client_session.get(url, params=common_params) as response:
                    print(f"{response.url} -> {response.status}")
                    if response.status in [200]:
                        maxpageno = int(response.headers.get('X-Pages', 1))
                        inventory_list.extend(await response.json())

            async with aiohttp.ClientSession(headers=session_headers) as client_session:
                for pageno in range(2, 1 + maxpageno):

                    request_params = {**common_params, **{
                        "page": int(pageno)
                    }}

                    async with client_session.get(url, params=request_params) as response:
                        print(f"{response.url} -> {response.status}")
                        if response.status in [200]:
                            inventory_list.extend(await response.json())

        print(len(inventory_list))
        await super().run()
