from typing import Final

from sso import EveSSO

from .task import EveTask


class EveInventoryTask(EveTask):

    async def run(self):

        if "esi-assets.read_assets.v1" in self.session.get(EveSSO.ESI_TOKEN_SCOPES, []):

            character_id: Final = self.session.get(EveSSO.ESI_CHARACTER_ID)

            url = f"https://esi.evetech.net/latest/characters/{character_id}/assets/"

            inventory_list: Final = await self.get_pages(url)

            print(len(inventory_list))
