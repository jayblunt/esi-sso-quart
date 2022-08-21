import asyncio
import collections
import collections.abc
import datetime
import inspect
import zoneinfo
from typing import Final, Union, Dict

import aiohttp
import aiohttp.client_exceptions
import dateutil.parser
import sqlalchemy
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql
from db import EveTables
from sso import EveSSO

from .task import EveTask


class EveStructureTask(EveTask):

    async def _get_type(self, type_id: int, http_session: aiohttp.ClientSession) -> Union[None, EveTables.UniverseType]:
        url: Final = f"https://esi.evetech.net/latest/universe/types/{type_id}/"
        attempts_remaining = self.ERROR_RETRY_COUNT
        while attempts_remaining > 0:
            async with http_session.get(url, params=self.common_params) as response:
                if response.status in [200]:
                    edict = dict()
                    for k, v in dict(await response.json()).items():
                        if k in ["name"]:
                            edict[k] = v
                        elif k in ["type_id", "group_id", "market_group_id"]:
                            edict[k] = int(v)
                        else:
                            continue
                    return EveTables.UniverseType(**edict)
                else:
                    attempts_remaining -= 1
                    self.logger.info("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"{response.url} -> {response.status}"))
                    await asyncio.sleep(self.ERROR_SLEEP_TIME)
        self.logger.error("- {}.{}: {} -> {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  id, None))
        return None

    async def run_structures(self, character_id: int, corporation_id: int, access_token: str):

        url = f"https://esi.evetech.net/latest/corporations/{corporation_id}/structures/"
        structures: Final = await self.get_pages(url, access_token)

        if len(structures) == 0:
            return

        type_id_set: Final = set()
        structure_obj_set: Final = set()
        structure_history_obj_set: Final = set()

        for x in structures:
            edict: Final = dict({
                "character_id": character_id,
            })
            for k, v in x.items():
                if k in ["fuel_expires", "state_timer_end", "state_timer_start", "unanchors_at"]:
                    v = dateutil.parser.parse(v).replace(tzinfo=zoneinfo.ZoneInfo("UTC"))
                elif k in ["corporation_id", "structure_id", "system_id", "type_id"]:
                    v = int(v)
                elif k in ["services"]:
                    v = list(filter(lambda x: x.get('name', '') == "Moon Drilling", v))
                    edict["has_moon_drill"] = bool(len(v) > 0)
                    continue
                elif k not in ["name", "state"]:
                    continue
                edict[k] = v
            obj = EveTables.Structure(**edict)
            structure_obj_set.add(obj)
            type_id_set.add(obj.type_id)

            history_obj = EveTables.StructureHistory(character_id=character_id, structure_id=int(x.get("structure_id", 0)), json=x)
            structure_history_obj_set.add(history_obj)

        # Add missing types
        async with await self.db.sessionmaker() as db, db.begin():

            existing_type_query = sqlalchemy.select(EveTables.UniverseType)
            existing_type_query_result = await db.execute(existing_type_query)
            existing_type_id_set: Final = {x.type_id for x in existing_type_query_result.scalars()}

            type_obj_set: Final = set()
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=self.LIMIT_PER_HOST)) as http_session:
                task_list: Final = list()
                for id in type_id_set - existing_type_id_set:
                    task_list.append(asyncio.ensure_future(self._get_type(id, http_session)))

                if len(task_list) > 0:
                    result_list = await asyncio.gather(*task_list)
                    self.logger.info("- {}.{}: {} = {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  "len(result_list)", len(result_list)))
                    for obj in result_list:
                        if obj is None:
                            continue
                        type_obj_set.add(obj)

            if len(type_obj_set) > 0:
                db.add_all(type_obj_set)
                await db.commit()

        # Add structures and history
        if len(structure_history_obj_set) > 0:
            # Save history
            async with await self.db.sessionmaker() as db, db.begin():
                db.add_all(structure_history_obj_set)
                await db.commit()

        if len(structure_obj_set) > 0:
            # Save current extractions
            async with await self.db.sessionmaker() as db, db.begin():
                all_structures_query: Final = sqlalchemy.select(EveTables.Structure).where(EveTables.Structure.corporation_id == corporation_id)
                all_structures_query_result = await db.execute(all_structures_query)
                existing_obj_set: Final = {result for result in all_structures_query_result.scalars()}

                if len(existing_obj_set) > 0:
                    [await db.delete(x) for x in existing_obj_set]

                db.add_all(structure_obj_set)

                await db.commit()

    async def _get_moon(self, moon_id: int, http_session: aiohttp.ClientSession) -> Union[None, EveTables.UniverseMoon]:
        url: Final = f"https://esi.evetech.net/latest/universe/moons/{moon_id}/"
        attempts_remaining = self.ERROR_RETRY_COUNT
        while attempts_remaining > 0:
            async with http_session.get(url, params=self.common_params) as response:
                if response.status in [200]:
                    edict = dict()
                    for k, v in dict(await response.json()).items():
                        if k in ["name"]:
                            edict[k] = v
                        elif k in ["moon_id", "system_id"]:
                            edict[k] = int(v)
                        else:
                            continue
                    return EveTables.UniverseMoon(**edict)
                else:
                    attempts_remaining -= 1
                    self.logger.info("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"{response.url} -> {response.status}"))
                    await asyncio.sleep(self.ERROR_SLEEP_TIME)
        self.logger.error("- {}.{}: {} -> {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  id, None))
        return None

    async def run_extractions(self, character_id: int, corporation_id: int, access_token: str):

        url = f"https://esi.evetech.net/latest/corporation/{corporation_id}/mining/extractions/"
        extractions: Final = await self.get_pages(url, access_token)

        if len(extractions) == 0:
            return

        moon_id_set: Final = set()
        extraction_obj_set: Final = set()
        extraction_history_obj_set: Final = set()

        for x in extractions:
            edict = dict({
                "character_id": character_id,
                "corporation_id": corporation_id,
            })
            for k, v in x.items():
                if k in ["chunk_arrival_time", "extraction_start_time", "natural_decay_time"]:
                    v = dateutil.parser.parse(v).replace(tzinfo=zoneinfo.ZoneInfo("UTC"))
                elif k in ["structure_id", "moon_id"]:
                    v = int(v)
                else:
                    continue
                edict[k] = v

            obj = EveTables.Extraction(**edict)
            extraction_obj_set.add(obj)
            moon_id_set.add(obj.moon_id)

            history_obj: Final = EveTables.ExtractionHistory(character_id=character_id, structure_id=int(x.get("structure_id", 0)), json=x)
            extraction_history_obj_set.add(history_obj)

        # Add missing moons
        async with await self.db.sessionmaker() as db, db.begin():

            existing_moon_query = sqlalchemy.select(EveTables.UniverseMoon)
            existing_moon_query_result = await db.execute(existing_moon_query)
            existing_moon_id_set: Final = {x.moon_id for x in existing_moon_query_result.scalars()}

            moon_obj_set: Final = set()
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=self.LIMIT_PER_HOST)) as http_session:
                task_list: Final = list()
                for id in moon_id_set - existing_moon_id_set:
                    task_list.append(asyncio.ensure_future(self._get_moon(id, http_session)))

                if len(task_list) > 0:
                    result_list = await asyncio.gather(*task_list)
                    self.logger.info("- {}.{}: {} = {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  "len(result_list)", len(result_list)))
                    for obj in result_list:
                        if obj is None:
                            continue
                        moon_obj_set.add(obj)

            if len(moon_obj_set) > 0:
                db.add_all(moon_obj_set)
                await db.commit()

        # Add extractions and history
        if len(extraction_history_obj_set) > 0:
            # Save history
            async with await self.db.sessionmaker() as db, db.begin():
                db.add_all(extraction_history_obj_set)
                await db.commit()

        if len(extraction_obj_set) > 0:
            # Save current extractions
            async with await self.db.sessionmaker() as db, db.begin():
                all_extractions_query: Final = sqlalchemy.select(EveTables.Extraction).where(EveTables.Extraction.corporation_id == corporation_id)
                all_extractions_query_result = await db.execute(all_extractions_query)
                existing_obj_set = {result for result in all_extractions_query_result.scalars()}

                if len(existing_obj_set) > 0:
                    [await db.delete(x) for x in existing_obj_set]

                db.add_all(extraction_obj_set)

                await db.commit()

    async def run(self, client_session: collections.abc.MutableMapping):
        if not client_session.get(EveSSO.ESI_CHARACTER_HAS_STATION_MANAGER_ROLE, False):
            return

        character_id: Final = int(client_session.get(EveSSO.ESI_CHARACTER_ID, 0))
        corporation_id: Final = int(client_session.get(EveSSO.ESI_CORPORATION_ID, 0))
        access_token: Final = client_session.get(EveSSO.ESI_ACCESS_TOKEN, '')

        task_list: Final = list()

        if "esi-corporations.read_structures.v1" in client_session.get(EveSSO.ESI_ACCESS_TOKEN_SCOPES, []):
            task_list.append(asyncio.ensure_future(self.run_structures(character_id, corporation_id, access_token)))

        if "esi-industry.read_corporation_mining.v1" in client_session.get(EveSSO.ESI_ACCESS_TOKEN_SCOPES, []):
            task_list.append(asyncio.ensure_future(self.run_extractions(character_id, corporation_id, access_token)))

        if len(task_list) > 0:
            await asyncio.gather(*task_list)


class EveStructurePollingTask(EveStructureTask):

    async def run(self, client_session: collections.abc.MutableSet):
        refresh_interval: Final = datetime.timedelta(seconds=900)
        while True:
            now: Final = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

            available_corporation_id_dict: Final[Dict[int, EveTables.PeriodicCredentials]] = dict()
            async with await self.db.sessionmaker() as db, db.begin():
                query = sqlalchemy.select(EveTables.PeriodicCredentials).where(EveTables.PeriodicCredentials.is_permitted.is_(True)).order_by(sqlalchemy.asc(EveTables.PeriodicCredentials.access_token_exiry))
                results = await db.execute(query)
                rl = [x for x in results.scalars()]
                for obj in rl:
                    if isinstance(obj, EveTables.PeriodicCredentials):
                        if not obj.is_station_manager_role:
                            continue

                        if not available_corporation_id_dict.get(obj.corporation_id) is None:
                            continue

                        available_corporation_id_dict[obj.corporation_id] = obj

            refresh_obj: EveTables.Structure = None
            async with await self.db.sessionmaker() as db, db.begin():
                query = sqlalchemy.select(EveTables.Structure).where(EveTables.Structure.corporation_id.in_(available_corporation_id_dict.keys())).order_by(sqlalchemy.asc(EveTables.Structure.timestamp)).limit(1)
                results = await db.execute(query)
                obj_set: Final = {x for x in results.scalars()}
                if len(obj_set) > 0:
                    refresh_obj = obj_set.pop()

            if refresh_obj is None:
                await asyncio.sleep(int(refresh_interval.total_seconds()))
                continue

            # Hack to avoid wierd timing bug
            if refresh_obj.timestamp + refresh_interval >= now - datetime.timedelta(seconds=15):
                remaining_interval: Final = (refresh_obj.timestamp + refresh_interval) - now
                await asyncio.sleep(int(min(refresh_interval.total_seconds(), remaining_interval.total_seconds())))
                continue

            corporation_id: Final = refresh_obj.corporation_id
            character_id: Final = available_corporation_id_dict[corporation_id].character_id
            access_token: Final = available_corporation_id_dict[corporation_id].access_token

            self.logger.warning("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"UPDATING {corporation_id} via {character_id}"))

            task_list: Final = [
                asyncio.ensure_future(self.run_structures(character_id, corporation_id, access_token)),
                asyncio.ensure_future(self.run_extractions(character_id, corporation_id, access_token)),
            ]
            if len(task_list) > 0:
                await asyncio.gather(*task_list)

