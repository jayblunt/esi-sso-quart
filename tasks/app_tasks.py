import asyncio
import collections
import collections.abc
import datetime
import inspect
import os
import zoneinfo
from typing import Dict, Final, Union

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
from telemetry import otel, otel_add_error, otel_add_event

from .task import EveTask


class EveStructureTask(EveTask):

    async def _get_url(self, url: str, http_session: aiohttp.ClientSession) -> dict | None:
        attempts_remaining = self.ERROR_RETRY_COUNT
        while attempts_remaining > 0:
            async with await http_session.get(url, params=self.common_params) as response:
                if response.status in [200]:
                    return dict(await response.json())
                else:
                    attempts_remaining -= 1
                    otel_add_error(f"{response.url} -> {response.status}")
                    self.logger.warning("- {}.{}: {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  f"{response.url} -> {response.status}"))
                    await asyncio.sleep(self.ERROR_SLEEP_TIME)
        self.logger.error(f"- {self.__class__.__name__}.{inspect.currentframe().f_code.co_name}: {url}")
        return None

    async def _get_type(self, type_id: int, http_session: aiohttp.ClientSession) -> None | EveTables.UniverseType:
        url: Final = f"https://esi.evetech.net/latest/universe/types/{type_id}/"
        rdict: Final = await self._get_url(url, http_session)
        if rdict is not None:
            edict: Final = dict()
            for k, v in rdict.items():
                if k in ["name"]:
                    edict[k] = v
                elif k in ["type_id", "group_id", "market_group_id"]:
                    edict[k] = int(v)
                else:
                    continue
            return EveTables.UniverseType(**edict)
        return None

    async def _get_corporation(self, corporation_id: int, http_session: aiohttp.ClientSession) -> None | EveTables.Corporation:
        url: Final = f"https://esi.evetech.net/latest/corporations/{corporation_id}/"
        rdict: Final = await self._get_url(url, http_session)
        if rdict is not None:
            edict: Final = dict({
                "corporation_id": corporation_id
            })
            for k, v in rdict.items():
                if k in ["alliance_id"]:
                    edict[k] = int(v)
                elif k not in ["name", "ticker"]:
                    continue
                edict[k] = v
            return EveTables.Corporation(**edict)
        return None

    async def _get_moon(self, moon_id: int, http_session: aiohttp.ClientSession) -> None | EveTables.UniverseMoon:
        url: Final = f"https://esi.evetech.net/latest/universe/moons/{moon_id}/"
        rdict: Final = await self._get_url(url, http_session)
        if rdict is not None:
            edict: Final = dict()
            for k, v in rdict.items():
                if k in ["name"]:
                    edict[k] = v
                elif k in ["moon_id", "system_id"]:
                    edict[k] = int(v)
                else:
                    continue
            return EveTables.UniverseMoon(**edict)
        return None

    @otel
    async def backfill_types(self, type_id_set: set) -> None:
        async with await self.db.sessionmaker() as db, db.begin():

            existing_type_query = sqlalchemy.select(EveTables.UniverseType)
            existing_type_query_result = await db.execute(existing_type_query)
            existing_type_id_set: Final = {x.type_id for x in existing_type_query_result.scalars()}

            type_obj_set: Final = set()
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=self.LIMIT_PER_HOST)) as http_session:
                type_task_list: Final = list()
                for id in type_id_set - existing_type_id_set:
                    type_task_list.append(asyncio.ensure_future(self._get_type(id, http_session)))

                if len(type_task_list) > 0:
                    result_list = await asyncio.gather(*type_task_list)
                    for obj in result_list:
                        if obj is None:
                            continue
                        type_obj_set.add(obj)

            if len(type_obj_set) > 0:
                db.add_all(type_obj_set)
                await db.commit()

    @otel
    async def backfill_moons(self, moon_id_set: set) -> None:
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
                    for obj in result_list:
                        if obj is None:
                            continue
                        moon_obj_set.add(obj)

            if len(moon_obj_set) > 0:
                db.add_all(moon_obj_set)
                await db.commit()

    @otel
    async def backfill_corporations(self, corporation_id_set: set) -> None:
        async with await self.db.sessionmaker() as db, db.begin():

            existing_corporation_query = sqlalchemy.select(EveTables.Corporation)
            existing_corporation_query_result = await db.execute(existing_corporation_query)
            existing_corporation_id_set: Final = {x.corporation_id for x in existing_corporation_query_result.scalars()}

            corporation_obj_set: Final = set()
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit_per_host=self.LIMIT_PER_HOST)) as http_session:
                corporation_task_list: Final = list()
                for id in corporation_id_set - existing_corporation_id_set:
                    corporation_task_list.append(asyncio.ensure_future(self._get_corporation(id, http_session)))

                if len(corporation_task_list) > 0:
                    result_list = await asyncio.gather(*corporation_task_list)
                    for obj in result_list:
                        if obj is None:
                            continue
                        corporation_obj_set.add(obj)

            if len(corporation_obj_set) > 0:
                db.add_all(corporation_obj_set)
                await db.commit()

    @otel
    async def run_structures(self, now: datetime.datetime, character_id: int, corporation_id: int, access_token: str):

        url = f"https://esi.evetech.net/latest/corporations/{corporation_id}/structures/"
        structures: Final = await self.get_pages(url, access_token)

        if len(structures) == 0:
            return

        type_id_set: Final = set()
        corporation_id_set: Final = set()
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
            corporation_id_set.add(obj.corporation_id)

            history_obj = EveTables.StructureHistory(character_id=character_id, structure_id=int(x.get("structure_id", 0)), json=x)
            structure_history_obj_set.add(history_obj)

        # Add missing types
        await self.backfill_types(type_id_set)

        # Add missing corporations
        await self.backfill_corporations(corporation_id_set)

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

    @otel
    async def run_extractions(self, now: datetime.datetime, character_id: int, corporation_id: int, access_token: str):

        url = f"https://esi.evetech.net/latest/corporation/{corporation_id}/mining/extractions/"
        extractions: Final = await self.get_pages(url, access_token)

        if len(extractions) == 0:
            return

        moon_id_set: Final = set()
        corporation_id_set: Final = set()
        fresh_extractions_obj_dict: Final = dict()
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

            obj = EveTables.ScheduledExtraction(**edict)
            fresh_extractions_obj_dict[obj.structure_id] = obj
            moon_id_set.add(obj.moon_id)
            corporation_id_set.add(obj.corporation_id)

            history_obj: Final = EveTables.ExtractionHistory(character_id=character_id, structure_id=int(x.get("structure_id", 0)), json=x)
            extraction_history_obj_set.add(history_obj)

        # Add missing moons
        await self.backfill_moons(moon_id_set)

        # Add missing corporations
        await self.backfill_corporations(corporation_id_set)

        # Append History
        if len(extraction_history_obj_set) > 0:
            async with await self.db.sessionmaker() as db, db.begin():
                db.add_all(extraction_history_obj_set)
                await db.commit()

        # Update Scheduled and Completed
        if len(fresh_extractions_obj_dict.keys()) > 0:
            async with await self.db.sessionmaker() as db, db.begin():

                completed_extractions_query: Final = sqlalchemy.select(EveTables.CompletedExtraction).where(EveTables.CompletedExtraction.corporation_id == corporation_id)
                completed_extractions_query_result = await db.execute(completed_extractions_query)
                completed_extractions_dict: Final = {x.structure_id: x for x in completed_extractions_query_result.scalars()}

                existing_extractions_query: Final = sqlalchemy.select(EveTables.ScheduledExtraction).where(EveTables.ScheduledExtraction.corporation_id == corporation_id)
                existing_extractions_query_result = await db.execute(existing_extractions_query)
                existing_extractions_obj_dict: Final = {x.structure_id: x for x in existing_extractions_query_result.scalars()}

                for structure_id in set(fresh_extractions_obj_dict.keys()).intersection(set(existing_extractions_obj_dict.keys())):

                    existing_extraction: EveTables.ScheduledExtraction = existing_extractions_obj_dict[structure_id]
                    if now >= existing_extraction.chunk_arrival_time:

                        # Remove the old / existing completed extraction
                        if structure_id in completed_extractions_dict.keys():
                            await db.delete(completed_extractions_dict[structure_id])

                        belt_lifetime_estimate: Final = datetime.timedelta(days=2)

                        modifier_query: Final = sqlalchemy.select(EveTables.StructureModifiers).where(EveTables.StructureModifiers.structure_id == structure_id).limit(1)
                        modifier_query_result: Final = await db.execute(modifier_query)
                        modifier_obj_dct: Final[dict[int, EveTables.StructureModifiers]] = {x.structure_id: x for x in modifier_query_result.scalars()}

                        modifier_obj = modifier_obj_dct.get(structure_id)
                        if modifier_obj is not None:
                            belt_lifetime_estimate *= float(modifier_obj.belt_lifetime_modifier)

                        completed_extraction = EveTables.CompletedExtraction(
                            character_id=existing_extraction.character_id,
                            corporation_id=existing_extraction.corporation_id,
                            structure_id=existing_extraction.structure_id,
                            moon_id=existing_extraction.moon_id,
                            extraction_start_time=existing_extraction.extraction_start_time,
                            chunk_arrival_time=existing_extraction.chunk_arrival_time,
                            natural_decay_time=existing_extraction.natural_decay_time,
                            belt_decay_time=existing_extraction.chunk_arrival_time + belt_lifetime_estimate)

                        db.add(completed_extraction)

                # We wipe and re-insert to handle the case where a structure goes away between taaks
                if len(existing_extractions_obj_dict.keys()) > 0:
                    [await db.delete(x) for x in existing_extractions_obj_dict.values()]

                db.add_all(fresh_extractions_obj_dict.values())

                await db.commit()

    @otel
    async def run(self, client_session: collections.abc.MutableMapping):
        if not client_session.get(EveSSO.ESI_CHARACTER_HAS_STATION_MANAGER_ROLE, False):
            return

        character_id: Final = int(client_session.get(EveSSO.ESI_CHARACTER_ID, 0))
        corporation_id: Final = int(client_session.get(EveSSO.ESI_CORPORATION_ID, 0))
        access_token: Final = client_session.get(EveSSO.ESI_ACCESS_TOKEN, '')

        now: Final = datetime.datetime.now(tz=datetime.timezone.utc)

        task_list: Final = list()

        if "esi-corporations.read_structures.v1" in client_session.get(EveSSO.ESI_ACCESS_TOKEN_SCOPES, []):
            task_list.append(asyncio.ensure_future(self.run_structures(now, character_id, corporation_id, access_token)))

        if "esi-industry.read_corporation_mining.v1" in client_session.get(EveSSO.ESI_ACCESS_TOKEN_SCOPES, []):
            task_list.append(asyncio.ensure_future(self.run_extractions(now, character_id, corporation_id, access_token)))

        if len(task_list) > 0:
            await asyncio.gather(*task_list)


class EveStructurePollingTask(EveStructureTask):

    @otel
    async def run(self, client_session: collections.abc.MutableSet):
        refresh_buffer: Final = datetime.timedelta(seconds=30)
        refresh_interval: Final = datetime.timedelta(seconds=600) - refresh_buffer
        while True:
            now: Final = datetime.datetime.now(tz=datetime.timezone.utc)

            available_corporation_id_dict: Final[dict[int, EveTables.PeriodicCredentials]] = dict()
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

            if refresh_obj.timestamp + refresh_interval > now:
                remaining_interval: Final = refresh_obj.timestamp + refresh_interval + refresh_buffer - now
                otel_add_event(inspect.currentframe().f_code.co_name, {"remaining_interval": remaining_interval.total_seconds()})
                await asyncio.sleep(int(min(refresh_interval.total_seconds(), remaining_interval.total_seconds())))
                continue

            corporation_id: Final = refresh_obj.corporation_id
            character_id: Final = available_corporation_id_dict[corporation_id].character_id
            access_token: Final = available_corporation_id_dict[corporation_id].access_token

            otel_add_event(inspect.currentframe().f_code.co_name, {"character_id": character_id, "corporation_id": corporation_id})
            self.logger.info("- {}.{}: {} {} {}".format(self.__class__.__name__, inspect.currentframe().f_code.co_name,  corporation_id, "structures updated by", character_id))

            await asyncio.gather(
                asyncio.ensure_future(self.run_structures(now, character_id, corporation_id, access_token)),
                asyncio.ensure_future(self.run_extractions(now, character_id, corporation_id, access_token)),
            )
            await asyncio.sleep(int(refresh_buffer.total_seconds()))
