import asyncio
import datetime
import inspect
import logging
import os
import typing
import json
import uuid
import dateutil

import sqlalchemy
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql

from db import EveDatabase, EveTables

# from telemetry import otel, otel_initialize


async def parse_old_extraction_archive(evedn: EveDatabase):

    epoch: typing.Final = datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc)
    switchover_dict: typing.Final = dict()
    character_dict: typing.Final = dict()
    querylog_obj_set = set()

    async with await evedb.sessionmaker() as session, session.begin():
        query = (
            sqlalchemy.select(EveTables.Character.character_id, EveTables.Character.corporation_id)
        )
        result: sqlalchemy.engine.Result = await session.execute(query)
        for one in result.all():
            character_dict[one[0]] = one[-1]

    async with await evedb.sessionmaker() as session, session.begin():
        query = (
            sqlalchemy.select(EveTables.ExtractionQueryLog.corporation_id, sqlalchemy.func.min(EveTables.ExtractionQueryLog.timestamp))
            .group_by(EveTables.ExtractionQueryLog.corporation_id)
        )
        result: sqlalchemy.engine.Result = await session.execute(query)
        for one in result.all():
            switchover_dict[one[0]] = one[-1]
        print(switchover_dict)

    async with await evedb.sessionmaker() as session, session.begin():
        query = (
            sqlalchemy.select(EveTables.ExtractionArchive)
            .order_by(sqlalchemy.asc(EveTables.ExtractionArchive.timestamp))
        )
        result: sqlalchemy.engine.Result = await session.execute(query)
        current_character_id = 0
        current_corporation_id = 0
        current_timestamp = epoch
        current_log = list()

        for one in result.scalars():
            one: EveTables.ExtractionArchive
            # print(one)

            character_id = one.character_id
            if not character_id > 0:
                continue

            if current_character_id == 0:
                current_character_id = character_id

            corporation_id = character_dict.get(character_id, 0)
            if not corporation_id > 0:
                continue

            if current_corporation_id == 0:
                current_corporation_id = corporation_id

            if current_timestamp == epoch:
                current_timestamp = one.timestamp

            if not one.timestamp < switchover_dict.get(corporation_id, epoch):
                break

            if one.timestamp - current_timestamp < datetime.timedelta(minutes=1):
                current_log.append(one.json)
            else:
                querylog_obj = EveTables.ExtractionQueryLog(timestamp=current_timestamp, corporation_id=corporation_id, character_id=character_id, json=current_log)
                querylog_obj_set.add(querylog_obj)
                print(querylog_obj)

                current_corporation_id = corporation_id
                current_timestamp = one.timestamp
                current_log = list([one.json])

        if current_character_id > 0 and current_corporation_id > 0 and current_timestamp < switchover_dict.get(current_corporation_id, epoch):
            querylog_obj = EveTables.ExtractionQueryLog(timestamp=current_timestamp, corporation_id=current_corporation_id, character_id=current_character_id, json=current_log)
            querylog_obj_set.add(querylog_obj)
            print(querylog_obj)

    if len(querylog_obj_set) > 0:
        async with await evedb.sessionmaker() as session, session.begin():
            session.add_all(querylog_obj_set)
            await session.commit()


async def parse_old_structure_archive(evedb: EveDatabase):

    epoch: typing.Final = datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc)
    switchover_dict: typing.Final = dict()
    querylog_obj_set = set()

    async with await evedb.sessionmaker() as session, session.begin():
        query = (
            sqlalchemy.select(EveTables.StructurQueryLog.corporation_id, sqlalchemy.func.min(EveTables.StructurQueryLog.timestamp))
            .group_by(EveTables.StructurQueryLog.corporation_id)
        )
        result: sqlalchemy.engine.Result = await session.execute(query)
        for one in result.all():
            switchover_dict[one[0]] = one[-1]

    async with await evedb.sessionmaker() as session, session.begin():
        query = (
            sqlalchemy.select(EveTables.StructureArchive)
            .order_by(sqlalchemy.asc(EveTables.StructureArchive.timestamp))
        )
        result: sqlalchemy.engine.Result = await session.execute(query)

        current_character_id = 0
        current_corporation_id = 0
        current_timestamp = epoch
        current_json = list()

        for one in result.scalars():
            one: EveTables.StructureArchive
            # print(one)

            if current_character_id != one.character_id:
                current_character_id = one.character_id

            corporation_id = one.json.get('corporation_id', 0)
            if corporation_id == 0:
                continue

            if current_corporation_id == 0:
                current_corporation_id = corporation_id

            if not one.timestamp < switchover_dict.get(corporation_id, epoch):
                break

            # print(one)
            if corporation_id == current_corporation_id and one.timestamp - current_timestamp < datetime.timedelta(minutes=1):
                current_json.append(one.json)
            else:
                querylog_obj = EveTables.StructurQueryLog(timestamp=current_timestamp, corporation_id=current_corporation_id, character_id=current_character_id, json=current_json)
                querylog_obj_set.add(querylog_obj)
                print(querylog_obj)

                current_corporation_id = corporation_id
                current_timestamp = one.timestamp
                current_json = list([one.json])

        if current_character_id > 0 and current_corporation_id > 0 and current_timestamp < switchover_dict.get(current_corporation_id, epoch):
            querylog_obj = EveTables.StructurQueryLog(timestamp=current_timestamp, corporation_id=current_corporation_id, character_id=current_character_id, json=current_json)
            querylog_obj_set.add(querylog_obj)
            print(querylog_obj)

    if len(querylog_obj_set) > 0:
        async with await evedb.sessionmaker() as session, session.begin():
            session.add_all(querylog_obj_set)
            await session.commit()

if __name__ == "__main__":

    # logging.basicConfig(level=logging.DEBUG)
    # otel_initialize()

    evedb: typing.Final = EveDatabase(os.getenv("SQLALCHEMY_DB_URL", ""), echo=True)

    async def async_main(evedb: EveDatabase):
        await evedb._initialize()

        await parse_old_structure_archive(evedb)
        await parse_old_extraction_archive(evedb)

        # await asyncio.sleep(10)

    asyncio.run(async_main(evedb))
