import asyncio
from asyncio import futures

import sqlalchemy
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql



class EveTables:


    Base = sqlalchemy.orm.declarative_base()


    class Structure(Base):
        __tablename__ = "structure"
        timestamp = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), server_default=sqlalchemy.sql.func.now(), primary_key=True)
        structure_id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
        json = sqlalchemy.Column(sqlalchemy.JSON)


    class Extraction(Base):
        __tablename__ = "extraction"
        timestamp = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), server_default=sqlalchemy.sql.func.now(), primary_key=True)
        structure_id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
        json = sqlalchemy.Column(sqlalchemy.JSON)



class EveDatabase:

    async def _initialize(self):
        if not self._initialized:
            async with self._engine.begin() as transaction:
                await transaction.run_sync(EveTables.Base.metadata.create_all)
            self._initialized = True

    def __init__(self, db: str, echo: bool = True) -> None:
        # future=True means use the 2.0 api
        self._engine = sqlalchemy.ext.asyncio.create_async_engine(db, echo=echo, future=True)
        self._initialized = False

