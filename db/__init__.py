import sqlalchemy
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql


class EveTables:

    Base = sqlalchemy.orm.declarative_base()

    class Structure(Base):
        __tablename__ = "structure"
        timestamp = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), server_default=sqlalchemy.sql.func.now(), nullable=False)
        character_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
        corporation_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        system_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
        structure_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        type_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
        name = sqlalchemy.Column(sqlalchemy.UnicodeText, nullable=False)
        state = sqlalchemy.Column(sqlalchemy.UnicodeText, nullable=True)
        state_timer_start = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=True)
        state_timer_end = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=True)
        fuel_expires = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=False)
        unanchors_at = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=True)
        has_moon_drill = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(structure_id={self.structure_id}, system_id={self.system_id}, name={self.name})"

    class StructureHistory(Base):
        __tablename__ = "structure_history"
        timestamp = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), server_default=sqlalchemy.sql.func.now(), primary_key=True, nullable=False)
        character_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        structure_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        json = sqlalchemy.Column(sqlalchemy.JSON, nullable=False)

    class Extraction(Base):
        __tablename__ = "extraction"
        timestamp = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), server_default=sqlalchemy.sql.func.now(), nullable=False)
        character_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
        corporation_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
        structure_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        moon_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        extraction_start_time = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=False)
        chunk_arrival_time = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=False)
        natural_decay_time = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(structure_id={self.structure_id}, moon_id={self.moon_id}, extraction_start_time={self.extraction_start_time})"

    class ExtractionHistory(Base):
        __tablename__ = "extraction_history"
        timestamp = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), server_default=sqlalchemy.sql.func.now(), primary_key=True, nullable=False)
        character_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        structure_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        json = sqlalchemy.Column(sqlalchemy.JSON, nullable=False)

    class CharacterAlt(Base):
        __tablename__ = "character_alt"
        alt_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        character_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)

    class AllianceMember(Base):
        __tablename__ = "alliance_member"
        alliance_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        corporation_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)

    class MoonYield(Base):
        __tablename__ = "moon_yield"
        system_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        planet_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=False, nullable=False)
        moon_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        type_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        yield_percent = sqlalchemy.Column(sqlalchemy.Float, nullable=False)


class EveDatabase:

    async def _initialize(self):
        if not self._initialized:
            async with self._engine.begin() as transaction:
                await transaction.run_sync(EveTables.Base.metadata.create_all)
            self._initialized = True

    @property
    async def engine(self) -> sqlalchemy.ext.asyncio.engine.AsyncEngine:
        return self._engine

    def __init__(self, db: str, echo: bool = True) -> None:
        self._engine = sqlalchemy.ext.asyncio.create_async_engine(db, echo=echo, future=False)
        self._initialized = False
