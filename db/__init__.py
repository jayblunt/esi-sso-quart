import enum

import sqlalchemy
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql


class EveAccessType(enum.Enum):
    CHARACTER = 0
    CORPORATION = 1
    ALLIANCE = 2


class EveTables:

    Base = sqlalchemy.orm.declarative_base()

    class Character(Base):
        __tablename__ = "esi_characters"
        character_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        corporation_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
        alliance_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=True)
        birthday = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=False)
        name = sqlalchemy.Column(sqlalchemy.UnicodeText, nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(character_id={self.character_id}, name={self.name})"

    class Corporation(Base):
        __tablename__ = "esi_corporations"
        corporation_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        alliance_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=True)
        name = sqlalchemy.Column(sqlalchemy.UnicodeText, nullable=False)
        ticker = sqlalchemy.Column(sqlalchemy.UnicodeText, nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(corporation_id={self.corporation_id}, name={self.name})"

    class Alliance(Base):
        __tablename__ = "esi_alliances"
        alliance_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=True)
        name = sqlalchemy.Column(sqlalchemy.UnicodeText, nullable=False)
        ticker = sqlalchemy.Column(sqlalchemy.UnicodeText, nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(alliance_id={self.alliance_id}, name={self.name})"

    class AllianceCorporation(Base):
        __tablename__ = "esi_alliances_corporations"
        alliance_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        corporation_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)

    class UniverseSystem(Base):
        __tablename__ = "esi_universe_systems"
        system_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        constellation_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
        name = sqlalchemy.Column(sqlalchemy.UnicodeText, nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(constellation_id={self.constellation_id}, system_id={self.system_id}, name={self.name})"

    class UniverseMoon(Base):
        __tablename__ = "esi_universe_moons"
        moon_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        system_id = sqlalchemy.Column(sqlalchemy.BigInteger, sqlalchemy.ForeignKey("esi_universe_systems.system_id"), nullable=False)
        name = sqlalchemy.Column(sqlalchemy.UnicodeText, nullable=False)

        system = sqlalchemy.orm.relationship("UniverseSystem")

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(system_id={self.system_id}, moon_id={self.moon_id}, name={self.name})"

    class UniversePlanet(Base):
        __tablename__ = "esi_universe_planets"
        planet_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        system_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
        type_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
        name = sqlalchemy.Column(sqlalchemy.UnicodeText, nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(system_id={self.system_id}, planet_id={self.planet_id}, name={self.name})"

    class UniverseConstellation(Base):
        __tablename__ = "esi_universe_constellations"
        constellation_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        region_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
        name = sqlalchemy.Column(sqlalchemy.UnicodeText, nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(region_id={self.region_id}, constellation_id={self.constellation_id}, name={self.name})"

    class UniverseRegion(Base):
        __tablename__ = "esi_universe_regions"
        region_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        name = sqlalchemy.Column(sqlalchemy.UnicodeText, nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(region_id={self.region_id}, name={self.name})"

    class UniverseType(Base):
        __tablename__ = "esi_universe_types"
        type_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        group_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
        market_group_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
        graphic_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
        icon_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
        name = sqlalchemy.Column(sqlalchemy.UnicodeText, nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(type_id={self.type_id}, name={self.name})"

    class Structure(Base):
        __tablename__ = "app_structure"
        timestamp = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        character_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
        corporation_id = sqlalchemy.Column(sqlalchemy.BigInteger, sqlalchemy.ForeignKey("esi_corporations.corporation_id"), nullable=False)
        system_id = sqlalchemy.Column(sqlalchemy.BigInteger, sqlalchemy.ForeignKey("esi_universe_systems.system_id"), nullable=False)
        type_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
        structure_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        name = sqlalchemy.Column(sqlalchemy.UnicodeText, nullable=False)
        state = sqlalchemy.Column(sqlalchemy.UnicodeText, nullable=True)
        state_timer_start = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=True)
        state_timer_end = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=True)
        fuel_expires = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=False)
        unanchors_at = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=True)
        has_moon_drill = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False)

        corporation = sqlalchemy.orm.relationship("Corporation")
        system = sqlalchemy.orm.relationship("UniverseSystem")

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(structure_id={self.structure_id}, system_id={self.system_id}, name={self.name})"

    class Extraction(Base):
        __tablename__ = "app_extraction"
        timestamp = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        character_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
        corporation_id = sqlalchemy.Column(sqlalchemy.BigInteger, sqlalchemy.ForeignKey("esi_corporations.corporation_id"), nullable=False)
        structure_id = sqlalchemy.Column(sqlalchemy.BigInteger, sqlalchemy.ForeignKey("app_structure.structure_id"), primary_key=True, nullable=False)
        moon_id = sqlalchemy.Column(sqlalchemy.BigInteger, sqlalchemy.ForeignKey("esi_universe_moons.moon_id"), nullable=False)
        extraction_start_time = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=False)
        chunk_arrival_time = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=False)
        natural_decay_time = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=False)

        structure = sqlalchemy.orm.relationship("Structure")
        corporation = sqlalchemy.orm.relationship("Corporation")
        moon = sqlalchemy.orm.relationship("UniverseMoon")

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(structure_id={self.structure_id}, moon_id={self.moon_id}, extraction_start_time={self.extraction_start_time})"

    class StructureHistory(Base):
        __tablename__ = "app_structure_history"
        timestamp = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), primary_key=True, server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        character_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        structure_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        json = sqlalchemy.Column(sqlalchemy.JSON, nullable=False)

    class ExtractionHistory(Base):
        __tablename__ = "app_extraction_history"
        timestamp = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), primary_key=True, server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        character_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        structure_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        json = sqlalchemy.Column(sqlalchemy.JSON, nullable=False)

    # class CharacterAlt(Base):
    #     __tablename__ = "app_character_alts"
    #     character_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
    #     alt_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)

    class MoonYield(Base):
        __tablename__ = "app_moon_yields"
        moon_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        planet_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=False, nullable=False)
        system_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
        type_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        yield_percent = sqlalchemy.Column(sqlalchemy.Float, nullable=False)

    class Credentials(Base):
        __tablename__ = "app_credentials"
        timestamp = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        character_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        json = sqlalchemy.Column(sqlalchemy.JSON, nullable=False)

    class AccessControls(Base):
        __tablename__ = "app_access"
        id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        type = sqlalchemy.Column(sqlalchemy.Enum(EveAccessType), primary_key=True, nullable=False)
        permit = sqlalchemy.Column(sqlalchemy.Boolean, primary_key=True, nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(id={self.id}, type={self.type}, permit={self.permit})"


class EveDatabase:

    async def _initialize(self):
        if not self._initialized:
            async with self._engine.begin() as transaction:
                await transaction.run_sync(EveTables.Base.metadata.create_all)
            self._initialized = True

    @property
    async def engine(self) -> sqlalchemy.ext.asyncio.engine.AsyncEngine:
        return self._engine

    async def sessionmaker(self) -> sqlalchemy.ext.asyncio.AsyncSession:
        if self._sessionmaker is None:
            self._sessionmaker = sqlalchemy.orm.sessionmaker(await self.engine, expire_on_commit=False, class_=sqlalchemy.ext.asyncio.AsyncSession)
        return self._sessionmaker()

    def __init__(self, db: str, echo: bool = True) -> None:
        self._engine = sqlalchemy.ext.asyncio.create_async_engine(db, echo=echo, future=False)
        self._sessionmaker = None
        self._initialized = False
