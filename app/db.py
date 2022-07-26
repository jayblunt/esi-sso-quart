import enum

import sqlalchemy
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.asyncio.engine
import sqlalchemy.orm
import sqlalchemy.sql

from support.telemetry import otel


class AppAccessType(enum.Enum):
    CHARACTER = 0
    CORPORATION = 1
    ALLIANCE = 2


class AppAuthType(enum.Enum):
    LOGIN = 0
    LOGOUT = 1
    REFRESH = 2
    LOGIN_USER = 3
    LOGIN_CONTRIBUTOR = 4


class AppTables:


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

        # structures = sqlalchemy.orm.relationship("Structure", back_populates="corporation", viewonly=True)
        # scheduled_extractions = sqlalchemy.orm.relationship("ScheduledExtraction", back_populates="corporation", viewonly=True)
        # completed_extractions = sqlalchemy.orm.relationship("CompletedExtraction", back_populates="corporation", viewonly=True)

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

        # moons = sqlalchemy.orm.relationship("UniverseMoon", back_populates="system", viewonly=True)
        # structures = sqlalchemy.orm.relationship("Structure", back_populates="system", viewonly=True)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(constellation_id={self.constellation_id}, system_id={self.system_id}, name={self.name})"


    class UniverseMoon(Base):
        __tablename__ = "esi_universe_moons"
        moon_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        system_id = sqlalchemy.Column(sqlalchemy.BigInteger, sqlalchemy.ForeignKey("esi_universe_systems.system_id"), nullable=False)
        name = sqlalchemy.Column(sqlalchemy.UnicodeText, nullable=False)

        system = sqlalchemy.orm.relationship("UniverseSystem", viewonly=True)
        # scheduled_extractions = sqlalchemy.orm.relationship("ScheduledExtraction", back_populates="moon", viewonly=True)
        # completed_extractions = sqlalchemy.orm.relationship("CompletedExtraction", back_populates="moon", viewonly=True)

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
        market_group_id = sqlalchemy.Column(sqlalchemy.BigInteger)
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
        fuel_expires = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=True)
        unanchors_at = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=True)
        has_moon_drill = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False)

        corporation = sqlalchemy.orm.relationship("Corporation", viewonly=True)
        system = sqlalchemy.orm.relationship("UniverseSystem", viewonly=True)

        # scheduled_extractions = sqlalchemy.orm.relationship("ScheduledExtraction", back_populates="structure", cascade="all, delete")
        # completed_extractions = sqlalchemy.orm.relationship("CompletedExtraction", back_populates="structure", cascade="all, delete")

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(corporation_id={self.corporation_id}, structure_id={self.structure_id}, name={self.name}, state={self.state}, fuel_expires={self.fuel_expires})"


    class StructureHistory(Base):
        __tablename__ = "app_structure_history"
        id = sqlalchemy.Column(sqlalchemy.BigInteger, sqlalchemy.Sequence("app_structure_history_id_seq", start=1), primary_key=True)
        exists = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False)
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
        fuel_expires = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=True)
        unanchors_at = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=True)
        has_moon_drill = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False)

        corporation = sqlalchemy.orm.relationship("Corporation", viewonly=True)
        system = sqlalchemy.orm.relationship("UniverseSystem", viewonly=True)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(exists={self.exists}, structure_id={self.structure_id}, system_id={self.system_id}, name={self.name})"


    class StructurQueryLog(Base):
        __tablename__ = "app_structure_query_log"
        timestamp = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), primary_key=True, server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        corporation_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        character_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        json = sqlalchemy.Column(sqlalchemy.JSON, nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(timestamp={self.timestamp}, corporation_id={self.corporation_id}, character_id={self.character_id}), len={len(self.json)})"


    class StructureModifiers(Base):
        __tablename__ = "app_structure_modifiers"
        structure_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        belt_lifetime_modifier = sqlalchemy.Column(sqlalchemy.Numeric, default=1.0, nullable=False)


    class ScheduledExtraction(Base):
        __tablename__ = "app_scheduled_extraction"
        timestamp = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        character_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
        corporation_id = sqlalchemy.Column(sqlalchemy.BigInteger, sqlalchemy.ForeignKey("esi_corporations.corporation_id"), nullable=False)
        structure_id = sqlalchemy.Column(sqlalchemy.BigInteger, sqlalchemy.ForeignKey("app_structure.structure_id"), primary_key=True, nullable=False)
        moon_id = sqlalchemy.Column(sqlalchemy.BigInteger, sqlalchemy.ForeignKey("esi_universe_moons.moon_id"), nullable=False)
        extraction_start_time = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=False)
        chunk_arrival_time = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=False)
        natural_decay_time = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=False)

        structure = sqlalchemy.orm.relationship("Structure", viewonly=True)
        corporation = sqlalchemy.orm.relationship("Corporation", viewonly=True)
        moon = sqlalchemy.orm.relationship("UniverseMoon", viewonly=True)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(structure_id={self.structure_id}, corporation_id={self.corporation_id}, moon_id={self.moon_id}, extraction_start_time={self.extraction_start_time}, chunk_arrival_time={self.chunk_arrival_time})"


    class CompletedExtraction(Base):
        __tablename__ = "app_completed_extraction"
        timestamp = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        character_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
        corporation_id = sqlalchemy.Column(sqlalchemy.BigInteger, sqlalchemy.ForeignKey("esi_corporations.corporation_id"), nullable=False)
        structure_id = sqlalchemy.Column(sqlalchemy.BigInteger, sqlalchemy.ForeignKey("app_structure.structure_id"), primary_key=True, nullable=False)
        moon_id = sqlalchemy.Column(sqlalchemy.BigInteger, sqlalchemy.ForeignKey("esi_universe_moons.moon_id"), nullable=False)
        extraction_start_time = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=False)
        chunk_arrival_time = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=False)
        natural_decay_time = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=False)
        belt_decay_time = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=False)

        structure = sqlalchemy.orm.relationship("Structure", viewonly=True)
        corporation = sqlalchemy.orm.relationship("Corporation", viewonly=True)
        moon = sqlalchemy.orm.relationship("UniverseMoon", viewonly=True)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(structure_id={self.structure_id}, corporation_id={self.corporation_id}, moon_id={self.moon_id}, extraction_start_time={self.extraction_start_time}, chunk_arrival_time={self.chunk_arrival_time}, belt_decay_time={self.belt_decay_time})"


    class ExtractionHistory(Base):
        __tablename__ = "app_extraction_history"
        id = sqlalchemy.Column(sqlalchemy.BigInteger, sqlalchemy.Sequence("app_extraction_history_id_seq", start=1), primary_key=True)
        exists = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False)
        timestamp = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        character_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
        corporation_id = sqlalchemy.Column(sqlalchemy.BigInteger, sqlalchemy.ForeignKey("esi_corporations.corporation_id"), nullable=False)
        structure_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        moon_id = sqlalchemy.Column(sqlalchemy.BigInteger, sqlalchemy.ForeignKey("esi_universe_moons.moon_id"), nullable=False)
        extraction_start_time = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=False)
        chunk_arrival_time = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=False)
        natural_decay_time = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=False)

        corporation = sqlalchemy.orm.relationship("Corporation", viewonly=True)
        moon = sqlalchemy.orm.relationship("UniverseMoon", viewonly=True)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(exists={self.exists}, structure_id={self.structure_id}, moon_id={self.moon_id}, extraction_start_time={self.extraction_start_time})"


    class ExtractionQueryLog(Base):
        __tablename__ = "app_extraction_query_log"
        timestamp = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), primary_key=True, server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        corporation_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        character_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        json = sqlalchemy.Column(sqlalchemy.JSON, nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(timestamp={self.timestamp}, corporation_id={self.corporation_id}, character_id={self.character_id}), len={len(self.json)})"


    class MoonYield(Base):
        __tablename__ = "app_moon_yields"
        moon_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        planet_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=False, nullable=False)
        system_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
        type_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        yield_percent = sqlalchemy.Column(sqlalchemy.Float, nullable=False)


    class PeriodicCredentials(Base):
        __tablename__ = "app_periodic_credentials"
        timestamp = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        character_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        corporation_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
        is_permitted = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False)
        is_enabled = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False)
        is_director_role = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False)
        is_accountant_role = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False)
        is_station_manager_role = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False)
        session_id = sqlalchemy.Column(sqlalchemy.UnicodeText, nullable=False)
        access_token_issued = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=False)
        access_token_expiry = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), nullable=False)
        refresh_token = sqlalchemy.Column(sqlalchemy.UnicodeText, nullable=False)
        access_token = sqlalchemy.Column(sqlalchemy.UnicodeText, nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(character_id={self.character_id}, corporation_id={self.corporation_id}, is_enabled={self.is_enabled}, is_director_role={self.is_director_role}, is_accountant_role={self.is_accountant_role}, is_station_manager_role={self.is_station_manager_role}, access_token_issued={self.access_token_issued}, access_token_expiry={self.access_token_expiry})"


    class PeriodicTaskTimestamp(Base):
        __tablename__ = "app_periodic_task_timestamp"
        # id = sqlalchemy.Column(sqlalchemy.BigInteger, sqlalchemy.Sequence("app_refresh_history_id_srq", start=1), primary_key=True)
        timestamp = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        corporation_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        character_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(timestamp={self.timestamp}, corporation_id={self.corporation_id}, character_id={self.character_id})"


    class AccessControls(Base):
        __tablename__ = "app_access_control"
        id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        type = sqlalchemy.Column(sqlalchemy.Enum(AppAccessType), primary_key=True, nullable=False)
        permit = sqlalchemy.Column(sqlalchemy.Boolean, primary_key=True, nullable=False)
        trust = sqlalchemy.Column(sqlalchemy.Boolean, nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(id={self.id}, type={self.type}, permit={self.permit})"


    class AccessHistory(Base):
        __tablename__ = "app_access_history"
        id = sqlalchemy.Column(sqlalchemy.BigInteger, sqlalchemy.Sequence("app_access_history_id_seq", start=1), primary_key=True)
        timestamp = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        character_id = sqlalchemy.Column(sqlalchemy.BigInteger, nullable=False)
        permitted = sqlalchemy.Column(sqlalchemy.Boolean)
        path = sqlalchemy.Column(sqlalchemy.UnicodeText, nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(timestamp={self.timestamp}, character_id={self.character_id}, path={self.path})"


    class AuthLog(Base):
        __tablename__ = "app_auth_log"
        timestamp = sqlalchemy.Column(sqlalchemy.DateTime(timezone=True), primary_key=True, server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        character_id = sqlalchemy.Column(sqlalchemy.BigInteger, primary_key=True, nullable=False)
        session_id = sqlalchemy.Column(sqlalchemy.UnicodeText, nullable=False)
        auth_type = sqlalchemy.Column(sqlalchemy.Enum(AppAuthType), nullable=False)


class AppDatabase:

    def __init__(self, db: str, echo: bool = False) -> None:
        self._engine = sqlalchemy.ext.asyncio.create_async_engine(db, echo=echo, future=False, pool_size=8, max_overflow=0)
        self._sessionmaker = None
        self._initialized = False

    async def _initialize(self) -> None:
        if not self._initialized:
            async with self._engine.begin() as transaction:
                await transaction.run_sync(AppTables.Base.metadata.create_all)
            self._initialized = True

    @property
    def engine(self) -> sqlalchemy.ext.asyncio.engine.AsyncEngine:
        return self._engine

    @otel
    async def sessionmaker(self) -> sqlalchemy.ext.asyncio.AsyncSession:
        if self._sessionmaker is None:
            self._sessionmaker = sqlalchemy.orm.sessionmaker(self.engine, expire_on_commit=False, class_=sqlalchemy.ext.asyncio.AsyncSession)
        return self._sessionmaker()
