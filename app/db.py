import datetime
import enum
import typing

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

    class Base(sqlalchemy.orm.DeclarativeBase):
        type_annotation_map: typing.Final = {
            int: sqlalchemy.types.BigInteger,
            float: sqlalchemy.types.Float,
            datetime.datetime: sqlalchemy.types.DateTime(timezone=True),
            str: sqlalchemy.types.UnicodeText,
            dict[str, str]: sqlalchemy.JSON,
            dict[str, str] | None: sqlalchemy.JSON,
            dict | list | None: sqlalchemy.JSON,
        }

    class Character(Base):
        __tablename__: typing.Final = "esi_characters"
        character_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        corporation_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(nullable=False)
        alliance_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(nullable=True)
        birthday: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(nullable=False)
        name: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({self.character_id=}, {self.name=})"

    class Corporation(Base):
        __tablename__: typing.Final = "esi_corporations"
        corporation_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        alliance_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(nullable=True)
        name: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(nullable=False)
        ticker: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(nullable=False)

        # structures = sqlalchemy.orm.relationship("Structure", back_populates="corporation", viewonly=True)
        # scheduled_extractions = sqlalchemy.orm.relationship("ScheduledExtraction", back_populates="corporation", viewonly=True)
        # completed_extractions = sqlalchemy.orm.relationship("CompletedExtraction", back_populates="corporation", viewonly=True)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({self.corporation_id=}, {self.name=})"

    class Alliance(Base):
        __tablename__: typing.Final = "esi_alliances"
        alliance_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=True)
        name: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(nullable=False)
        ticker: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({self.alliance_id=}, {self.name=})"

    class AllianceCorporation(Base):
        __tablename__: typing.Final = "esi_alliances_corporations"
        alliance_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        corporation_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)

    class UniverseSystem(Base):
        __tablename__: typing.Final = "esi_universe_systems"
        system_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        constellation_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(nullable=False)
        name: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(nullable=False)

        # moons = sqlalchemy.orm.relationship("UniverseMoon", back_populates="system", viewonly=True)
        # structures = sqlalchemy.orm.relationship("Structure", back_populates="system", viewonly=True)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({self.constellation_id=}, {self.system_id=}, {self.name=})"

    class UniverseMoon(Base):
        __tablename__: typing.Final = "esi_universe_moons"
        moon_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        system_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.ForeignKey("esi_universe_systems.system_id"), nullable=False)
        name: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(nullable=False)

        system = sqlalchemy.orm.relationship("UniverseSystem", viewonly=True)
        # scheduled_extractions = sqlalchemy.orm.relationship("ScheduledExtraction", back_populates="moon", viewonly=True)
        # completed_extractions = sqlalchemy.orm.relationship("CompletedExtraction", back_populates="moon", viewonly=True)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({self.system_id=}, {self.moon_id=}, {self.name=})"

    class UniversePlanet(Base):
        __tablename__: typing.Final = "esi_universe_planets"
        planet_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        system_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(nullable=False)
        type_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(nullable=False)
        name: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({self.system_id=}, {self.planet_id=}, {self.name=})"

    class UniverseConstellation(Base):
        __tablename__: typing.Final = "esi_universe_constellations"
        constellation_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        region_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(nullable=False)
        name: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({self.region_id=}, {self.constellation_id=}, {self.name=})"

    class UniverseRegion(Base):
        __tablename__: typing.Final = "esi_universe_regions"
        region_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        name: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({self.region_id=}, {self.name=})"

    class UniverseType(Base):
        __tablename__: typing.Final = "esi_universe_types"
        type_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        group_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(nullable=False)
        market_group_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column()
        graphic_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(nullable=True)
        icon_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(nullable=True)
        mass: sqlalchemy.orm.Mapped[float] = sqlalchemy.orm.mapped_column(nullable=False, default=0.0)
        volume: sqlalchemy.orm.Mapped[float] = sqlalchemy.orm.mapped_column(nullable=False, default=0.0)
        name: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({self.type_id=}, {self.name=})"

    class Structure(Base):
        __tablename__: typing.Final = "app_structure"
        timestamp: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        character_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(nullable=False)
        corporation_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.ForeignKey("esi_corporations.corporation_id"), nullable=False)
        system_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.ForeignKey("esi_universe_systems.system_id"), nullable=False)
        type_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(nullable=False)
        structure_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        name: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(nullable=False)
        state: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(nullable=True)
        state_timer_start: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(nullable=True)
        state_timer_end: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(nullable=True)
        fuel_expires: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(nullable=True)
        unanchors_at: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(nullable=True)
        has_moon_drill: sqlalchemy.orm.Mapped[bool] = sqlalchemy.orm.mapped_column(nullable=False)

        corporation = sqlalchemy.orm.relationship("Corporation", viewonly=True)
        system = sqlalchemy.orm.relationship("UniverseSystem", viewonly=True)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({self.corporation_id=}, {self.structure_id=}, {self.name=}, {self.state=}, {self.fuel_expires=})"

    class StructureHistory(Base):
        __tablename__: typing.Final = "app_structure_history"
        id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.Sequence("app_structure_history_id_seq", start=1), primary_key=True, unique=True)
        exists: sqlalchemy.orm.Mapped[bool] = sqlalchemy.orm.mapped_column(nullable=False)
        timestamp: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        character_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(nullable=False)
        corporation_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.ForeignKey("esi_corporations.corporation_id"), nullable=False)
        system_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.ForeignKey("esi_universe_systems.system_id"), nullable=False)
        type_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(nullable=False)
        structure_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        name: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(nullable=False)
        state: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(nullable=True)
        state_timer_start: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(nullable=True)
        state_timer_end: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(nullable=True)
        fuel_expires: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(nullable=True)
        unanchors_at: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(nullable=True)
        has_moon_drill: sqlalchemy.orm.Mapped[bool] = sqlalchemy.orm.mapped_column(nullable=False)

        corporation = sqlalchemy.orm.relationship("Corporation", viewonly=True)
        system = sqlalchemy.orm.relationship("UniverseSystem", viewonly=True)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({self.exists=}, {self.structure_id=}, {self.system_id=}, {self.name=})"

    class StructurQueryLog(Base):
        __tablename__: typing.Final = "app_structure_query_log"
        timestamp: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(primary_key=True, server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        corporation_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        character_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        json: sqlalchemy.orm.Mapped[dict | list | None] = sqlalchemy.orm.mapped_column(sqlalchemy.JSON, nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({self.timestamp=}, {self.corporation_id=}, {self.character_id=})"

    class StructureModifiers(Base):
        __tablename__: typing.Final = "app_structure_modifiers"
        structure_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        belt_lifetime_modifier: sqlalchemy.orm.Mapped[float] = sqlalchemy.orm.mapped_column(default=1.0, nullable=False)

    class ScheduledExtraction(Base):
        __tablename__: typing.Final = "app_scheduled_extraction"
        timestamp: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        character_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(nullable=False)
        corporation_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.ForeignKey("esi_corporations.corporation_id"), nullable=False)
        structure_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.ForeignKey("app_structure.structure_id"), primary_key=True, nullable=False)
        moon_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.ForeignKey("esi_universe_moons.moon_id"), nullable=False)
        extraction_start_time: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(nullable=False)
        chunk_arrival_time: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(nullable=False)
        natural_decay_time: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(nullable=False)

        structure = sqlalchemy.orm.relationship("Structure", viewonly=True)
        corporation = sqlalchemy.orm.relationship("Corporation", viewonly=True)
        moon = sqlalchemy.orm.relationship("UniverseMoon", viewonly=True)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({self.structure_id=}, {self.corporation_id=}, {self.moon_id=}, {self.extraction_start_time=}, {self.chunk_arrival_time=})"

    class CompletedExtraction(Base):
        __tablename__: typing.Final = "app_completed_extraction"
        timestamp: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        character_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(nullable=False)
        corporation_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.ForeignKey("esi_corporations.corporation_id"), nullable=False)
        structure_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.ForeignKey("app_structure.structure_id"), primary_key=True, nullable=False)
        moon_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.ForeignKey("esi_universe_moons.moon_id"), nullable=False)
        extraction_start_time: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(nullable=False)
        chunk_arrival_time: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(nullable=False)
        natural_decay_time: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(nullable=False)
        belt_decay_time: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(nullable=False)

        structure = sqlalchemy.orm.relationship("Structure", viewonly=True)
        corporation = sqlalchemy.orm.relationship("Corporation", viewonly=True)
        moon = sqlalchemy.orm.relationship("UniverseMoon", viewonly=True)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({self.structure_id=}, {self.corporation_id=}, {self.moon_id=}, {self.extraction_start_time=}, {self.chunk_arrival_time=}, {self.belt_decay_time=})"

    class ExtractionHistory(Base):
        __tablename__: typing.Final = "app_extraction_history"
        id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.Sequence("app_extraction_history_id_seq", start=1), primary_key=True, unique=True)
        exists: sqlalchemy.orm.Mapped[bool] = sqlalchemy.orm.mapped_column(nullable=False)
        timestamp: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        character_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(nullable=False)
        corporation_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.ForeignKey("esi_corporations.corporation_id"), nullable=False)
        structure_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        moon_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.ForeignKey("esi_universe_moons.moon_id"), nullable=False)
        extraction_start_time: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(nullable=False)
        chunk_arrival_time: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(nullable=False)
        natural_decay_time: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(nullable=False)

        corporation = sqlalchemy.orm.relationship("Corporation", viewonly=True)
        moon = sqlalchemy.orm.relationship("UniverseMoon", viewonly=True)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({self.exists=}, {self.structure_id=}, {self.moon_id=}, {self.extraction_start_time=})"

    class ExtractionQueryLog(Base):
        __tablename__: typing.Final = "app_extraction_query_log"
        timestamp: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(primary_key=True, server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        corporation_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        character_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        json: sqlalchemy.orm.Mapped[dict | list | None] = sqlalchemy.orm.mapped_column(sqlalchemy.JSON, nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({self.timestamp=}, {self.corporation_id=}, {self.character_id=})"

    class ObserverHistory(Base):
        __tablename__: typing.Final = "app_observer_history"
        id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.Sequence("app_observer_history_id_seq", start=1), primary_key=True, unique=True)
        exists: sqlalchemy.orm.Mapped[bool] = sqlalchemy.orm.mapped_column(nullable=False)
        timestamp: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        corporation_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        observer_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        observer_type: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(nullable=False)
        last_updated: sqlalchemy.orm.Mapped[datetime.date] = sqlalchemy.orm.mapped_column(nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({self.corporation_id=}, {self.observer_id=}, {self.observer_type=})"

    class ObserverQueryLog(Base):
        __tablename__: typing.Final = "app_observer_query_log"
        timestamp: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(primary_key=True, server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        corporation_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        character_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        json: sqlalchemy.orm.Mapped[dict | list | None] = sqlalchemy.orm.mapped_column(sqlalchemy.JSON, nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({self.timestamp=}, {self.corporation_id=}, {self.character_id=})"

    class ObserverRecordHistory(Base):
        __tablename__: typing.Final = "app_observer_record_history"
        id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.Sequence("app_observer_record_history_id_seq", start=1), primary_key=True, unique=True)
        timestamp: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(primary_key=True, server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        observer_history_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.ForeignKey("app_observer_history.id"), nullable=False)
        corporation_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        observer_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        character_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        recorded_corporation_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(nullable=False)
        last_updated: sqlalchemy.orm.Mapped[datetime.date] = sqlalchemy.orm.mapped_column(nullable=False)
        quantity: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(nullable=False)
        type_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({self.timestamp=}, {self.observer_id=}, {self.character_id=}, {self.last_updated=}, {self.quantity=}, {self.type_id=})"

    class ObserverRecordQueryLog(Base):
        __tablename__: typing.Final = "app_observer_record_query_log"
        timestamp: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(primary_key=True, server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        corporation_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        observer_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        json: sqlalchemy.orm.Mapped[dict | list | None] = sqlalchemy.orm.mapped_column(sqlalchemy.JSON, nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({self.timestamp=}, {self.corporation_id=}, {self.observer_id=})"

    class MoonYield(Base):
        __tablename__: typing.Final = "app_moon_yields"
        moon_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        planet_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=False, nullable=False)
        system_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(nullable=False)
        type_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        yield_percent: sqlalchemy.orm.Mapped[float] = sqlalchemy.orm.mapped_column(nullable=False)

    # class SSODebugLog(Base):
    #     __tablename__: typing.Final = "app_sso_debug_log"
    #     timestamp: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(primary_key=True, server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
    #     json: sqlalchemy.orm.Mapped[dict | list | None] = sqlalchemy.orm.mapped_column(sqlalchemy.JSON, nullable=False)

    #     def __repr__(self) -> str:
    #         return f"{self.__class__.__name__}(timestamp={self.timestamp})"

    class PeriodicCredentials(Base):
        __tablename__: typing.Final = "app_periodic_credentials"
        timestamp: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        character_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        corporation_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(nullable=False)
        is_permitted: sqlalchemy.orm.Mapped[bool] = sqlalchemy.orm.mapped_column(nullable=False)
        is_enabled: sqlalchemy.orm.Mapped[bool] = sqlalchemy.orm.mapped_column(nullable=False)
        is_director_role: sqlalchemy.orm.Mapped[bool] = sqlalchemy.orm.mapped_column(nullable=False)
        is_accountant_role: sqlalchemy.orm.Mapped[bool] = sqlalchemy.orm.mapped_column(nullable=False)
        is_station_manager_role: sqlalchemy.orm.Mapped[bool] = sqlalchemy.orm.mapped_column(nullable=False)
        session_id: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(nullable=False)
        access_token_issued: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(nullable=False)
        access_token_expiry: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(nullable=False)
        refresh_token: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(nullable=False)
        access_token: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({self.character_id=}, {self.corporation_id=}, {self.is_enabled=}, {self.is_director_role=}, {self.is_accountant_role=}, {self.is_station_manager_role=}, {self.access_token_issued=}, {self.access_token_expiry=})"

    class PeriodicTaskTimestamp(Base):
        __tablename__: typing.Final = "app_periodic_task_timestamp"
        timestamp: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        corporation_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        character_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({self.timestamp=}, {self.corporation_id=}, {self.character_id=})"

    class Configuration(Base):
        __tablename__: typing.Final = "app_configuration"
        key: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        value: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)

    class AccessControls(Base):
        __tablename__: typing.Final = "app_access_control"
        id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        type: sqlalchemy.orm.Mapped[AppAccessType] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        permit: sqlalchemy.orm.Mapped[bool] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        trust: sqlalchemy.orm.Mapped[bool] = sqlalchemy.orm.mapped_column(nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({self.id=}, {self.type=}, {self.permit=})"

    class AccessHistory(Base):
        __tablename__: typing.Final = "app_access_history"
        id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(sqlalchemy.Sequence("app_access_history_id_seq", start=1), primary_key=True, unique=True)
        timestamp: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        character_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(nullable=False)
        permitted: sqlalchemy.orm.Mapped[bool] = sqlalchemy.orm.mapped_column(nullable=False)
        path: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(nullable=False)

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({self.timestamp=}, {self.character_id=}, {self.path=})"

    class AuthLog(Base):
        __tablename__: typing.Final = "app_auth_log"
        timestamp: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(primary_key=True, server_default=sqlalchemy.sql.func.now(), onupdate=sqlalchemy.sql.func.now(), nullable=False)
        character_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(primary_key=True, nullable=False)
        session_id: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(nullable=False)
        auth_type: sqlalchemy.orm.Mapped[AppAuthType] = sqlalchemy.orm.mapped_column(nullable=False)


class AppDatabase:

    def __init__(self, db: str, echo: bool = False) -> None:
        self._engine: typing.Final = sqlalchemy.ext.asyncio.create_async_engine(db, echo=echo, pool_size=8, max_overflow=4)
        self._sessionmaker = None
        self._initialized = False

    async def _initialize(self) -> None:
        if not self._initialized:
            async with self._engine.begin() as transaction:
                await transaction.run_sync(AppTables.Base.metadata.create_all)
            self._initialized = True

    @otel
    async def sessionmaker(self) -> sqlalchemy.ext.asyncio.AsyncSession:
        if self._sessionmaker is None:
            self._sessionmaker = sqlalchemy.ext.asyncio.async_sessionmaker(self._engine)
            # self._sessionmaker = sqlalchemy.orm.sessionmaker(self._engine, expire_on_commit=False, class_=sqlalchemy.ext.asyncio.AsyncSession)
        return self._sessionmaker()
