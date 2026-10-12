from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


# Power-device type vocabulary. The whole hierarchy is exactly these three
# levels, wired MSB -> PDU -> rack_pdu via PowerConnection rows.
DEVICE_TYPES = ("msb", "pdu", "rack_pdu")


class Owner(Base):
    """The organization that owns and operates a datacenter (the landlord).

    Renamed from the real project's ``Customer``. Plain domain data here — there
    is no login or tenant enforcement in the interview project.
    """
    __tablename__ = "owners"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)

    datacenters = relationship("Datacenter", back_populates="owner")


class Client(Base):
    """A colocation tenant: the company whose equipment sits in the racks.

    Clients rent rack space and hold contracts; distinct from :class:`Owner`,
    who owns the building.
    """
    __tablename__ = "clients"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    category = Column(String, nullable=True)  # e.g. cloud|enterprise|ai|crypto

    racks = relationship("Rack", back_populates="client")
    contracts = relationship("ClientContract", back_populates="client")


class Datacenter(Base):
    __tablename__ = "datacenters"

    id = Column(Integer, primary_key=True, autoincrement=True)
    owner_id = Column(Integer, ForeignKey("owners.id"), nullable=False)
    name = Column(String, nullable=True)
    address = Column(String, nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)

    owner = relationship("Owner", back_populates="datacenters")
    suites = relationship("Suite", back_populates="datacenter", cascade="all, delete-orphan")
    power_devices = relationship(
        "PowerDevice", back_populates="datacenter", cascade="all, delete-orphan"
    )


class Suite(Base):
    __tablename__ = "suites"

    id = Column(Integer, primary_key=True, autoincrement=True)
    datacenter_id = Column(Integer, ForeignKey("datacenters.id"), nullable=False)
    name = Column(String, nullable=False)
    sla_capacity_kw = Column(Float, nullable=True)  # contracted power ceiling for the room

    datacenter = relationship("Datacenter", back_populates="suites")
    spaces = relationship("Space", back_populates="suite", cascade="all, delete-orphan")
    racks = relationship("Rack", back_populates="suite", cascade="all, delete-orphan")
    power_devices = relationship("PowerDevice", back_populates="suite")
    contracts = relationship(
        "ClientContract", back_populates="suite", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("datacenter_id", "name", name="uq_suite_datacenter_name"),
    )


class Space(Base):
    """A fixed rack position on the floor.

    Spaces are permanent and provided up front (never created/destroyed by the
    app). Each carries floor coordinates (``x``/``y``) so a visual layer can be
    built later, and a designated rack PDU (``power_device_id``) that feeds
    whatever rack occupies it.
    """
    __tablename__ = "spaces"

    id = Column(Integer, primary_key=True, autoincrement=True)
    suite_id = Column(Integer, ForeignKey("suites.id"), nullable=False)
    row = Column(String(6), nullable=False)       # e.g. "AA", "AB"
    position = Column(Integer, nullable=False)    # 1-N within the row
    # Designated power feed: every space has one rack PDU (device_type='rack_pdu').
    power_device_id = Column(Integer, ForeignKey("power_devices.id"), nullable=False)
    # Floor geometry (world units). Only spaces are placed; power devices are not.
    x = Column(Float, nullable=True)
    y = Column(Float, nullable=True)
    rotation = Column(Float, nullable=True, default=0)
    size = Column(String(16), nullable=False, default="standard")  # standard|shallow|large

    suite = relationship("Suite", back_populates="spaces")
    power_device = relationship("PowerDevice", back_populates="spaces")
    rack = relationship("Rack", back_populates="space", uselist=False)

    __table_args__ = (
        UniqueConstraint("suite_id", "row", "position", name="uq_space_suite_row_position"),
    )


class Rack(Base):
    __tablename__ = "racks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    suite_id = Column(Integer, ForeignKey("suites.id"), nullable=False)
    client_id = Column(Integer, ForeignKey("clients.id"), nullable=True)
    space_id = Column(Integer, ForeignKey("spaces.id"), nullable=True, unique=True)
    name = Column(String, nullable=True)  # e.g. "AA01"; derive from row+position if null
    size = Column(String(16), nullable=False, default="standard")

    suite = relationship("Suite", back_populates="racks")
    client = relationship("Client", back_populates="racks")
    space = relationship("Space", back_populates="rack")

    @property
    def rack_pdu(self) -> "PowerDevice | None":
        """The rack PDU metering this rack, derived through the space it occupies
        (``space.power_device``). The rack PDU is tied to the *space*, not the
        rack — which is what lets us avoid a circular rack ⇄ power-device FK."""
        return self.space.power_device if self.space is not None else None


class PowerDevice(Base):
    """A node in the power tree: an MSB, a PDU, or a rack PDU.

    Wiring is expressed by :class:`PowerConnection` rows (MSB -> PDU -> rack_pdu).
    Only rack PDUs are metered (they own :class:`PowerReading` rows); a PDU's or
    MSB's load is derived by summing its descendants.
    """
    __tablename__ = "power_devices"

    id = Column(Integer, primary_key=True, autoincrement=True)
    datacenter_id = Column(Integer, ForeignKey("datacenters.id"), nullable=False)
    # Rack PDUs and PDUs live in a suite; the MSB is datacenter-wide (suite_id NULL).
    suite_id = Column(Integer, ForeignKey("suites.id"), nullable=True)
    device_type = Column(String, nullable=False)  # one of DEVICE_TYPES
    name = Column(String, nullable=False)
    capacity_limit_kw = Column(Float, nullable=False)   # rated power limit

    datacenter = relationship("Datacenter", back_populates="power_devices")
    suite = relationship("Suite", back_populates="power_devices")
    # A rack PDU is installed at a Space (``Space.power_device_id``); the rack that
    # occupies that space is reached via ``space.rack``. No direct rack FK — that
    # would close a Space → PowerDevice → Rack → Space cycle.
    spaces = relationship("Space", back_populates="power_device")

    outgoing_connections = relationship(
        "PowerConnection",
        foreign_keys="PowerConnection.source_id",
        back_populates="source",
        cascade="all, delete-orphan",
    )
    incoming_connections = relationship(
        "PowerConnection",
        foreign_keys="PowerConnection.target_id",
        back_populates="target",
        cascade="all, delete-orphan",
    )
    readings = relationship(
        "PowerReading",
        back_populates="device",
        cascade="all, delete-orphan",
        order_by="PowerReading.timestamp",
    )

    __table_args__ = (
        Index("ix_power_devices_datacenter_id", "datacenter_id"),
        Index("ix_power_devices_device_type", "device_type"),
    )


class PowerConnection(Base):
    """A directed edge in the power tree: ``source`` feeds ``target`` (upstream
    to downstream). MSB -> PDU and PDU -> rack_pdu."""
    __tablename__ = "power_connections"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(Integer, ForeignKey("power_devices.id"), nullable=False)
    target_id = Column(Integer, ForeignKey("power_devices.id"), nullable=False)
    allocated_kw = Column(Float, nullable=True)  # optional reserved capacity on this edge

    source = relationship(
        "PowerDevice", foreign_keys=[source_id], back_populates="outgoing_connections"
    )
    target = relationship(
        "PowerDevice", foreign_keys=[target_id], back_populates="incoming_connections"
    )

    __table_args__ = (
        UniqueConstraint("source_id", "target_id", name="uq_power_connection_source_target"),
        Index("ix_power_connections_source_id", "source_id"),
        Index("ix_power_connections_target_id", "target_id"),
    )


class PowerReading(Base):
    """A metered power sample. Only rack PDUs have readings; upstream device load
    is rolled up by summing descendants."""
    __tablename__ = "power_readings"

    device_id = Column(
        Integer, ForeignKey("power_devices.id"), primary_key=True, nullable=False
    )
    timestamp = Column(DateTime, primary_key=True, nullable=False)  # UTC
    watts = Column(Float, nullable=False)

    device = relationship("PowerDevice", back_populates="readings")

    __table_args__ = (
        Index("ix_power_readings_device_id_timestamp", "device_id", "timestamp"),
    )


class FinalizedPowerMinute(Base):
    """A committed minute; unacked inbox messages can be safely replayed."""

    __tablename__ = "finalized_power_minutes"

    datacenter_id = Column(Integer, ForeignKey("datacenters.id"), primary_key=True)
    timestamp = Column(DateTime, primary_key=True)  # naive UTC, like PowerReading


class PowerMinuteCoverage(Base):
    """Accepted sample-count histogram for a finalized datacenter minute."""

    __tablename__ = "power_minute_coverage"

    datacenter_id = Column(Integer, ForeignKey("datacenters.id"), primary_key=True)
    timestamp = Column(DateTime, primary_key=True)  # naive UTC
    sample_counts = Column(JSON, nullable=False)


class WeatherReading(Base):
    """Historical weather at a datacenter's location, ingested by the weather
    ingester (see ``ingestion/weather_ingester.py``). Provisional — the schema
    may change once the upstream source is wired up."""
    __tablename__ = "weather_readings"

    datacenter_id = Column(Integer, ForeignKey("datacenters.id"), primary_key=True)
    timestamp = Column(DateTime, primary_key=True)  # UTC, hourly
    temp_c = Column(Float, nullable=True)
    humidity_pct = Column(Float, nullable=True)
    wind_mps = Column(Float, nullable=True)


class ClientContract(Base):
    """A client's contracted power within a suite."""
    __tablename__ = "client_contracts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    suite_id = Column(Integer, ForeignKey("suites.id"), nullable=False)
    client_id = Column(Integer, ForeignKey("clients.id"), nullable=False)
    contracted_power_kw = Column(Float, nullable=False)
    start_date = Column(DateTime, nullable=True)
    end_date = Column(DateTime, nullable=True)

    suite = relationship("Suite", back_populates="contracts")
    client = relationship("Client", back_populates="contracts")

    __table_args__ = (
        UniqueConstraint("suite_id", "client_id", name="uq_client_contract_suite_client"),
        Index("ix_client_contracts_suite_id", "suite_id"),
    )
