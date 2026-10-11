"""Seed the schema and a single datacenter's fixtures.

Run from the repo root::

    python -m jobs.seed_datacenter

Drops and recreates every table, then builds one owner, a handful of colocation
clients, and a datacenter that is deliberately **aligned with the telemetry
publisher** (``~/Documents/fake_bms``) so ingested readings line up:

  - the datacenter has ``id=1`` and ``name="London Demo DC"`` (matching the BMS);
  - there are 1,000 racks named ``RACK-0001``…``RACK-1000`` (matching the BMS
    rack device names);
  - each rack's metering **rack PDU** is given the id of the matching BMS device
    (``1``…``1000``), so a reading streamed for ``device.id = N`` is stored
    against the correct rack PDU. The MSB and row PDUs live in a high id range to
    avoid colliding with those.

Power readings are not seeded — they arrive from the live telemetry stream (see
``jobs.run_power_ingest``).
"""
from __future__ import annotations

from datetime import datetime

from db.database import SessionLocal, engine
from db.models import (
    Base,
    Client,
    ClientContract,
    Datacenter,
    Owner,
    PowerConnection,
    PowerDevice,
    Rack,
    Space,
    Suite,
)


def _row_names(n: int) -> list[str]:
    """AA, AB, … AZ, BA, … — as many two-letter row labels as needed."""
    labels = []
    i = 0
    while len(labels) < n:
        labels.append(f"{chr(ord('A') + i // 26)}{chr(ord('A') + i % 26)}")
        i += 1
    return labels


# ── Alignment with the BMS ───────────────────────────────────────────────────
DATACENTER_ID = 1
DATACENTER_NAME = "London Demo DC"
RACK_COUNT = 1_000                    # matches RACKS_PER_DATA_CENTER in fake_bms

# ── Floor / grid layout (5 × 25 × 8 = 1000 fully-occupied spaces) ─────────────
N_SUITES = 5
ROWS = _row_names(25)                  # 25 rows per suite, one PDU each
POSITIONS = list(range(1, 9))          # 8 rack positions per row
X_SPACING = 1.2                        # metres between positions (space centres)
Y_SPACING = 2.4                        # metres between rows (cold-aisle pitch)
X_ORIGIN = 2.0
Y_ORIGIN = 2.0

# ── Capacities (kW) ──────────────────────────────────────────────────────────
RACK_PDU_CAPACITY_KW = 15.0            # comfortably above the BMS ~10 kW nominal
PDU_CAPACITY_KW = 200.0
SUITE_SLA_KW = 2500.0
MSB_CAPACITY_KW = 12000.0

# ── Power-device id ranges ───────────────────────────────────────────────────
# Rack PDUs take ids 1..RACK_COUNT to mirror the BMS device ids. Facility gear
# (MSB, row PDUs) is pushed above that range so nothing collides.
MSB_ID = 1_000_000
PDU_ID_BASE = 1_000_000                # PDUs get 1_000_001, 1_000_002, …

# ── Clients (colocation tenants) ─────────────────────────────────────────────
CLIENTS = [
    ("Cirrus Cloud", "cloud"),
    ("Helix AI", "ai"),
    ("Meridian Bank", "enterprise"),
    ("Nimbus Systems", "enterprise"),
]


def main() -> None:
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    session = SessionLocal()
    try:
        owner = Owner(name="Zendo")
        clients = [Client(name=name, category=cat) for name, cat in CLIENTS]

        dc = Datacenter(
            id=DATACENTER_ID,
            name=DATACENTER_NAME,
            address="1 Grid Way, London, UK",
            latitude=51.5074,
            longitude=-0.1278,
        )
        dc.owner = owner
        session.add(owner)
        session.add_all(clients)
        session.add(dc)

        # One datacenter-wide main switchboard (no suite).
        msb = PowerDevice(
            id=MSB_ID,
            device_type="msb",
            name="MSB-1",
            capacity_limit_kw=MSB_CAPACITY_KW,
        )
        msb.datacenter = dc
        session.add(msb)

        number = 0            # rack index → rack PDU id (matches BMS device id)
        pdu_counter = 0
        global_row = 0
        contracts: dict[tuple[Suite, Client], float] = {}

        for suite_idx in range(N_SUITES):
            suite = Suite(
                name=f"Hall {chr(ord('A') + suite_idx)}",
                sla_capacity_kw=SUITE_SLA_KW,
            )
            suite.datacenter = dc
            session.add(suite)

            for row_idx, row in enumerate(ROWS):
                pdu_counter += 1
                pdu = PowerDevice(
                    id=PDU_ID_BASE + pdu_counter,
                    device_type="pdu",
                    name=f"Hall {chr(ord('A') + suite_idx)}-PDU-{row}",
                    capacity_limit_kw=PDU_CAPACITY_KW,
                )
                pdu.datacenter = dc
                pdu.suite = suite
                session.add(pdu)
                session.add(PowerConnection(source=msb, target=pdu))

                # Each row is rented by one client (contiguous rows per tenant).
                client = clients[global_row % len(clients)]
                global_row += 1

                for position in POSITIONS:
                    number += 1
                    rack_pdu = PowerDevice(
                        id=number,  # ← matches the BMS device id for this rack
                        device_type="rack_pdu",
                        name=f"RACK-{number:04d}-PDU",
                        capacity_limit_kw=RACK_PDU_CAPACITY_KW,
                    )
                    rack_pdu.datacenter = dc
                    rack_pdu.suite = suite
                    session.add(rack_pdu)
                    session.add(PowerConnection(source=pdu, target=rack_pdu))

                    space = Space(
                        row=row,
                        position=position,
                        x=X_ORIGIN + (position - 1) * X_SPACING,
                        y=Y_ORIGIN + row_idx * Y_SPACING,
                        rotation=0.0,
                        size="standard",
                    )
                    space.suite = suite
                    space.power_device = rack_pdu

                    rack = Rack(name=f"RACK-{number:04d}", size="standard")
                    rack.suite = suite
                    rack.client = client
                    rack.space = space
                    # rack ⇄ rack_pdu is linked through the shared space:
                    # space.power_device is this rack_pdu, and space.rack is this rack.

                    session.add_all([space, rack])
                    contracts[(suite, client)] = (
                        contracts.get((suite, client), 0.0) + RACK_PDU_CAPACITY_KW
                    )

        # Contracts: one per (suite, client), sized to the racks they hold plus
        # a little contracted headroom.
        for (suite, client), racked_kw in contracts.items():
            contract = ClientContract(
                contracted_power_kw=round(racked_kw * 1.25, 1),
                start_date=datetime(2025, 1, 1),
                end_date=None,
            )
            contract.suite = suite
            contract.client = client
            session.add(contract)

        session.commit()

        print(
            f"Seeded datacenter {dc.name!r} (id {dc.id}): "
            f"{session.query(Suite).count()} suites, "
            f"{session.query(Space).count()} spaces, "
            f"{session.query(Rack).count()} racks, "
            f"{session.query(PowerDevice).count()} power devices, "
            f"{session.query(ClientContract).count()} contracts."
        )
    finally:
        session.close()


if __name__ == "__main__":
    main()
