from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from api.routers.dashboard import snapshot
from db.models import (Base, Datacenter, FinalizedPowerMinute, Owner, PowerDevice,
                       PowerMinuteCoverage, PowerReading, Rack, Space, Suite)
from ingestion.inbox import SQLiteInbox
from ingestion.power_ingester import PowerIngester, PowerWorker, parse_power_event
from ingestion.stream_client import RealDatacenterStream


class PowerIngestionTest(unittest.TestCase):
    def setUp(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        path = Path(temp.name)
        self.inbox_path = path / "inbox.db"
        self.inbox = SQLiteInbox(self.inbox_path)
        engine = create_engine(f"sqlite:///{path / 'app.db'}")
        self.addCleanup(engine.dispose)
        Base.metadata.create_all(engine)
        self.sessions = sessionmaker(bind=engine, expire_on_commit=False)
        with self.sessions() as session:
            session.add(Owner(id=1, name="owner"))
            session.add(Datacenter(id=1, owner_id=1, name="dc"))
            session.add(PowerDevice(id=1, datacenter_id=1, device_type="rack_pdu",
                                    name="rack", capacity_limit_kw=15))
            session.commit()
        self.now = datetime.now(timezone.utc)
        self.minute = self.now.replace(second=0, microsecond=0)

    def event(self, second: int, watts: float, device_id: int = 1) -> bytes:
        return json.dumps({
            "device": {"id": device_id},
            "observed_at": (self.minute + timedelta(seconds=second)).isoformat(),
            "metrics": {"active_power_kw": watts / 1000},
        }).encode()

    def worker(self, *, filter_spikes: bool = True) -> PowerWorker:
        return PowerWorker(1, SQLiteInbox(self.inbox_path), self.sessions,
                           filter_spikes=filter_spikes, clock=lambda: self.now)

    def statuses(self) -> list[str]:
        with closing(sqlite3.connect(self.inbox_path)) as db:
            return [row[0] for row in db.execute("SELECT status FROM inbox ORDER BY id")]

    def test_collector_keeps_malformed_raw_frame(self) -> None:
        class Stream:
            def subscribe(self, token, data_center_id):
                yield "{malformed"
                raise RuntimeError("stop test stream")

        with self.assertRaisesRegex(RuntimeError, "stop test stream"):
            PowerIngester(Stream(), "token", 1, self.inbox).run()
        self.assertEqual(self.inbox.claim(1, 0, 10)[0].payload, b"{malformed")

    def test_stream_client_yields_only_parsed_metric_events(self) -> None:
        class WebSocket:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                pass

            def send(self, message):
                pass

            def recv(self):
                return '{"type":"authenticated"}'

            def __iter__(self):
                return iter(['{"type":"heartbeat"}', self.event])

        socket = WebSocket()
        socket.event = self.event(5, 1000).decode()
        with patch("ingestion.stream_client.connect", return_value=socket):
            events = list(RealDatacenterStream("ws://example").subscribe("token", 1))
        self.assertEqual(events, [json.loads(socket.event)])

    def test_collector_serializes_stream_events_for_inbox(self) -> None:
        event = json.loads(self.event(5, 1000))

        class Stream:
            def subscribe(self, token, data_center_id):
                yield event
                raise RuntimeError("stop test stream")

        with self.assertRaisesRegex(RuntimeError, "stop test stream"):
            PowerIngester(Stream(), "token", 1, self.inbox).run()
        self.assertEqual(json.loads(self.inbox.claim(1, 0, 10)[0].payload), event)

    def test_parser_rejects_bad_values_and_normalizes_utc(self) -> None:
        valid = json.loads(self.event(5, 1000))
        valid["observed_at"] = "2026-10-11T15:00:05+01:00"
        self.assertEqual(parse_power_event(json.dumps(valid).encode())[1].isoformat(),
                         "2026-10-11T14:00:05+00:00")
        for power in (None, -1, float("nan"), 10 ** 1000):
            valid["metrics"]["active_power_kw"] = power
            self.assertIsNone(parse_power_event(json.dumps(valid).encode()))

    def test_collector_retries_same_frame_after_append_failure(self) -> None:
        class Stream:
            def subscribe(self, token, data_center_id):
                yield "raw-frame"
                raise RuntimeError("stop test stream")

        original = self.inbox.append
        attempts = []

        def append(data_center_id, payload):
            attempts.append(payload)
            if len(attempts) == 1:
                raise sqlite3.OperationalError("database is locked")
            original(data_center_id, payload)

        with patch.object(self.inbox, "append", side_effect=append), \
                patch("ingestion.power_ingester.time.sleep"):
            with self.assertRaisesRegex(RuntimeError, "stop test stream"):
                PowerIngester(Stream(), "token", 1, self.inbox).run()
        self.assertEqual(attempts, [b"raw-frame", b"raw-frame"])
        self.assertEqual(self.inbox.claim(1, 0, 10)[0].payload, b"raw-frame")

    def test_restart_rebuilds_minute_and_deduplicates(self) -> None:
        for payload in (self.event(5, 1000), self.event(10, 3000),
                        self.event(5, 9000), b"not-json", self.event(20, 5000, 999)):
            self.inbox.append(1, payload)

        self.worker().process_once()
        self.assertEqual(self.statuses(), ["in_flight", "in_flight", "acked", "acked", "acked"])

        self.now = self.minute + timedelta(minutes=2)
        restarted = self.worker()
        restarted.process_once()
        restarted.process_once()
        with self.sessions() as session:
            readings = session.scalars(select(PowerReading)).all()
            self.assertEqual(len(readings), 1)
            self.assertEqual(readings[0].watts, 2000)
            self.assertEqual(session.query(FinalizedPowerMinute).count(), 1)
        self.assertEqual(self.statuses(), ["acked"] * 5)

    def test_repeats_spikes_delays_and_uneven_coverage(self) -> None:
        with self.sessions() as session:
            PowerMinuteCoverage.__table__.drop(bind=session.get_bind())
        with self.sessions() as session:
            session.add(PowerDevice(id=2, datacenter_id=1, device_type="rack_pdu",
                                    name="other rack", capacity_limit_kw=15))
            session.commit()
        for payload in (self.event(5, 1000), self.event(5, 1000),
                        self.event(5, 2000), self.event(10, 968535),
                        self.event(10, 5000), self.event(20, 9000),
                        self.event(5, 2000, 2)):
            self.inbox.append(1, payload)
        with closing(sqlite3.connect(self.inbox_path)) as db:
            with db:
                db.execute("UPDATE inbox SET received_at = ?",
                           ((self.minute + timedelta(seconds=25)).isoformat(),))

        worker = self.worker()
        with self.assertLogs("ingestion.power_ingester", level="INFO") as logs:
            worker.process_once()
            self.now = self.minute + timedelta(minutes=2)
            worker.process_once()
        with self.sessions() as session:
            readings = {r.device_id: r.watts for r in session.scalars(select(PowerReading))}
            coverage = session.get(PowerMinuteCoverage,
                                   (1, self.minute.replace(tzinfo=None)))
        self.assertEqual(readings, {1: 5000, 2: 2000})
        self.assertEqual(coverage.sample_counts, {"1": 1, "3": 1})
        self.assertEqual(worker.stats["delayed"], 4)
        with closing(sqlite3.connect(self.inbox_path)) as db:
            results = [row[0] for row in db.execute("SELECT result FROM inbox ORDER BY id")]
        self.assertEqual(results, ["accepted", "duplicate", "conflicting_duplicate",
                                   "spike", "accepted", "accepted", "accepted"])
        self.assertTrue(any("power spike ignored" in line for line in logs.output))
        self.assertTrue(any("conflicting power reading" in line for line in logs.output))
        self.assertTrue(any("devices=2/2 sample_counts={1: 1, 3: 1}" in line
                            for line in logs.output))

    def test_spike_only_minute_is_dropped_without_finalization(self) -> None:
        self.inbox.append(1, self.event(5, 968535))
        worker = self.worker()
        worker.process_once()
        self.now = self.minute + timedelta(minutes=2)
        worker.process_once()
        with self.sessions() as session:
            self.assertEqual(session.query(PowerReading).count(), 0)
            self.assertEqual(session.query(FinalizedPowerMinute).count(), 0)
        self.assertEqual(self.statuses(), ["acked"])

    def test_spike_filter_can_be_disabled(self) -> None:
        self.inbox.append(1, self.event(5, 968535))
        worker = self.worker(filter_spikes=False)
        worker.process_once()
        self.now = self.minute + timedelta(minutes=2)
        worker.process_once()
        with self.sessions() as session:
            self.assertEqual(session.query(PowerReading).one().watts, 968535)
        self.assertEqual(worker.stats["spike"], 0)

    def test_commit_before_ack_replays_without_second_result(self) -> None:
        self.inbox.append(1, self.event(5, 1000))
        worker = self.worker()
        worker.process_once()
        self.now = self.minute + timedelta(minutes=2)
        with patch.object(worker._inbox, "ack", side_effect=RuntimeError("crash")):
            with self.assertRaisesRegex(RuntimeError, "crash"):
                worker.process_once()
        self.assertEqual(self.statuses(), ["in_flight"])

        restarted = self.worker()
        restarted.process_once()
        with self.sessions() as session:
            self.assertEqual(session.query(PowerReading).count(), 1)
            self.assertEqual(session.query(FinalizedPowerMinute).count(), 1)
            self.assertEqual(session.query(PowerMinuteCoverage).count(), 1)
        self.assertEqual(self.statuses(), ["acked"])

    def test_failed_application_commit_leaves_message_for_recovery(self) -> None:
        self.inbox.append(1, self.event(5, 1000))
        worker = self.worker()
        worker.process_once()
        self.now = self.minute + timedelta(minutes=2)
        with patch("sqlalchemy.orm.Session.commit", side_effect=RuntimeError("database down")):
            with self.assertRaisesRegex(RuntimeError, "database down"):
                worker.process_once()
        self.assertEqual(self.statuses(), ["in_flight"])
        with self.sessions() as session:
            self.assertEqual(session.query(PowerReading).count(), 0)
            self.assertEqual(session.query(PowerMinuteCoverage).count(), 0)

        restarted = self.worker()
        restarted.process_once()
        restarted.process_once()
        with self.sessions() as session:
            self.assertEqual(session.query(PowerReading).one().watts, 1000)
            self.assertEqual(session.query(PowerMinuteCoverage).count(), 1)
        self.assertEqual(self.statuses(), ["acked"])

    def test_late_and_future_samples_are_acked_without_results(self) -> None:
        self.inbox.append(1, self.event(5, 1000))
        self.inbox.append(1, self.event(180, 1000))
        with closing(sqlite3.connect(self.inbox_path)) as db:
            with db:
                db.execute("UPDATE inbox SET received_at = ? WHERE id = 1",
                           ((self.minute + timedelta(minutes=2)).isoformat(),))
        worker = self.worker()
        worker.process_once()
        self.assertEqual(worker.stats["late"], 1)
        self.assertEqual(worker.stats["future_timestamp"], 1)
        self.assertEqual(self.statuses(), ["acked", "acked"])
        with self.sessions() as session:
            self.assertEqual(session.query(PowerReading).count(), 0)

    def test_backlog_finalizes_older_minute_before_drain(self) -> None:
        first = self.minute - timedelta(minutes=2)
        second = first + timedelta(minutes=1)
        self.minute = first
        self.inbox.append(1, self.event(5, 1000))
        self.minute = second
        self.inbox.append(1, self.event(5, 3000))
        with closing(sqlite3.connect(self.inbox_path)) as db:
            with db:
                db.execute("UPDATE inbox SET received_at = ? WHERE id = 1",
                           ((first + timedelta(seconds=20)).isoformat(),))
                db.execute("UPDATE inbox SET received_at = ? WHERE id = 2",
                           ((second + timedelta(seconds=35)).isoformat(),))
        self.now = second + timedelta(minutes=3)
        worker = self.worker()
        worker.process_once()
        self.assertEqual(self.statuses(), ["acked", "in_flight"])
        worker.process_once()
        with self.sessions() as session:
            self.assertEqual(session.query(PowerReading).count(), 2)
        self.assertEqual(self.statuses(), ["acked", "acked"])

    def test_claim_batches_do_not_finalize_an_open_minute(self) -> None:
        for index in range(501):
            event = json.loads(self.event(5, 1000))
            event["observed_at"] = (self.minute + timedelta(seconds=5,
                                    microseconds=index)).isoformat()
            self.inbox.append(1, json.dumps(event).encode())
        worker = self.worker()
        self.assertEqual(worker.process_once(), 500)
        self.assertEqual(self.statuses().count("in_flight"), 500)
        self.assertEqual(worker.process_once(), 1)
        self.now = self.minute + timedelta(minutes=2)
        worker.process_once()
        with self.sessions() as session:
            self.assertEqual(session.query(PowerReading).one().watts, 1000)
        self.assertEqual(self.statuses(), ["acked"] * 501)

    def test_cleanup_only_removes_old_acknowledged_messages(self) -> None:
        self.inbox.append(1, b"old")
        self.inbox.append(1, b"pending")
        self.inbox.ack([1])
        with closing(sqlite3.connect(self.inbox_path)) as db:
            with db:
                db.execute("UPDATE inbox SET acked_at = '2020-01-01T00:00:00+00:00' WHERE id = 1")
        self.assertEqual(self.inbox.cleanup(self.now - timedelta(days=1)), 1)
        self.assertEqual(self.statuses(), ["pending"])

    def test_snapshot_uses_one_minute_and_reports_retained_outcomes(self) -> None:
        previous = (self.minute - timedelta(minutes=1)).replace(tzinfo=None)
        minute = self.minute.replace(tzinfo=None)
        with self.sessions() as session:
            session.add(Suite(id=1, datacenter_id=1, name="Hall A"))
            session.add(PowerDevice(id=2, datacenter_id=1, suite_id=1,
                                    device_type="rack_pdu", name="device 2", capacity_limit_kw=15))
            session.add(PowerDevice(id=3, datacenter_id=1, suite_id=1,
                                    device_type="rack_pdu", name="device 3", capacity_limit_kw=15))
            session.add_all([
                Space(id=1, suite_id=1, row="AA", position=1, power_device_id=1),
                Space(id=2, suite_id=1, row="AA", position=2, power_device_id=2),
                Space(id=3, suite_id=1, row="AA", position=3, power_device_id=3),
                Rack(id=1, suite_id=1, space_id=1, name="rack 1"),
                Rack(id=2, suite_id=1, space_id=2, name="rack 2"),
                Rack(id=3, suite_id=1, space_id=3, name="rack 3"),
                FinalizedPowerMinute(datacenter_id=1, timestamp=minute),
                PowerMinuteCoverage(datacenter_id=1, timestamp=minute,
                                    sample_counts={2: 1}),
                PowerReading(device_id=1, timestamp=minute, watts=16000),
                PowerReading(device_id=2, timestamp=previous, watts=1000),
            ])
            session.commit()
        self.inbox.append(1, b"bad")
        self.inbox.ack([1], "invalid")
        self.inbox.append(1, b"waiting")
        with self.sessions() as session, patch("api.routers.dashboard.SQLiteInbox",
                                                return_value=self.inbox):
            result = snapshot(1, session)
        self.assertEqual([rack["load_kw"] for rack in result["racks"]], [16, None, None])
        self.assertEqual([rack["last_reading_at"] for rack in result["racks"][1:]],
                         [previous.isoformat() + "Z", None])
        self.assertEqual(result["inbox"]["counts"], {"invalid": 1, "pending": 1})
        self.assertEqual(result["minute"], self.minute.isoformat().replace("+00:00", "Z"))
        self.assertEqual(result["sample_counts"], {"2": 1})
        with self.sessions() as session:
            PowerMinuteCoverage.__table__.drop(bind=session.get_bind())
        with self.sessions() as session, patch("api.routers.dashboard.SQLiteInbox",
                                                return_value=self.inbox):
            self.assertIsNone(snapshot(1, session)["sample_counts"])

    def test_existing_inbox_keeps_frames_when_result_column_is_added(self) -> None:
        path = self.inbox_path.parent / "legacy.db"
        with closing(sqlite3.connect(path)) as db:
            with db:
                db.execute("""CREATE TABLE inbox (
                    id INTEGER PRIMARY KEY, datacenter_id INTEGER NOT NULL,
                    payload BLOB NOT NULL, received_at TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending', acked_at TEXT
                )""")
                db.execute("""INSERT INTO inbox VALUES
                    (1, 1, X'6F6C64', '2026-01-01T00:00:00+00:00', 'acked',
                     '2026-01-01T00:00:00+00:00')""")
        upgraded = SQLiteInbox(path)
        self.assertEqual(upgraded.summary(1)["counts"], {"legacy": 1})
        upgraded.append(1, b"new frame")
        upgraded.ack([2], "invalid")
        self.assertEqual(upgraded.summary(1)["counts"], {"legacy": 1, "invalid": 1})


if __name__ == "__main__":
    unittest.main()
