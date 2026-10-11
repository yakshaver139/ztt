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

from db.models import Base, Datacenter, FinalizedPowerMinute, Owner, PowerDevice, PowerReading
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

    def worker(self) -> PowerWorker:
        return PowerWorker(1, SQLiteInbox(self.inbox_path), self.sessions,
                           clock=lambda: self.now)

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

        restarted = self.worker()
        restarted.process_once()
        restarted.process_once()
        with self.sessions() as session:
            self.assertEqual(session.query(PowerReading).one().watts, 1000)
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


if __name__ == "__main__":
    unittest.main()
