from __future__ import annotations

import math
import os
import signal
import time
from multiprocessing import get_context

from ingestion.power_ingester import PowerIngester, PowerWorker
from ingestion.stream_client import RealDatacenterStream, StreamAuthError
from lib.logging import get_logger


logger = get_logger(__name__)


def _stop(_signum, _frame) -> None:
    raise KeyboardInterrupt


def _collect(token: str, data_center_id: int, url: str | None) -> None:
    try:
        PowerIngester(RealDatacenterStream(url), token, data_center_id).run()
    except StreamAuthError as exc:
        logger.error("BMS authentication failed: %s", exc)
        raise SystemExit(2) from exc


def _work(data_center_id: int, lateness: float, retention: float) -> None:
    PowerWorker(data_center_id, lateness_seconds=lateness,
                retention_seconds=retention).run()


def main() -> None:
    token = os.getenv("BMS_TOKEN")
    if not token:
        raise SystemExit(
            "Set BMS_TOKEN (and optionally BMS_DATACENTER_ID / BMS_WS_URL) to connect "
            "to the telemetry publisher."
        )

    data_center_id = int(os.getenv("BMS_DATACENTER_ID", "1"))
    lateness = float(os.getenv("BMS_ALLOWED_LATENESS_SECONDS", "30"))
    retention = float(os.getenv("BMS_INBOX_RETENTION_HOURS", "1")) * 3600
    if not math.isfinite(lateness) or not math.isfinite(retention):
        raise SystemExit("lateness and retention must be finite")
    if lateness < 0 or retention <= 0:
        raise SystemExit("lateness must be nonnegative and retention must be positive")
    ctx = get_context("spawn")
    targets = {
        "collector": (_collect, (token, data_center_id, os.getenv("BMS_WS_URL"))),
        "worker": (_work, (data_center_id, lateness, retention)),
    }
    children = {name: ctx.Process(target=target, args=args, name=name)
                for name, (target, args) in targets.items()}
    previous_sigterm = signal.signal(signal.SIGTERM, _stop)
    previous_sigurg = signal.getsignal(signal.SIGURG)
    try:
        for child in children.values():
            child.start()
        # SIGURG is ignored by supervisors started before this handler existed.
        signal.signal(signal.SIGURG, lambda _signum, _frame: children["worker"].kill())
        while True:
            time.sleep(1)
            for name, child in list(children.items()):
                if child.exitcode is None:
                    continue
                child.join()
                if name == "collector" and child.exitcode == 2:
                    raise SystemExit("BMS authentication failed; collector stopped")
                logger.error("%s exited with code %s; restarting", name, child.exitcode)
                target, args = targets[name]
                replacement = ctx.Process(target=target, args=args, name=name)
                replacement.start()
                children[name] = replacement
    except KeyboardInterrupt:
        logger.info("stopping power ingestion")
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm)
        signal.signal(signal.SIGURG, previous_sigurg)
        for child in children.values():
            if child.pid is None:
                continue
            if child.is_alive():
                child.terminate()
            child.join(timeout=5)
            if child.is_alive():
                child.kill()
                child.join()


if __name__ == "__main__":
    main()
