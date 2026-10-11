from __future__ import annotations

import os

from ingestion.power_ingester import PowerIngester
from ingestion.stream_client import RealDatacenterStream


def main() -> None:
    token = os.getenv("BMS_TOKEN")
    if not token:
        raise SystemExit(
            "Set BMS_TOKEN (and optionally BMS_DATACENTER_ID / BMS_WS_URL) to connect "
            "to the telemetry publisher."
        )

    data_center_id = int(os.getenv("BMS_DATACENTER_ID", "1"))
    stream = RealDatacenterStream(os.getenv("BMS_WS_URL"))
    PowerIngester(stream, token=token, data_center_id=data_center_id).run()
    print(f"Stopped ingesting stream for data center {data_center_id}.")


if __name__ == "__main__":
    main()
