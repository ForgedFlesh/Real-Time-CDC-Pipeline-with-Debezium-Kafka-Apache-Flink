from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


CONNECT_URL = os.getenv("CONNECT_URL", "http://localhost:8083")
CONFIG_PATH = Path(
    os.getenv(
        "CONNECTOR_CONFIG",
        str(Path(__file__).resolve().parents[1] / "debezium" / "mysql-shop-connector.json"),
    )
)


def session_with_retries() -> requests.Session:
    retry = Retry(
        total=10,
        connect=10,
        read=10,
        backoff_factor=1.0,
        status_forcelist=(408, 429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST", "PUT"}),
    )
    session = requests.Session()
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def wait_for_connect(session: requests.Session, attempts: int = 60) -> None:
    for attempt in range(1, attempts + 1):
        try:
            response = session.get(CONNECT_URL, timeout=5)
            if response.ok:
                print(f"Kafka Connect is ready: {response.json()}")
                return
        except requests.RequestException:
            pass
        print(f"Waiting for Kafka Connect ({attempt}/{attempts})...")
        time.sleep(2)
    raise RuntimeError("Kafka Connect did not become ready.")


def load_config() -> tuple[str, dict[str, Any]]:
    payload = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    name = payload["name"]
    config = payload["config"]
    return name, config


def main() -> int:
    session = session_with_retries()
    wait_for_connect(session)

    name, config = load_config()
    url = f"{CONNECT_URL}/connectors/{name}/config"

    response = session.put(url, json=config, timeout=30)
    if not response.ok:
        print(response.text, file=sys.stderr)
        response.raise_for_status()

    print(json.dumps(response.json(), indent=2))

    status_url = f"{CONNECT_URL}/connectors/{name}/status"
    for attempt in range(1, 31):
        status_response = session.get(status_url, timeout=10)

        # Registration may succeed before the status endpoint is ready.
        if status_response.status_code == 404:
            print(f"Connector status not ready yet ({attempt}/30)")
            time.sleep(2)
            continue

        status_response.raise_for_status()

        status = status_response.json()
        connector_state = status.get("connector", {}).get("state")
        task_states = [task.get("state") for task in status.get("tasks", [])]

        print(
            f"Connector state={connector_state}, task states={task_states} "
            f"({attempt}/30)"
        )

        if connector_state == "RUNNING" and task_states and all(
            state == "RUNNING" for state in task_states
        ):
            return 0

        if connector_state == "FAILED" or "FAILED" in task_states:
            print(json.dumps(status, indent=2), file=sys.stderr)
            return 1

        time.sleep(2)


if __name__ == "__main__":
    raise SystemExit(main())
