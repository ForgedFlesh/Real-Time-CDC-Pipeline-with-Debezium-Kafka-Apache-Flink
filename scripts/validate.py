from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pymysql
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


@dataclass(frozen=True)
class Config:
    mysql_host: str = os.getenv("MYSQL_HOST", "localhost")
    mysql_port: int = int(os.getenv("MYSQL_PORT", "3306"))
    mysql_db: str = os.getenv("MYSQL_DATABASE", "shop")
    mysql_user: str = os.getenv("MYSQL_APP_USER", "appuser")
    mysql_password: str = os.getenv("MYSQL_APP_PASSWORD", "apppw")

    pg_host: str = os.getenv("POSTGRES_HOST", "localhost")
    pg_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    pg_db: str = os.getenv("POSTGRES_DB", "analytics")
    pg_user: str = os.getenv("POSTGRES_USER", "postgres")
    pg_password: str = os.getenv("POSTGRES_PASSWORD", "postgres")


BUSINESS_COLUMNS = (
    "order_id",
    "customer_id",
    "status",
    "amount",
    "currency",
    "shipping_address",
    "created_at",
    "updated_at",
)


def normalize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.isoformat(timespec="milliseconds")
    if isinstance(value, date):
        return value.isoformat()
    return value


def row_to_tuple(row: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(normalize(row[column]) for column in BUSINESS_COLUMNS)


def fetch_mysql_rows(config: Config) -> dict[int, tuple[Any, ...]]:
    connection = pymysql.connect(
        host=config.mysql_host,
        port=config.mysql_port,
        user=config.mysql_user,
        password=config.mysql_password,
        database=config.mysql_db,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT order_id, customer_id, status, amount, currency,
                       shipping_address, created_at, updated_at
                FROM orders
                ORDER BY order_id
                """
            )
            return {
                int(row["order_id"]): row_to_tuple(row)
                for row in cursor.fetchall()
            }
    finally:
        connection.close()


def fetch_postgres_rows(
    config: Config,
) -> tuple[dict[int, tuple[Any, ...]], dict[str, int]]:
    connection = psycopg.connect(
        host=config.pg_host,
        port=config.pg_port,
        dbname=config.pg_db,
        user=config.pg_user,
        password=config.pg_password,
        autocommit=True,
    )
    try:
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT order_id, customer_id, status, amount, currency,
                       shipping_address, created_at, updated_at
                FROM orders_current
                ORDER BY order_id
                """
            )
            target = {
                int(row["order_id"]): row_to_tuple(row)
                for row in cursor.fetchall()
            }

            cursor.execute("SELECT COUNT(*) AS count FROM orders_valid_current")
            valid_target_count = int(cursor.fetchone()["count"])

            cursor.execute("SELECT COUNT(*) AS count FROM cdc_rejects")
            reject_count = int(cursor.fetchone()["count"])

            cursor.execute("SELECT COUNT(*) AS count FROM cdc_event_audit")
            audit_count = int(cursor.fetchone()["count"])

            cursor.execute("SELECT COUNT(*) AS count FROM cdc_raw_events")
            raw_event_count = int(cursor.fetchone()["count"])

            return target, {
                "valid_target_count": valid_target_count,
                "reject_count": reject_count,
                "audit_count": audit_count,
                "raw_event_count": raw_event_count,
            }
    finally:
        connection.close()


def compare(
    source: dict[int, tuple[Any, ...]],
    target: dict[int, tuple[Any, ...]],
) -> dict[str, Any]:
    source_ids = set(source)
    target_ids = set(target)

    missing = sorted(source_ids - target_ids)
    extra = sorted(target_ids - source_ids)
    mismatches = {
        order_id: {
            "source": source[order_id],
            "target": target[order_id],
        }
        for order_id in sorted(source_ids & target_ids)
        if source[order_id] != target[order_id]
    }

    return {
        "source_count": len(source),
        "target_count": len(target),
        "missing_ids": missing,
        "extra_ids": extra,
        "mismatches": mismatches,
    }


def write_result(
    config: Config,
    result: dict[str, Any],
    metrics: dict[str, int],
) -> None:
    passed = (
        not result["missing_ids"]
        and not result["extra_ids"]
        and not result["mismatches"]
    )
    status = "PASS" if passed else "FAIL"

    connection = psycopg.connect(
        host=config.pg_host,
        port=config.pg_port,
        dbname=config.pg_db,
        user=config.pg_user,
        password=config.pg_password,
        autocommit=True,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO pipeline_reconciliation (
                    source_count,
                    target_count,
                    valid_target_count,
                    missing_in_target,
                    extra_in_target,
                    mismatched_rows,
                    reject_count,
                    audit_count,
                    raw_event_count,
                    status,
                    details
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    result["source_count"],
                    result["target_count"],
                    metrics["valid_target_count"],
                    len(result["missing_ids"]),
                    len(result["extra_ids"]),
                    len(result["mismatches"]),
                    metrics["reject_count"],
                    metrics["audit_count"],
                    metrics["raw_event_count"],
                    status,
                    Jsonb(
                        {
                            "missing_ids": result["missing_ids"][:100],
                            "extra_ids": result["extra_ids"][:100],
                            "mismatch_ids": list(result["mismatches"])[:100],
                        }
                    ),
                ),
            )
    finally:
        connection.close()


def main() -> int:
    config = Config()
    timeout_seconds = int(os.getenv("VALIDATION_TIMEOUT_SECONDS", "90"))
    poll_seconds = int(os.getenv("VALIDATION_POLL_SECONDS", "3"))
    deadline = time.time() + timeout_seconds
    last_result: dict[str, Any] | None = None
    last_metrics: dict[str, int] | None = None

    while time.time() < deadline:
        source = fetch_mysql_rows(config)
        target, metrics = fetch_postgres_rows(config)
        result = compare(source, target)
        last_result, last_metrics = result, metrics

        if (
            not result["missing_ids"]
            and not result["extra_ids"]
            and not result["mismatches"]
        ):
            write_result(config, result, metrics)
            print(
                json.dumps(
                    {
                        **result,
                        **metrics,
                        "status": "PASS",
                    },
                    indent=2,
                    default=str,
                )
            )
            return 0

        print(
            "Waiting for source and target to converge: "
            f"missing={len(result['missing_ids'])}, "
            f"extra={len(result['extra_ids'])}, "
            f"mismatched={len(result['mismatches'])}"
        )
        time.sleep(poll_seconds)

    assert last_result is not None and last_metrics is not None
    write_result(config, last_result, last_metrics)
    print(
        json.dumps(
            {
                **last_result,
                **last_metrics,
                "status": "FAIL",
            },
            indent=2,
            default=str,
        ),
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
