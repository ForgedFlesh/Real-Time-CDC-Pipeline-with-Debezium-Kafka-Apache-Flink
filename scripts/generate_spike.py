from __future__ import annotations

import argparse
import os
import random
from decimal import Decimal

import pymysql


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a burst of MySQL order CDC events."
    )
    parser.add_argument("--count", type=int, default=10000)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--update-percent", type=float, default=10.0)
    parser.add_argument("--delete-percent", type=float, default=2.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    connection = pymysql.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_APP_USER", "appuser"),
        password=os.getenv("MYSQL_APP_PASSWORD", "apppw"),
        database=os.getenv("MYSQL_DATABASE", "shop"),
        autocommit=False,
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COALESCE(MAX(order_id), 5000) FROM orders")
            start_id = int(cursor.fetchone()[0]) + 1

            rows = []
            for offset in range(args.count):
                order_id = start_id + offset
                customer_id = random.choice((1001, 1002, 1003))
                amount = Decimal(random.randint(100, 500000)) / Decimal("100")
                rows.append(
                    (
                        order_id,
                        customer_id,
                        random.choice(("CREATED", "PAID", "PROCESSING")),
                        amount,
                        "INR",
                        random.choice(("Kolkata", "Bengaluru", "Chennai", "Delhi")),
                    )
                )

                if len(rows) >= args.batch_size:
                    cursor.executemany(
                        """
                        INSERT INTO orders (
                            order_id, customer_id, status, amount,
                            currency, shipping_address
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        rows,
                    )
                    connection.commit()
                    print(f"Inserted through order_id={rows[-1][0]}")
                    rows.clear()

            if rows:
                cursor.executemany(
                    """
                    INSERT INTO orders (
                        order_id, customer_id, status, amount,
                        currency, shipping_address
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    rows,
                )
                connection.commit()

            update_count = int(args.count * args.update_percent / 100.0)
            delete_count = int(args.count * args.delete_percent / 100.0)

            if update_count:
                cursor.execute(
                    """
                    UPDATE orders
                    SET status = 'PAID',
                        updated_at = CURRENT_TIMESTAMP(3)
                    WHERE order_id >= %s
                      AND order_id < %s
                    """,
                    (start_id, start_id + update_count),
                )
                connection.commit()
                print(f"Updated {cursor.rowcount} rows.")

            if delete_count:
                cursor.execute(
                    """
                    DELETE FROM orders
                    WHERE order_id >= %s
                      AND order_id < %s
                    """,
                    (start_id + args.count - delete_count, start_id + args.count),
                )
                connection.commit()
                print(f"Deleted {cursor.rowcount} rows.")

        print("Spike generation complete.")
        return 0
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
