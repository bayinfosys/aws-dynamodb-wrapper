"""PostgreSQL backend implementation."""

import json
import logging
from typing import Literal

import psycopg2
import psycopg2.extras

from .base import DBBackend

logger = logging.getLogger(__name__)


class PostgresBackend(DBBackend):
    """PostgreSQL backend using psycopg2.

    Uses a single table with (pk, sk), schema_version, and data (JSONB) columns.
    All model fields are stored in data. No migrations are required.

    Accepts a psycopg2 connection. Each operation creates its own cursor
    and commits immediately, mirroring the per-call semantics of the
    DynamoDB backend. Connection lifecycle is the caller's responsibility.

    Example:
        conn = psycopg2.connect(dsn)
        backend = PostgresBackend(conn)
        backend.save("stories", story)

    Table must be created before use:
        PostgresBackend.create_table(conn, "stories")
    """

    def __init__(self, conn):
        self._conn = conn

    def _cursor(self):
        return self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def _serialize(self, data: dict) -> str:
        """Serialise a dict to a JSON string for storage."""
        return json.dumps(data, default=str)

    def _deserialize(self, raw) -> dict:
        """Deserialise stored data back to a plain dict.

        psycopg2 automatically parses JSONB columns to dicts, but we
        accept either a dict or a string to handle both cases explicitly.
        """
        if isinstance(raw, dict):
            return raw
        return json.loads(raw)

    @staticmethod
    def create_table(conn, table_name):
        """Create the backend table and indexes if they do not exist.

        Idempotent - safe to call on every application startup.
        """
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    pk TEXT NOT NULL,
                    sk TEXT NOT NULL,
                    schema_version TEXT,
                    data JSONB NOT NULL,
                    PRIMARY KEY (pk, sk)
                );
                CREATE INDEX IF NOT EXISTS idx_{table_name}_pk ON {table_name} (pk);
                CREATE INDEX IF NOT EXISTS idx_{table_name}_sk ON {table_name} (sk);
            """
            )
        conn.commit()

    def save(self, table_name, item):
        """Insert or replace an item."""
        data = item.to_dict()
        key = item.create_item_key(**data)
        schema_version = data.get("schema_version", "")

        with self._cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO {table_name} (pk, sk, schema_version, data)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (pk, sk) DO UPDATE SET
                    schema_version = EXCLUDED.schema_version,
                    data = EXCLUDED.data
            """,
                (key["PK"], key["SK"], schema_version, self._serialize(data)),
            )

        self._conn.commit()
        logger.debug("Saved item PK=%s SK=%s", key["PK"], key["SK"])

    def get(self, table_name, item_class, **kwargs):
        """Get a single item by key. Returns None if not found.

        Raises:
            ValueError: If the key cannot be fully resolved from kwargs.
        """
        key = item_class.create_item_key(**kwargs)

        with self._cursor() as cursor:
            cursor.execute(
                f"""
                SELECT data FROM {table_name}
                WHERE pk = %s AND sk = %s
            """,
                (key["PK"], key["SK"]),
            )
            row = cursor.fetchone()

        if not row:
            return None

        return item_class.from_dict(self._deserialize(row["data"]))

    def query(
        self,
        table_name,
        item_class,
        limit=0,
        reverse=False,
        on_error: Literal["warn", "skip", "raise"] = "warn",
        **kwargs,
    ):
        """Query by PK with optional SK prefix. Yields DBItem instances.

        Raises:
            ValueError: If the PK cannot be fully resolved from kwargs.
        """
        try:
            pk_str = item_class.format_key(item_class.pk_pattern, **kwargs)
        except KeyError as e:
            raise ValueError(f"Cannot resolve PK for query: {e}")

        sk_prefix = item_class.partial_key_prefix(item_class.sk_pattern, **kwargs)
        order = "DESC" if reverse else "ASC"
        params = [pk_str]

        sk_clause = ""
        if sk_prefix:
            sk_clause = "AND sk LIKE %s"
            params.append(sk_prefix + "%")

        limit_clause = ""
        if limit > 0:
            limit_clause = "LIMIT %s"
            params.append(limit)

        with self._cursor() as cursor:
            cursor.execute(
                f"""
                SELECT data FROM {table_name}
                WHERE pk = %s {sk_clause}
                ORDER BY sk {order}
                {limit_clause}
            """,
                params,
            )
            rows = cursor.fetchall()

        for row in rows:
            try:
                yield item_class.from_dict(self._deserialize(row["data"]))
            except Exception as e:
                if on_error == "warn":
                    logger.warning("Failed to parse item as %s: %s", item_class, e)
                elif on_error == "skip":
                    continue
                elif on_error == "raise":
                    raise

    def delete(self, table_name, item):
        """Delete an item by key."""
        data = item.to_dict()
        key = item.create_item_key(**data)

        with self._cursor() as cursor:
            cursor.execute(
                f"""
                DELETE FROM {table_name} WHERE pk = %s AND sk = %s
            """,
                (key["PK"], key["SK"]),
            )

        self._conn.commit()
        logger.debug("Deleted item PK=%s SK=%s", key["PK"], key["SK"])

    def batch_write(self, table_name, items, batch_size=100):
        """Write multiple items using execute_values."""
        for i in range(0, len(items), batch_size):
            batch = items[i : i + batch_size]
            rows = []
            for item in batch:
                data = item.to_dict()
                key = item.create_item_key(**data)
                rows.append(
                    (
                        key["PK"],
                        key["SK"],
                        data.get("schema_version", ""),
                        self._serialize(data),
                    )
                )

            with self._cursor() as cursor:
                psycopg2.extras.execute_values(
                    cursor,
                    f"""
                    INSERT INTO {table_name} (pk, sk, schema_version, data)
                    VALUES %s
                    ON CONFLICT (pk, sk) DO UPDATE SET
                        schema_version = EXCLUDED.schema_version,
                        data = EXCLUDED.data
                """,
                    rows,
                )

            self._conn.commit()
            logger.debug("Batch wrote %d items to %s", len(batch), table_name)
