"""Tests for PostgreSQL backend using mocked psycopg2 connection."""
import json
import unittest
from typing import ClassVar
from unittest.mock import MagicMock, call, patch

from pydantic import BaseModel

from dynawrap.backends.postgres import PostgresBackend
from dynawrap.dbitem import DBItem


class Story(DBItem, BaseModel):
    pk_pattern: ClassVar[str] = "USER#{owner}"
    sk_pattern: ClassVar[str] = "STORY#{story_id}"

    owner: str
    story_id: str
    title: str
    content: str = ""


def make_row(item):
    """Wrap a DBItem as a mock row with a data key, as psycopg2 RealDictCursor returns."""
    return {"data": item.to_dict()}


class TestPostgresBackend(unittest.TestCase):

    def setUp(self):
        self.conn = MagicMock()
        self.cursor = MagicMock()
        self.conn.cursor.return_value.__enter__ = MagicMock(return_value=self.cursor)
        self.conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        self.backend = PostgresBackend(self.conn)
        self.table = "test_stories"
        self.story = Story(owner="alice", story_id="123", title="Test Story", content="Once upon a time...")

    def test_save_executes_insert(self):
        self.backend.save(self.table, self.story)
        self.cursor.execute.assert_called_once()
        sql, params = self.cursor.execute.call_args[0]
        self.assertIn("INSERT INTO", sql)
        self.assertIn("ON CONFLICT", sql)
        self.assertEqual(params[0], "USER#alice")
        self.assertEqual(params[1], "STORY#123")
        self.conn.commit.assert_called_once()

    def test_save_serialises_data_as_json(self):
        self.backend.save(self.table, self.story)
        _, params = self.cursor.execute.call_args[0]
        data = json.loads(params[3])
        self.assertEqual(data["owner"], "alice")
        self.assertEqual(data["story_id"], "123")

    def test_get_returns_item(self):
        self.cursor.fetchone.return_value = make_row(self.story)
        result = self.backend.get(self.table, Story, owner="alice", story_id="123")
        self.assertIsNotNone(result)
        self.assertEqual(result.owner, "alice")
        self.assertEqual(result.story_id, "123")

    def test_get_returns_none_when_not_found(self):
        self.cursor.fetchone.return_value = None
        result = self.backend.get(self.table, Story, owner="bob", story_id="999")
        self.assertIsNone(result)

    def test_get_queries_correct_pk_sk(self):
        self.cursor.fetchone.return_value = None
        self.backend.get(self.table, Story, owner="alice", story_id="123")
        _, params = self.cursor.execute.call_args[0]
        self.assertEqual(params[0], "USER#alice")
        self.assertEqual(params[1], "STORY#123")

    def test_query_returns_items(self):
        rows = [make_row(Story(owner="alice", story_id=str(i), title=f"Story {i}")) for i in range(3)]
        self.cursor.fetchall.return_value = rows
        results = list(self.backend.query(self.table, Story, owner="alice"))
        self.assertEqual(len(results), 3)
        for r in results:
            self.assertEqual(r.owner, "alice")

    def test_query_with_sk_prefix(self):
        self.cursor.fetchall.return_value = []
        list(self.backend.query(self.table, Story, owner="alice", story_id="10"))
        sql, params = self.cursor.execute.call_args[0]
        self.assertIn("LIKE", sql)
        self.assertIn("STORY#10%", params)

    def test_query_with_limit(self):
        self.cursor.fetchall.return_value = []
        list(self.backend.query(self.table, Story, owner="alice", limit=5))
        sql, params = self.cursor.execute.call_args[0]
        self.assertIn("LIMIT", sql)
        self.assertIn(5, params)

    def test_query_reverse_order(self):
        self.cursor.fetchall.return_value = []
        list(self.backend.query(self.table, Story, owner="alice", reverse=True))
        sql, _ = self.cursor.execute.call_args[0]
        self.assertIn("DESC", sql)

    def test_query_raises_on_unresolvable_pk(self):
        with self.assertRaises(ValueError):
            list(self.backend.query(self.table, Story))

    def test_delete_executes_delete(self):
        self.backend.delete(self.table, self.story)
        sql, params = self.cursor.execute.call_args[0]
        self.assertIn("DELETE FROM", sql)
        self.assertEqual(params[0], "USER#alice")
        self.assertEqual(params[1], "STORY#123")
        self.conn.commit.assert_called_once()

    def test_save_overwrites_existing(self):
        self.backend.save(self.table, self.story)
        updated = self.story.model_copy(update={"title": "Updated"})
        self.backend.save(self.table, updated)
        self.assertEqual(self.conn.commit.call_count, 2)
        _, params = self.cursor.execute.call_args[0]
        data = json.loads(params[3])
        self.assertEqual(data["title"], "Updated")

    def test_schema_version_stored(self):
        self.backend.save(self.table, self.story)
        _, params = self.cursor.execute.call_args[0]
        self.assertIsNotNone(params[2])
        self.assertNotEqual(params[2], "")

    @patch("dynawrap.backends.postgres.psycopg2.extras.execute_values")
    def test_batch_write_commits_per_batch(self, mock_execute_values):
        stories = [Story(owner="alice", story_id=str(i), title=f"Story {i}") for i in range(5)]
        self.backend.batch_write(self.table, stories, batch_size=2)
        self.assertEqual(self.conn.commit.call_count, 3)
        self.assertEqual(mock_execute_values.call_count, 3)

    def test_create_table_executes_ddl(self):
        PostgresBackend.create_table(self.conn, "stories")
        cursor = self.conn.cursor.return_value.__enter__.return_value
        sql = cursor.execute.call_args[0][0]
        self.assertIn("CREATE TABLE IF NOT EXISTS", sql)
        self.assertIn("stories", sql)
        self.conn.commit.assert_called_once()


if __name__ == "__main__":
    unittest.main()
