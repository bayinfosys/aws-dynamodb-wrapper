"""Tests for DynamoDB backend using moto."""

import unittest
from typing import ClassVar

import boto3
from boto3.dynamodb.types import TypeSerializer
from moto import mock_aws
from pydantic import BaseModel

from dynawrap.backends.dynamodb import DynamoDBBackend
from dynawrap.dbitem import DBItem


class Story(DBItem, BaseModel):
    pk_pattern: ClassVar[str] = "USER#{owner}"
    sk_pattern: ClassVar[str] = "STORY#{story_id}"

    owner: str
    story_id: str
    title: str
    content: str = ""


@mock_aws
class TestDynamoDBBackend(unittest.TestCase):

    def setUp(self):
        self.client = boto3.client("dynamodb", region_name="us-east-1")
        self.table_name = "test_stories"
        self.client.create_table(
            TableName=self.table_name,
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        self.backend = DynamoDBBackend(self.client)
        self.story = Story(
            owner="alice",
            story_id="123",
            title="Test Story",
            content="Once upon a time...",
        )

    def test_save_and_get(self):
        self.backend.save(self.table_name, self.story)
        retrieved = self.backend.get(
            self.table_name, Story, owner="alice", story_id="123"
        )
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.owner, "alice")
        self.assertEqual(retrieved.story_id, "123")
        self.assertEqual(retrieved.title, "Test Story")
        self.assertEqual(retrieved.content, "Once upon a time...")

    def test_get_not_found(self):
        result = self.backend.get(
            self.table_name, Story, owner="bob", story_id="999"
        )
        self.assertIsNone(result)

    def test_query_by_owner(self):
        stories = [
            Story(owner="alice", story_id="1", title="Story 1"),
            Story(owner="alice", story_id="2", title="Story 2"),
            Story(owner="alice", story_id="3", title="Story 3"),
            Story(owner="bob", story_id="1", title="Bob's Story"),
        ]
        for story in stories:
            self.backend.save(self.table_name, story)

        results = list(self.backend.query(self.table_name, Story, owner="alice"))
        self.assertEqual(len(results), 3)
        for story in results:
            self.assertEqual(story.owner, "alice")

    def test_query_with_sk_prefix(self):
        stories = [
            Story(owner="alice", story_id="100", title="Story 100"),
            Story(owner="alice", story_id="101", title="Story 101"),
            Story(owner="alice", story_id="200", title="Story 200"),
        ]
        for story in stories:
            self.backend.save(self.table_name, story)

        results = list(
            self.backend.query(self.table_name, Story, owner="alice", story_id="10")
        )
        self.assertEqual(len(results), 2)
        self.assertIn("100", [s.story_id for s in results])
        self.assertIn("101", [s.story_id for s in results])

    def test_query_with_limit(self):
        for i in range(10):
            self.backend.save(
                self.table_name,
                Story(owner="alice", story_id=str(i), title=f"Story {i}"),
            )
        results = list(
            self.backend.query(self.table_name, Story, owner="alice", limit=5)
        )
        self.assertEqual(len(results), 5)

    def test_query_reverse_order(self):
        stories = [
            Story(owner="alice", story_id="1", title="Story 1"),
            Story(owner="alice", story_id="2", title="Story 2"),
            Story(owner="alice", story_id="3", title="Story 3"),
        ]
        for story in stories:
            self.backend.save(self.table_name, story)

        results = list(
            self.backend.query(self.table_name, Story, owner="alice", reverse=True)
        )
        self.assertEqual(results[0].story_id, "3")
        self.assertEqual(results[-1].story_id, "1")

    def test_delete(self):
        self.backend.save(self.table_name, self.story)
        retrieved = self.backend.get(
            self.table_name, Story, owner="alice", story_id="123"
        )
        self.assertIsNotNone(retrieved)

        self.backend.delete(self.table_name, self.story)
        retrieved = self.backend.get(
            self.table_name, Story, owner="alice", story_id="123"
        )
        self.assertIsNone(retrieved)

    def test_batch_write(self):
        stories = [
            Story(owner="alice", story_id=str(i), title=f"Story {i}")
            for i in range(30)
        ]
        self.backend.batch_write(self.table_name, stories)
        results = list(self.backend.query(self.table_name, Story, owner="alice"))
        self.assertEqual(len(results), 30)

    def test_schema_version_preserved(self):
        story = Story(
            owner="alice",
            story_id="123",
            title="Test",
            schema_version="custom_version",
        )
        self.backend.save(self.table_name, story)
        retrieved = self.backend.get(
            self.table_name, Story, owner="alice", story_id="123"
        )
        self.assertEqual(retrieved.schema_version, "custom_version")


class TestDynamoDBBackendStreams(unittest.TestCase):
    """Tests for from_stream_record. No AWS calls - deserialisation only."""

    def setUp(self):
        self.client = boto3.client("dynamodb", region_name="us-east-1")
        self.backend = DynamoDBBackend(self.client)
        self.serializer = TypeSerializer()

        # Plain DBItem subclass without pydantic - manual __init__
        class SignupItem(DBItem):
            pk_pattern: ClassVar[str] = "USER#SIGNUP"
            sk_pattern: ClassVar[str] = "TS#{timestamp}"

            def __init__(self, username=None, timestamp=None, email=None, **kwargs):
                self.username = username
                self.timestamp = timestamp
                self.email = email

        # Plain DBItem subclass with simple SK
        class StoryItem(DBItem):
            pk_pattern: ClassVar[str] = "USER#{owner}#STORY"
            sk_pattern: ClassVar[str] = "TS#{created_at}"

            def __init__(self, owner=None, created_at=None, title=None, **kwargs):
                self.owner = owner
                self.created_at = created_at
                self.title = title

        # Plain DBItem subclass with complex SK and numeric types
        class UserComment(DBItem):
            pk_pattern: ClassVar[str] = "USER#COMMENT"
            sk_pattern: ClassVar[str] = "REF#{item_ref}#USER#{username}#TS#{timestamp}"

            def __init__(self, trace_id=None, item_ref=None, event_name=None,
                         comment=None, ttl=None, timestamp=None, username=None, **kwargs):
                self.trace_id = trace_id
                self.item_ref = item_ref
                self.event_name = event_name
                self.comment = comment
                self.ttl = int(ttl) if ttl is not None else None
                self.timestamp = int(timestamp) if timestamp is not None else None
                self.username = username

        self.test_cases = [
            {
                "cls": SignupItem,
                "stream_record": {
                    "dynamodb": {
                        "NewImage": {
                            "PK": {"S": "USER#SIGNUP"},
                            "SK": {"S": "TS#2025-05-04T22:57:47"},
                            "username": {"S": "tester-001"},
                            "timestamp": {"S": "2025-05-04T22:57:47"},
                            "email": {"S": "tester@example.com"},
                        }
                    }
                },
                "expected": {
                    "username": "tester-001",
                    "timestamp": "2025-05-04T22:57:47",
                    "email": "tester@example.com",
                },
            },
            {
                "cls": StoryItem,
                "stream_record": {
                    "dynamodb": {
                        "NewImage": {
                            "PK": {"S": "USER#alice#STORY"},
                            "SK": {"S": "TS#2025-05-10T15:30:00"},
                            "owner": {"S": "alice"},
                            "created_at": {"S": "2025-05-10T15:30:00"},
                            "title": {"S": "A Cool Story"},
                        }
                    }
                },
                "expected": {
                    "owner": "alice",
                    "created_at": "2025-05-10T15:30:00",
                    "title": "A Cool Story",
                },
            },
            {
                "cls": UserComment,
                "stream_record": {
                    "dynamodb": {
                        "NewImage": {
                            "PK": {"S": "USER#COMMENT"},
                            "SK": {"S": "REF#78d5e580-968e-4f37-a116-d29ad988cc80#USER#tester-002#TS#1746990301418"},
                            "trace_id": {"S": "c8e9f5d3-224d-48b3-a9ec-4766d869c417"},
                            "item_ref": {"S": "78d5e580-968e-4f37-a116-d29ad988cc80"},
                            "event_name": {"S": "event.user.comment"},
                            "comment": {"S": "there we are then"},
                            "ttl": {"N": "1749582301"},
                            "timestamp": {"N": "1746990301418"},
                            "username": {"S": "tester-002"},
                        }
                    }
                },
                "expected": {
                    "trace_id": "c8e9f5d3-224d-48b3-a9ec-4766d869c417",
                    "item_ref": "78d5e580-968e-4f37-a116-d29ad988cc80",
                    "event_name": "event.user.comment",
                    "comment": "there we are then",
                    "ttl": 1749582301,
                    "timestamp": 1746990301418,
                    "username": "tester-002",
                },
            },
        ]

    def _make_record(self, data):
        """Build a stream record from plain dict values using TypeSerializer."""
        return {
            "dynamodb": {
                "NewImage": {k: self.serializer.serialize(v) for k, v in data.items()}
            }
        }

    def test_from_stream_record_varied_models(self):
        """Test deserialisation across model types including numeric fields and complex SKs."""
        for case in self.test_cases:
            with self.subTest(cls=case["cls"].__name__):
                obj = self.backend.from_stream_record(case["stream_record"], case["cls"])
                for field, expected in case["expected"].items():
                    self.assertEqual(getattr(obj, field), expected)

    def test_from_stream_record_pydantic_model(self):
        record = self._make_record({
            "PK": "USER#alice", "SK": "STORY#123",
            "owner": "alice", "story_id": "123", "title": "Streamed", "content": ""
        })
        result = self.backend.from_stream_record(record, Story)
        self.assertIsInstance(result, Story)
        self.assertEqual(result.owner, "alice")
        self.assertEqual(result.title, "Streamed")

    def test_from_stream_record_missing_pk(self):
        record = self._make_record({
            "SK": "STORY#123",
            "owner": "alice", "story_id": "123", "title": "NoPK"
        })
        with self.assertRaises(ValueError):
            self.backend.from_stream_record(record, Story)

    def test_from_stream_record_only_keys(self):
        record = self._make_record({"PK": "USER#alice", "SK": "STORY#123"})
        with self.assertRaises(ValueError):
            self.backend.from_stream_record(record, Story)

    def test_from_stream_record_non_matching_pattern(self):
        record = self._make_record({
            "PK": "WRONG#alice", "SK": "STORY#123",
            "owner": "alice", "story_id": "123", "title": "BadMatch"
        })
        with self.assertRaises(ValueError):
            self.backend.from_stream_record(record, Story)


if __name__ == "__main__":
    unittest.main()
