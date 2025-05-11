import unittest
from typing import ClassVar
from dynawrap import DBItem


class TestDBItemFromStream(unittest.TestCase):
    def setUp(self):
        class SignupItem(DBItem):
            pk_pattern: ClassVar[str] = "USER#SIGNUP"
            sk_pattern: ClassVar[str] = "TS#{timestamp}"

            def __init__(self, username=None, timestamp=None, email=None):
                self.username = username
                self.timestamp = timestamp
                self.email = email

        class StoryItem(DBItem):
            pk_pattern: ClassVar[str] = "USER#{owner}#STORY"
            sk_pattern: ClassVar[str] = "TS#{created_at}"

            def __init__(self, owner=None, created_at=None, title=None):
                self.owner = owner
                self.created_at = created_at
                self.title = title

        class UserComment(DBItem):
            pk_pattern: ClassVar[str] = "USER#COMMENT"
            sk_pattern: ClassVar[str] = "REF#{item_ref}#USER#{username}#TS#{timestamp}"

            def __init__(
                self,
                trace_id=None,
                item_ref=None,
                event_name=None,
                comment=None,
                ttl=None,
                timestamp=None,
                username=None,
            ):
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
                "expected_dict": {
                    "username": "tester-001",
                    "timestamp": "2025-05-04T22:57:47",
                    "email": "tester@example.com",
                },
                "expected_keys": {
                    "PK": "USER#SIGNUP",
                    "SK": "TS#2025-05-04T22:57:47",
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
                "expected_dict": {
                    "owner": "alice",
                    "created_at": "2025-05-10T15:30:00",
                    "title": "A Cool Story",
                },
                "expected_keys": {
                    "PK": "USER#alice#STORY",
                    "SK": "TS#2025-05-10T15:30:00",
                },
            },
            {
                "cls": UserComment,
                "stream_record": {
                    "dynamodb": {
                        "NewImage": {
                            "PK": {"S": "USER#COMMENT"},
                            "SK": {
                                "S": "REF#78d5e580-968e-4f37-a116-d29ad988cc80#USER#tester-002#TS#1746990301418"
                            },
                            "trace_id": {
                                "S": "c8e9f5d3-224d-48b3-a9ec-4766d869c417"
                            },
                            "item_ref": {
                                "S": "78d5e580-968e-4f37-a116-d29ad988cc80"
                            },
                            "event_name": {"S": "event.user.comment"},
                            "comment": {"S": "there we are then"},
                            "ttl": {"N": "1749582301"},
                            "timestamp": {"N": "1746990301418"},
                            "username": {"S": "tester-002"},
                        }
                    }
                },
                "expected_dict": {
                    "trace_id": "c8e9f5d3-224d-48b3-a9ec-4766d869c417",
                    "item_ref": "78d5e580-968e-4f37-a116-d29ad988cc80",
                    "event_name": "event.user.comment",
                    "comment": "there we are then",
                    "ttl": 1749582301,
                    "timestamp": 1746990301418,
                    "username": "tester-002",
                },
                "expected_keys": {
                    "PK": "USER#COMMENT",
                    "SK": "REF#78d5e580-968e-4f37-a116-d29ad988cc80#USER#tester-002#TS#1746990301418",
                },
            },
        ]

    def test_from_stream_record(self):
        for case in self.test_cases:
            with self.subTest(cls=case["cls"].__name__):
                obj = case["cls"].from_stream_record(case["stream_record"])
                for field, expected_value in case["expected_dict"].items():
                    self.assertEqual(getattr(obj, field), expected_value)

    def test_deserialize_user_fields_only(self):
        for case in self.test_cases:
            with self.subTest(cls=case["cls"].__name__):
                cls = case["cls"]
                deserialized = cls.deserialize_db_item(
                    case["stream_record"]["dynamodb"]["NewImage"]
                )
                filtered = {k: deserialized[k] for k in case["expected_dict"]}
                self.assertEqual(filtered, case["expected_dict"])

    def test_deserialize_includes_correct_keys(self):
        for case in self.test_cases:
            with self.subTest(cls=case["cls"].__name__):
                cls = case["cls"]
                deserialized = cls.deserialize_db_item(
                    case["stream_record"]["dynamodb"]["NewImage"]
                )
                for key, value in case["expected_keys"].items():
                    self.assertEqual(deserialized[key], value, f"{key} mismatch")
