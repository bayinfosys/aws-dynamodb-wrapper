import unittest
from typing import ClassVar
from dynawrap import DBItem


class TestDBItemFromStream(unittest.TestCase):
    class X(DBItem):
        pk_pattern: ClassVar[str] = "USER#SIGNUP"
        sk_pattern: ClassVar[str] = "TS#{timestamp}"

        def __init__(self, username=None, timestamp=None, email=None):
            self.username = username
            self.timestamp = timestamp
            self.email = email

    def setUp(self):
        self.stream_record = {
            "eventID": "81193db2e4b50f75909a915b236d96ac",
            "eventName": "INSERT",
            "eventVersion": "1.1",
            "eventSource": "aws:dynamodb",
            "awsRegion": "eu-west-2",
            "dynamodb": {
                "ApproximateCreationDateTime": 1746399467.0,
                "Keys": {
                    "SK": {"S": "TS#2025-05-04T22:57:47"},
                    "PK": {"S": "USER#SIGNUP"},
                },
                "NewImage": {
                    "SK": {"S": "TS#2025-05-04T22:57:47"},
                    "PK": {"S": "USER#SIGNUP"},
                    "email": {"S": "tester@example.com"},
                    "timestamp": {"S": "2025-05-04T22:57:47"},
                    "username": {"S": "tester-001"},
                },
                "SequenceNumber": "7416300002271126548794192",
                "SizeBytes": 143,
                "StreamViewType": "NEW_IMAGE",
            },
            "eventSourceARN": "arn:aws:dynamodb:eu-west-2:192117775384:table/popstory-fm-dev-events/stream/2025-05-03T08:57:00.290",
        }

        self.expected_dict = {
            "email": "tester@example.com",
            "timestamp": "2025-05-04T22:57:47",
            "username": "tester-001"
        }

        self.expected_keys = {
            "PK": "USER#SIGNUP",
            "SK": "TS#2025-05-04T22:57:47"
        }

    def test_from_stream_record(self):
        obj = self.X.from_stream_record(self.stream_record)
        self.assertEqual(obj.timestamp, self.expected_dict["timestamp"])
        self.assertEqual(obj.username, self.expected_dict["username"])
        self.assertEqual(obj.email, self.expected_dict["email"])

    def test_deserialize_user_fields_only(self):
        deserialized = self.X.deserialize_db_item(self.stream_record["dynamodb"]["NewImage"])
        filtered = {k: deserialized[k] for k in self.expected_dict}
        self.assertEqual(filtered, self.expected_dict)

    def test_deserialize_includes_correct_keys(self):
        deserialized = self.X.deserialize_db_item(self.stream_record["dynamodb"]["NewImage"])
        for key, value in self.expected_keys.items():
            self.assertEqual(deserialized[key], value, f"{key} mismatch")
