import unittest
from unittest.mock import MagicMock
from typing import ClassVar
from boto3.dynamodb.types import TypeSerializer
from pydantic import BaseModel

from dynawrap import DBItem


class MockItem(DBItem, BaseModel):
    pk_pattern: ClassVar[str] = "ITEM#{item_id}"
    sk_pattern: ClassVar[str] = "DETAIL#{detail_id}"

    item_id: str | None = None
    detail_id: str | None = None
    name: str | None = None


class TestDBItemCore(unittest.TestCase):
    def test_format_key(self):
        self.assertEqual(
            MockItem.format_key("ITEM#{item_id}", item_id="123"), "ITEM#123"
        )

    def test_format_key_missing_field(self):
        with self.assertRaises(KeyError):
            MockItem.format_key("ITEM#{item_id}", wrong_arg="oops")

    def test_partial_key_prefix(self):
        result = MockItem.partial_key_prefix("DETAIL#{detail_id}", item_id="abc")
        self.assertEqual(result, "DETAIL#")

    def test_create_item_key_complete(self):
        keys = MockItem.create_item_key(item_id="x", detail_id="y")
        self.assertEqual(keys, {"PK": "ITEM#x", "SK": "DETAIL#y"})

    def test_create_item_key_partial(self):
        keys = MockItem.create_item_key(item_id="x")
        self.assertEqual(keys, {"PK": "ITEM#x", "SK": "DETAIL#"})

    def test_is_match_true(self):
        self.assertTrue(MockItem.is_match("ITEM#a", "DETAIL#b"))

    def test_is_match_false(self):
        self.assertFalse(MockItem.is_match("OTHER#1", "DETAIL#b"))

    def test_from_dynamo_item(self):
        item = {
            "PK": "ITEM#1",
            "SK": "DETAIL#2",
            "item_id": "1",
            "detail_id": "2",
            "name": "test",
        }
        obj = MockItem.from_dynamo_item(item)
        self.assertIsInstance(obj, MockItem)
        self.assertEqual(obj.name, "test")

    def test_to_dynamo_item(self):
        inst = MockItem(item_id="a", detail_id="b", name="Zed")
        item = inst.to_dynamo_item()
        self.assertEqual(item["PK"], "ITEM#a")
        self.assertEqual(item["SK"], "DETAIL#b")
        self.assertEqual(item["name"], "Zed")

    def test_repr(self):
        inst = MockItem(item_id="1", detail_id="2", name="x")
        rep = repr(inst)
        self.assertIn("ITEM#1", rep)
        self.assertIn("DETAIL#2", rep)

    def test_read_success(self):
        mock_table = MagicMock()
        mock_table.get_item.return_value = {
            "Item": {
                "PK": "ITEM#1",
                "SK": "DETAIL#2",
                "item_id": "1",
                "detail_id": "2",
                "name": "Alpha",
            }
        }
        result = MockItem.read(mock_table, item_id="1", detail_id="2")
        self.assertIsInstance(result, MockItem)
        self.assertEqual(result.name, "Alpha")

    def test_read_not_found(self):
        mock_table = MagicMock()
        mock_table.get_item.return_value = {}

        with self.assertRaises(KeyError):
            MockItem.read(mock_table, item_id="missing")

    def test_format_key_missing_field(self):
        with self.assertRaises(KeyError):
            MockItem.format_key("ITEM#{item_id}", wrong_arg="oops")

    def test_partial_key_prefix_with_all_fields(self):
        prefix = MockItem.partial_key_prefix("DETAIL#{detail_id}", detail_id="123")
        self.assertEqual(prefix, "DETAIL#123")

    def test_repr_contains_class_name(self):
        inst = MockItem(item_id="10", detail_id="20", name="X")
        self.assertIn("MockItem", repr(inst))

    def test_from_dynamo_item_with_extra_fields(self):
        item = {
            "PK": "ITEM#1",
            "SK": "DETAIL#2",
            "item_id": "1",
            "detail_id": "2",
            "name": "Extra",
            "extra_field": "ignored",
        }
        result = MockItem.from_dynamo_item(item)
        self.assertIsInstance(result, MockItem)
        self.assertFalse(hasattr(result, "extra_field"))

    def test_from_stream_record_valid(self):
        serializer = TypeSerializer()
        record = {
            "dynamodb": {
                "NewImage": {
                    "PK": serializer.serialize("ITEM#1"),
                    "SK": serializer.serialize("DETAIL#2"),
                    "item_id": serializer.serialize("1"),
                    "detail_id": serializer.serialize("2"),
                    "name": serializer.serialize("Streamed"),
                }
            }
        }
        result = MockItem.from_stream_record(record)
        self.assertIsInstance(result, MockItem)
        self.assertEqual(result.name, "Streamed")

    def test_from_stream_record_missing_pk(self):
        serializer = TypeSerializer()
        record = {
            "dynamodb": {
                "NewImage": {
                    "SK": serializer.serialize("DETAIL#2"),
                    "item_id": serializer.serialize("1"),
                    "detail_id": serializer.serialize("2"),
                    "name": serializer.serialize("MissingPK"),
                }
            }
        }
        with self.assertRaises(ValueError):
            MockItem.from_stream_record(record)

    def test_from_stream_record_only_keys(self):
        serializer = TypeSerializer()
        record = {
            "dynamodb": {
                "NewImage": {
                    "PK": serializer.serialize("ITEM#1"),
                    "SK": serializer.serialize("DETAIL#2"),
                }
            }
        }
        with self.assertRaises(ValueError):
            MockItem.from_stream_record(record)

    def test_from_stream_record_non_matching_pattern(self):
        serializer = TypeSerializer()
        record = {
            "dynamodb": {
                "NewImage": {
                    "PK": serializer.serialize("WRONG#1"),
                    "SK": serializer.serialize("DETAIL#2"),
                    "item_id": serializer.serialize("1"),
                    "detail_id": serializer.serialize("2"),
                    "name": serializer.serialize("BadMatch"),
                }
            }
        }
        with self.assertRaises(ValueError):
            MockItem.from_stream_record(record)
