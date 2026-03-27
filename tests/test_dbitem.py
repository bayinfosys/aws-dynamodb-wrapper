"""Tests for DBItem base class methods."""

import unittest
from typing import ClassVar

from pydantic import BaseModel

from dynawrap.dbitem import DBItem


class Story(DBItem, BaseModel):
    pk_pattern: ClassVar[str] = "USER#{owner}"
    sk_pattern: ClassVar[str] = "STORY#{story_id}"

    owner: str
    story_id: str
    title: str
    content: str = ""


class MockItem(DBItem, BaseModel):
    pk_pattern: ClassVar[str] = "ITEM#{item_id}"
    sk_pattern: ClassVar[str] = "DETAIL#{detail_id}"

    item_id: str | None = None
    detail_id: str | None = None
    name: str | None = None


class TestDBItemSerialisation(unittest.TestCase):

    def test_to_dict(self):
        story = Story(owner="alice", story_id="123", title="Test Story", content="Content")
        data = story.to_dict()
        self.assertEqual(data["owner"], "alice")
        self.assertEqual(data["story_id"], "123")
        self.assertEqual(data["title"], "Test Story")
        self.assertEqual(data["content"], "Content")
        self.assertIn("schema_version", data)
        self.assertNotIn("PK", data)
        self.assertNotIn("SK", data)

    def test_from_dict(self):
        data = {"owner": "alice", "story_id": "123", "title": "Test Story", "content": "Content"}
        story = Story.from_dict(data)
        self.assertEqual(story.owner, "alice")
        self.assertEqual(story.story_id, "123")
        self.assertEqual(story.title, "Test Story")


class TestDBItemKeys(unittest.TestCase):

    def test_format_key(self):
        self.assertEqual(MockItem.format_key("ITEM#{item_id}", item_id="123"), "ITEM#123")

    def test_format_key_missing_field(self):
        with self.assertRaises(KeyError):
            MockItem.format_key("ITEM#{item_id}", wrong_arg="oops")

    def test_partial_key_prefix_full(self):
        prefix = MockItem.partial_key_prefix("DETAIL#{detail_id}", detail_id="123")
        self.assertEqual(prefix, "DETAIL#123")

    def test_partial_key_prefix_partial(self):
        result = MockItem.partial_key_prefix("DETAIL#{detail_id}", item_id="abc")
        self.assertEqual(result, "DETAIL#")

    def test_create_item_key_complete(self):
        keys = MockItem.create_item_key(item_id="x", detail_id="y")
        self.assertEqual(keys, {"PK": "ITEM#x", "SK": "DETAIL#y"})

    def test_create_item_key_partial(self):
        keys = MockItem.create_item_key(item_id="x")
        self.assertEqual(keys, {"PK": "ITEM#x", "SK": "DETAIL#"})

    def test_create_item_key_story(self):
        key = Story.create_item_key(owner="alice", story_id="123")
        self.assertEqual(key["PK"], "USER#alice")
        self.assertEqual(key["SK"], "STORY#123")

    def test_partial_key_prefix_story_full(self):
        prefix = Story.partial_key_prefix(Story.sk_pattern, story_id="123")
        self.assertEqual(prefix, "STORY#123")

    def test_partial_key_prefix_story_empty(self):
        prefix = Story.partial_key_prefix(Story.sk_pattern)
        self.assertEqual(prefix, "STORY#")

    def test_is_match_true(self):
        self.assertTrue(MockItem.is_match("ITEM#a", "DETAIL#b"))

    def test_is_match_false(self):
        self.assertFalse(MockItem.is_match("OTHER#1", "DETAIL#b"))

    def test_is_match_story_true(self):
        self.assertTrue(Story.is_match("USER#alice", "STORY#123"))

    def test_is_match_story_false_pk(self):
        self.assertFalse(Story.is_match("WRONG#alice", "STORY#123"))

    def test_is_match_story_false_sk(self):
        self.assertFalse(Story.is_match("USER#alice", "WRONG#123"))

    def test_repr(self):
        inst = MockItem(item_id="1", detail_id="2", name="x")
        rep = repr(inst)
        self.assertIn("ITEM#1", rep)
        self.assertIn("DETAIL#2", rep)
        self.assertIn("MockItem", rep)


if __name__ == "__main__":
    unittest.main()
