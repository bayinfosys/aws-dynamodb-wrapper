"""Tests for DynamoDB backend using moto."""
import unittest
from moto import mock_aws
import boto3
from pydantic import BaseModel
from typing import ClassVar

from dynawrap.dbitem import DBItem
from dynawrap.backends.dynamodb import DynamoDBBackend


class Story(DBItem, BaseModel):
    """Test model for backend tests."""
    pk_pattern : ClassVar[str] = "USER#{owner}"
    sk_pattern: ClassVar[str] = "STORY#{story_id}"
    
    owner: str
    story_id: str
    title: str
    content: str = ""


@mock_aws
class TestDynamoDBBackend(unittest.TestCase):
    """Test DynamoDB backend with moto mocking."""
    
    def setUp(self):
        """Create DynamoDB table and backend for each test."""
        # Create mocked DynamoDB client
        self.client = boto3.client('dynamodb', region_name='us-east-1')
        
        # Create test table
        self.table_name = 'test_stories'
        self.client.create_table(
            TableName=self.table_name,
            KeySchema=[
                {'AttributeName': 'PK', 'KeyType': 'HASH'},
                {'AttributeName': 'SK', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'PK', 'AttributeType': 'S'},
                {'AttributeName': 'SK', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        
        # Create backend
        self.backend = DynamoDBBackend(self.client)
        
        # Create test item
        self.story = Story(
            owner="alice",
            story_id="123",
            title="Test Story",
            content="Once upon a time..."
        )
    
    def test_save_and_get(self):
        """Test saving and retrieving an item."""
        # Save
        self.backend.save(self.table_name, self.story)
        
        # Get
        retrieved = self.backend.get(
            self.table_name, 
            Story, 
            owner="alice", 
            story_id="123"
        )
        
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.owner, "alice")
        self.assertEqual(retrieved.story_id, "123")
        self.assertEqual(retrieved.title, "Test Story")
        self.assertEqual(retrieved.content, "Once upon a time...")
    
    def test_get_not_found(self):
        """Test getting an item that doesn't exist."""
        result = self.backend.get(
            self.table_name, 
            Story, 
            owner="bob", 
            story_id="999"
        )
        
        self.assertIsNone(result)
    
    def test_query_by_owner(self):
        """Test querying items by partition key."""
        # Save multiple stories
        stories = [
            Story(owner="alice", story_id="1", title="Story 1"),
            Story(owner="alice", story_id="2", title="Story 2"),
            Story(owner="alice", story_id="3", title="Story 3"),
            Story(owner="bob", story_id="1", title="Bob's Story"),
        ]
        
        for story in stories:
            self.backend.save(self.table_name, story)
        
        # Query alice's stories
        results = list(self.backend.query(self.table_name, Story, owner="alice"))
        
        self.assertEqual(len(results), 3)
        for story in results:
            self.assertEqual(story.owner, "alice")
    
    def test_query_with_sk_prefix(self):
        """Test querying with partial sort key."""
        # Save stories
        stories = [
            Story(owner="alice", story_id="100", title="Story 100"),
            Story(owner="alice", story_id="101", title="Story 101"),
            Story(owner="alice", story_id="200", title="Story 200"),
        ]
        
        for story in stories:
            self.backend.save(self.table_name, story)
        
        # Query with SK prefix (stories starting with "10")
        results = list(self.backend.query(
            self.table_name, 
            Story, 
            owner="alice", 
            story_id="10"
        ))
        
        self.assertEqual(len(results), 2)
        self.assertIn("100", [s.story_id for s in results])
        self.assertIn("101", [s.story_id for s in results])
    
    def test_query_with_limit(self):
        """Test querying with a limit."""
        # Save multiple stories
        for i in range(10):
            story = Story(owner="alice", story_id=str(i), title=f"Story {i}")
            self.backend.save(self.table_name, story)
        
        # Query with limit
        results = list(self.backend.query(
            self.table_name, 
            Story, 
            owner="alice", 
            limit=5
        ))
        
        self.assertEqual(len(results), 5)
    
    def test_query_reverse_order(self):
        """Test querying in reverse order."""
        # Save stories
        stories = [
            Story(owner="alice", story_id="1", title="Story 1"),
            Story(owner="alice", story_id="2", title="Story 2"),
            Story(owner="alice", story_id="3", title="Story 3"),
        ]
        
        for story in stories:
            self.backend.save(self.table_name, story)
        
        # Query in reverse
        results = list(self.backend.query(
            self.table_name, 
            Story, 
            owner="alice", 
            reverse=True
        ))
        
        self.assertEqual(results[0].story_id, "3")
        self.assertEqual(results[-1].story_id, "1")
    
    def test_delete(self):
        """Test deleting an item."""
        # Save
        self.backend.save(self.table_name, self.story)
        
        # Verify it exists
        retrieved = self.backend.get(
            self.table_name, 
            Story, 
            owner="alice", 
            story_id="123"
        )
        self.assertIsNotNone(retrieved)
        
        # Delete
        self.backend.delete(self.table_name, self.story)
        
        # Verify it's gone
        retrieved = self.backend.get(
            self.table_name, 
            Story, 
            owner="alice", 
            story_id="123"
        )
        self.assertIsNone(retrieved)
    
    def test_batch_write(self):
        """Test batch writing multiple items."""
        stories = [
            Story(owner="alice", story_id=str(i), title=f"Story {i}")
            for i in range(30)
        ]
        
        # Batch write
        self.backend.batch_write(self.table_name, stories)
        
        # Verify all items were written
        results = list(self.backend.query(self.table_name, Story, owner="alice"))
        self.assertEqual(len(results), 30)
    
    def test_schema_version_preserved(self):
        """Test that schema_version is preserved during save/load."""
        story = Story(
            owner="alice",
            story_id="123",
            title="Test",
            schema_version="custom_version"
        )
        
        self.backend.save(self.table_name, story)
        retrieved = self.backend.get(
            self.table_name, 
            Story, 
            owner="alice", 
            story_id="123"
        )
        
        self.assertEqual(retrieved.schema_version, "custom_version")

if __name__ == '__main__':
    unittest.main()
