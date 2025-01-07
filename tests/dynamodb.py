import unittest
from unittest.mock import MagicMock, patch
from src.dynamodb import AccessPattern, DynamodbWrapper, DBItem


# Define test DBItem subclasses
class TestStory(DBItem):
    table_name = "TestStoryTable"
    pk_pattern = "USER#{owner}#STORY#{story_id}"
    sk_pattern = "STORY#{story_id}"


class TestMetrics(DBItem):
    table_name = "TestMetricsTable"
    pk_pattern = "USER#{username}"
    sk_pattern = "DATE#{date}#EXECUTION#{execution_id}"


class TestDynamodbUtilities(unittest.TestCase):
    """Unit tests for dynamodb utilities."""

    def test_access_pattern_generation(self):
        """Test AccessPattern generates keys correctly."""
        pattern = AccessPattern("test_pattern", "USER#{owner}#ITEM#{item_id}")
        generated = pattern.generate(owner="johndoe", item_id="1234")
        self.assertEqual(generated, "USER#johndoe#ITEM#1234")

    @patch("src.dynamodb.boto3.resource")
    def test_dynamodb_wrapper_initialization(self, mock_boto_resource):
        """Test DynamoDBWrapper initialization and table name usage."""
        mock_table = MagicMock()
        mock_boto_resource.return_value.Table.return_value = mock_table

        # Initialize the wrapper
        wrapper = DynamodbWrapper(TestStory)

        self.assertEqual(wrapper.table_name, "TestStoryTable")
        self.assertIn("TestStory_pk", wrapper.access_patterns)
        self.assertIn("TestStory_sk", wrapper.access_patterns)

    @patch("src.dynamodb.boto3.resource")
    def test_item_save(self, mock_boto_resource):
        """Test saving a DBItem."""
        mock_table = MagicMock()
        mock_boto_resource.return_value.Table.return_value = mock_table

        # Initialize wrapper and item
        wrapper = DynamodbWrapper(TestStory)
        story = TestStory(wrapper)

        # Save data
        story_data = {"owner": "johndoe", "story_id": "1234", "title": "Test Story"}
        story.save(story_data)

        # Verify data passed to put_item
        mock_table.put_item.assert_called_once()
        put_params = mock_table.put_item.call_args[1]
        self.assertEqual(put_params["Item"]["PK"], "USER#johndoe#STORY#1234")
        self.assertEqual(put_params["Item"]["SK"], "STORY#1234")
        self.assertEqual(put_params["Item"]["title"], "Test Story")

    @patch("src.dynamodb.boto3.resource")
    def test_item_read(self, mock_boto_resource):
        """Test reading a DBItem."""
        mock_table = MagicMock()
        mock_boto_resource.return_value.Table.return_value = mock_table

        # Mock DynamoDB response
        mock_table.get_item.return_value = {
            "Item": {
                "PK": "USER#johndoe#STORY#1234",
                "SK": "STORY#1234",
                "title": "Test Story",
            }
        }

        # Initialize wrapper and read item
        wrapper = DynamodbWrapper(TestStory)
        story = TestStory.read(wrapper, owner="johndoe", story_id="1234")

        # Validate the retrieved data
        self.assertEqual(story.data["PK"], "USER#johndoe#STORY#1234")
        self.assertEqual(story.data["SK"], "STORY#1234")
        self.assertEqual(story.data["title"], "Test Story")
