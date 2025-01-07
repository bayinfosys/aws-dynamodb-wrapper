import unittest
from src.dynamodb import AccessPattern, DynamodbWrapper, DBItemMeta, DBItem


# Define test DBItem subclasses
class TestStory(DBItem):
    table_name = "TestStoryTable"
    pk_pattern = "USER#{owner}#STORY#{story_id}"
    sk_pattern = "STORY#{story_id}"


class TestMetrics(DBItem):
    table_name = "TestMetricsTable"
    pk_pattern = "USER#{username}"
    sk_pattern = "DATE#{date}#EXECUTION#{execution_id}"


class TestAccessPatternRegistration(unittest.TestCase):
    """Test for ensuring AccessPatterns are registered correctly by DBItemMeta."""

    def test_access_pattern_registration(self):
        """Verify that access patterns are registered correctly."""
        # Retrieve all registered access patterns
        access_patterns = DBItemMeta.get_access_patterns()

        # Check if TestStory's patterns are registered
        self.assertIn("TestStory_pk", access_patterns)
        self.assertIn("TestStory_sk", access_patterns)
        self.assertEqual(
            access_patterns["TestStory_pk"].pattern, "USER#{owner}#STORY#{story_id}"
        )
        self.assertEqual(access_patterns["TestStory_sk"].pattern, "STORY#{story_id}")

        # Check if TestMetrics's patterns are registered
        self.assertIn("TestMetrics_pk", access_patterns)
        self.assertIn("TestMetrics_sk", access_patterns)
        self.assertEqual(access_patterns["TestMetrics_pk"].pattern, "USER#{username}")
        self.assertEqual(
            access_patterns["TestMetrics_sk"].pattern,
            "DATE#{date}#EXECUTION#{execution_id}",
        )

    def test_access_pattern_usage_in_dynamodb_wrapper(self):
        """Verify that DynamodbWrapper uses the registered access patterns."""
        # Initialize DynamoDB wrapper for TestStory
        wrapper = DynamodbWrapper(TestStory)

        # Verify that access patterns are available in the wrapper
        self.assertIn("TestStory_pk", wrapper.access_patterns)
        self.assertIn("TestStory_sk", wrapper.access_patterns)

        # Generate keys using the patterns
        pk = wrapper.key("TestStory_pk", owner="johndoe", story_id="1234")
        sk = wrapper.key("TestStory_sk", story_id="1234")

        # Check that the generated keys match the expected values
        self.assertEqual(pk, "USER#johndoe#STORY#1234")
        self.assertEqual(sk, "STORY#1234")

        # Initialize DynamoDB wrapper for TestMetrics
        wrapper_metrics = DynamodbWrapper(TestMetrics)

        # Verify that access patterns are available in the wrapper
        self.assertIn("TestMetrics_pk", wrapper_metrics.access_patterns)
        self.assertIn("TestMetrics_sk", wrapper_metrics.access_patterns)

        # Generate keys using the patterns
        pk_metrics = wrapper_metrics.key("TestMetrics_pk", username="johndoe")
        sk_metrics = wrapper_metrics.key(
            "TestMetrics_sk", date="2023-01-05", execution_id="5678"
        )

        # Check that the generated keys match the expected values
        self.assertEqual(pk_metrics, "USER#johndoe")
        self.assertEqual(sk_metrics, "DATE#2023-01-05#EXECUTION#5678")


if __name__ == "__main__":
    unittest.main()
