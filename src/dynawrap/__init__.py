"""This module provides a utility framework for interacting with DynamoDB using
an object-oriented approach. It includes functionality for dynamically generating
DynamoDB keys based on access patterns and managing DynamoDB items as Python objects.

Features:
    - Encapsulates DynamoDB operations (get, put, update) through a `DynamodbWrapper`.
    - Supports custom table names for each `DBItem` subclass.
    - Defines `DBItem` as a base class for DynamoDB row objects.

Table Management:
    - Each `DynamodbWrapper` instance operates on a single DynamoDB table.
    - Subclasses of `DBItem` can specify their associated table via the `table_name` attribute.
    - Validation ensures that every `DBItem` subclass defines a valid `table_name`.

Example Usage:
  class Story(DBItem):
      table_name = "StoryTable"
      pk_pattern = "USER#{owner}#STORY#{story_id}"
      sk_pattern = "STORY#{story_id}"

  class Metrics(DBItem):
      table_name = "MetricsTable"
      pk_pattern = "USER#{username}"
      sk_pattern = "DATE#{date}#EXECUTION#{execution_id}"

  # Automatically register patterns and validate table_name
  db_wrapper_story = DynamodbWrapper(Story)
  db_wrapper_metrics = DynamodbWrapper(Metrics)

  # Save and read operations
  story = Story(db_wrapper_story)
  story.data = {"owner": "johndoe", "story_id": "1234", "title": "My Story"}
  story.save()

  retrieved_story = Story.read(db_wrapper_story, owner="johndoe", story_id="1234")
  print(retrieved_story.data)
"""
from .dbitem import DBItem
from .dynamodb import DynamodbWrapper
