"""
dynamodb_utilities

This module provides a utility framework for interacting with DynamoDB using 
an object-oriented approach. It includes functionality for dynamically generating 
DynamoDB keys based on access patterns and managing DynamoDB items as Python objects.

Features:
    - Encapsulates DynamoDB operations (get, put, update) through a `DynamodbWrapper`.
    - Supports custom table names for each `DBItem` subclass.
    - Uses `AccessPattern` to define reusable key generation logic.
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
import os
import logging

import boto3
from botocore.exceptions import BotoCoreError, ClientError


logger = logging.getLogger(__name__)


class AccessPattern:
    """A named access pattern for a PK/SK in a DynamoDB row.

    The pattern is a string with named placeholders, which are replaced by
    values from keyword arguments at runtime.

    Attributes:
        name (str): The name of the access pattern.
        pattern (str): The pattern string with placeholders.

    Methods:
        generate(**kwargs): Generates a key string by replacing placeholders with values.
    """

    def __init__(self, name, pattern):
        self.name = name
        self.pattern = pattern

    def generate(self, **kwargs):
        """Generates a key string by replacing placeholders in the pattern.

        Args:
            **kwargs: Keyword arguments for replacing placeholders.

        Returns:
            str: The generated key string.
        """
        return self.pattern.format(**kwargs)


class DynamodbWrapper:
    """Encapsulates DynamoDB get/put access and key management.

    Each `DynamodbWrapper` instance operates on a single DynamoDB table,
    specified during initialization. Subclasses of `DBItem` can also define
    their associated table via the `table_name` attribute.

    Attributes:
        table_name (str): The name of the DynamoDB table this wrapper operates on.
        access_patterns (dict): A dictionary of `AccessPattern` objects indexed by their names.

    Methods:
        key(key_type, **kwargs): Generates a key string based on the specified access pattern.
        create_item_key(pk_pattern_name, sk_pattern_name, **kwargs): Generates PK and SK keys.
        _insert_item_base(item, condition_expression=None): Inserts an item into the table.
        upsert_item(pk, sk, item, condition_expression=None): Upserts an item into the table.
        _get_item_from_db(item_key): Retrieves an item by its key.
    """
    @classmethod
    def get_table_spec(cls, table_name, gsi_name):
        """Returns the table specification for DynamoDB.
        TODO: multiple gsi, optional in spec depending on params
        """
        return {
            "TableName": table_name,
            "AttributeDefinitions": [
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
                {"AttributeName": "GSIPK", "AttributeType": "S"},
                {"AttributeName": "GSISK", "AttributeType": "N"},
            ],
            "KeySchema": [
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
            "GlobalSecondaryIndexes": [
                {
                    "IndexName": gsi_name,
                    "KeySchema": [
                        {"AttributeName": "GSIPK", "KeyType": "HASH"},
                        {"AttributeName": "GSISK", "KeyType": "RANGE"},
                    ],
                    "Projection": {
                        "ProjectionType": "KEYS_ONLY",
                    },
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": 1,
                        "WriteCapacityUnits": 1,
                    },
                },
            ],
        }

    def __init__(self, db_item_class):
        """
        Initializes the DynamoDB wrapper.

        Args:
            db_item_class (type): A `DBItem` subclass whose table and patterns to use.
        """
        assert issubclass(db_item_class, DBItem), "db_item_class must be a subclass of DBItem"
        self.table_name = db_item_class.table_name
        self.access_patterns = DBItemMeta.get_access_patterns()

        self.dynamodb = boto3.resource("dynamodb")
        self.client = boto3.client("dynamodb")


    def key(self, key_type, **kwargs):
        """Generates a key string based on the specified access pattern.

        Args:
            key_type (str): The name of the access pattern.
            **kwargs: Values to replace the placeholders in the pattern.

        Returns:
            str: The generated key string.
        """
        try:
            return self.access_patterns[key_type].generate(**kwargs)
        except KeyError as e:
            logger.error(
                "KeyError for 'key_type=%s', with kwargs='%s', access_pattern='%s'",
                str(key_type),
                str(kwargs),
                str(self.access_patterns.get(key_type, None)),
            )
            raise e

    def create_item_key(self, pk_pattern_name, sk_pattern_name, **kwargs):
        """Generates PK and SK keys based on specified access patterns.

        Args:
            pk_pattern_name (str): Name of the PK access pattern.
            sk_pattern_name (str): Name of the SK access pattern.
            **kwargs: Values to replace placeholders in the patterns.

        Returns:
            dict: A dictionary containing the generated PK and SK keys.
        """
        pk = self.key(pk_pattern_name, **kwargs)
        sk = self.key(sk_pattern_name, **kwargs)
        return {"PK": pk, "SK": sk}

    def _insert_item_base(self, item, table_name=None, condition_expression=None):
        """Inserts an item into the DynamoDB table."""
        table = self.dynamodb.Table(self.table_name)
        put_params = {"Item": item}
        if condition_expression:
            put_params.update({"ConditionExpression": condition_expression})

        try:
            logger.debug(f"Inserted item with PK: {item['PK']} and SK: {item['SK']}")
            table.put_item(**put_params)
        except self.dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            logger.warning(f"Item with PK: {item['PK']} and SK: {item['SK']} already exists.")
        except (BotoCoreError, ClientError) as error:
            logger.error(f"Error inserting item into DynamoDB: {error}")

    def upsert_item(self, pk_name, sk_name, item, table_name=None, condition_expression=None):
        """Upserts an item into the DynamoDB table."""
        assert pk_name in self.access_patterns
        assert sk_name in self.access_patterns
        item_key = self.create_item_key(pk_name, sk_name, **item)
        item.update(item_key)
        self._insert_item_base(item)

    def _get_item_from_db(self, item_key, table_name=None):
        """Retrieves an item from the DynamoDB table by its key."""
        table = self.dynamodb.Table(self.table_name)
        try:
            response = table.get_item(Key=item_key)
            if "Item" not in response:
                logger.error(f"Item '{item_key}' not found.")
                return None
            return response["Item"]
        except (BotoCoreError, ClientError) as error:
            logger.error(f"Error retrieving item from DynamoDB: {error}")
            return None


class DBItemMeta(type):
    """Metaclass for DBItem to automatically register AccessPatterns.
    injects the pk_name and sk_name variables into the sub-class
    """

    _access_patterns = {}

    def __new__(cls, name, bases, dct):
        pk_pattern = dct.get("pk_pattern")
        sk_pattern = dct.get("sk_pattern")
        table_name = dct.get("table_name")

        # Register patterns and set names
        if pk_pattern:
            pk_name = f"{name}_pk"
            DBItemMeta._access_patterns[pk_name] = AccessPattern(name=pk_name, pattern=pk_pattern)
            dct["pk_name"] = pk_name

        if sk_pattern:
            sk_name = f"{name}_sk"
            DBItemMeta._access_patterns[sk_name] = AccessPattern(name=sk_name, pattern=sk_pattern)
            dct["sk_name"] = sk_name

        if name != "DBItem" and not table_name:
            raise ValueError(f"Class {name} must define a table_name.")

        return super().__new__(cls, name, bases, dct)

    @classmethod
    def get_access_patterns(cls):
        """Retrieves all registered AccessPatterns."""
        return cls._access_patterns


class DBItem(metaclass=DBItemMeta):
    """Base class for a DynamoDB row item.

    Subclasses define their table-specific `table_name`, `pk_pattern`, and `sk_pattern`.
    Each `DBItem` interacts with DynamoDB via a `DynamodbWrapper`.

    Attributes:
        table_name (str): The name of the DynamoDB table associated with this item.
        pk_pattern (str): The primary key pattern for the item.
        sk_pattern (str): The sort key pattern for the item.

    Example:

        class Story(DBItem):
            table_name = "StoryTable"
            pk_pattern = "USER#{owner}#STORY#{story_id}"
            sk_pattern = "STORY#{story_id}"

        # Initialize the DynamoDB wrapper
        patterns = [
            AccessPattern("story_pk", "USER#{owner}#STORY#{story_id}"),
            AccessPattern("story_sk", "STORY#{story_id}")
        ]
        db_wrapper = DynamodbWrapper("StoryTable", access_patterns=patterns)

        # Save a story
        story = Story(db_wrapper)
        story.data = {"owner": "johndoe", "story_id": "1234", "title": "My Story"}
        story.save()

        # Read a story
        retrieved_story = Story.read(db_wrapper, owner="johndoe", story_id="1234")
        print(retrieved_story.data)
    """
    table_name = None  # Specify the table name here
    pk_pattern = None
    sk_pattern = None

    def __init__(self, db_wrapper):
        self.db_wrapper = db_wrapper

    @classmethod
    def get_table_name(cls):
        """Returns the table name for this item."""
        return cls.table_name

    def save(self, data):
        """Saves the current object's data to DynamoDB."""
        self.db_wrapper.upsert_item(self.pk_name, self.sk_name, data)

    @classmethod
    def read(cls, db_wrapper, **kwargs):
        """Reads an item from DynamoDB and returns a new instance."""
        item_key = db_wrapper.create_item_key(cls.pk_name, cls.sk_name, **kwargs)
        item_data = db_wrapper._get_item_from_db(item_key)

        if not item_data:
            raise ValueError(f"No item found for key: {item_key}")

        instance = cls(db_wrapper)
        instance.data = item_data
        return instance

