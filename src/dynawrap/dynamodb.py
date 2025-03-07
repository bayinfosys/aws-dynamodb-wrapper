import logging
import string

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from boto3.dynamodb.conditions import Key


logger = logging.getLogger(__name__)


class DynamodbWrapper:
    """Encapsulates DynamoDB get/put access and key management.

    Each `DynamodbWrapper` instance operates on a single DynamoDB table,
    specified during initialization.

    Attributes:
        table_name (str): The name of the DynamoDB table this wrapper operates on.

    Methods:
        key(key_type, **kwargs): Generates a key string based on the specified access pattern.
        create_item_key(pk_pattern_name, sk_pattern_name, **kwargs): Generates PK and SK keys.
        _insert_item_base(item, condition_expression=None): Inserts an item into the table.
        upsert_item(pk, sk, item, condition_expression=None): Upserts an item into the table.
        get_item_from_db(item_key): Retrieves an item by its key.
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

    def __init__(self, table_name: str, endpoint_url=None):
        """
        Initializes the DynamoDB wrapper.
        """
        self.dynamodb = boto3.resource("dynamodb", endpoint_url=endpoint_url)
        self.client = boto3.client("dynamodb", endpoint_url=endpoint_url)
        self.table_name = table_name

    def key(self, key_pattern, **kwargs):
        """Generates a key string based on the specified access pattern.

        Args:
            key_type (str): The name of the access pattern.
            **kwargs: Values to replace the placeholders in the pattern.

        Returns:
            str: The generated key string.
        """
        try:
            return key_pattern.format(**kwargs)
        except KeyError as e:
            # key error is allowed because we retry with a prefix
            raise e

    def prefix_key(self, key_pattern: str, **kwargs):
        """Dynamically generate an SK prefix by trimming the last missing variable.
        Do not include the latter placeholder key in kwargs, and this will generate
        a prefix search.

        Args:
            key_pattern (str): The key pattern with placeholders.
            **kwargs: Key-value pairs to populate the SK sans last placeholder

        Returns:
            str: The SK prefix (truncated if necessary).
        """
        formatter = string.Formatter()
        parsed_fields = list(formatter.parse(key_pattern))

        if not parsed_fields:
            return self.key(key_pattern, **kwargs)

        # TODO: starting from the end, work backwards until we find a placeholder in kwargs
        # NB: this is a little hardcoded for integer format strings
        last_placeholder = ":".join(list(parsed_fields)[-1][1:3])  # last placeholder wuth format string

        # Check if the last placeholder is missing in kwargs
        if last_placeholder not in kwargs:
            prefix_pattern = key_pattern.split(f"{{{last_placeholder}}}")[0]
            return self.key(prefix_pattern, **kwargs)
        else:
            logger.warning("all placeholders found in kwargs")

        # If all placeholders are present, format normally
        return self.key(key_pattern, **kwargs)

    def create_item_key(self, pk_pattern, sk_pattern, **kwargs):
        """Generates PK and SK keys based on specified access patterns.

        if the kwargs for sk are not all present, the method attempts to build a prefix sk

        Args:
            pk_pattern_name (str): Name of the PK access pattern.
            sk_pattern_name (str): Name of the SK access pattern.
            **kwargs: Values to replace placeholders in the patterns.

        Returns:
            dict: A dictionary containing the generated PK and SK keys.
        """
        pk = self.key(pk_pattern, **kwargs)

        # if the sk is not fully define, attempt to build a prefix sk
        try:
            sk = self.key(sk_pattern, **kwargs)
        except KeyError:
            sk = self.prefix_key(sk_pattern, **kwargs)

        return {"PK": pk, "SK": sk}

    def _insert_item_base(self, item: dict, condition_expression=None):
        """Inserts an item into the DynamoDB table.
        NB: `item` is just splatted into the table.put_item function so attributes
            should be named fields in the dict; extra attributes will be inserted into
            the db as fields.
            All strings.
        """
        table = self.dynamodb.Table(self.table_name)
        put_params = {"Item": item}

        if condition_expression:
            put_params.update({"ConditionExpression": condition_expression})

        try:
            logger.debug(f"Inserted item with PK: {item['PK']} and SK: {item['SK']}")
            table.put_item(**put_params)
        except self.dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            logger.warning(f"PK: {item['PK']}, SK: {item['SK']} already exists.")
        except (BotoCoreError, ClientError) as error:
            logger.error(f"Error inserting item into DynamoDB: {error}")

    def upsert_item(
        self,
        pk_pattern: str,
        sk_pattern: str,
        **kwargs,
    ):
        """Upserts an item into the DynamoDB table.

        the PK and SK are generated and added to the `item` dict
        `item` is then written to the database
        """
        item_key = self.create_item_key(pk_pattern, sk_pattern, **kwargs)
        item = dict(**kwargs or {}, **item_key)
        self._insert_item_base(item)

    def get_item_from_db(self, item_key: dict):
        """Retrieves an item from the DynamoDB table by its key.

        NB: the raw `Item` field is returned from the response with all the ["PK"]["S"] adornments
        """
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

    @classmethod
    def deserialize_db_item(cls, item_data):
        """convert the db annotated item to python dict"""
        try:
            # remove the ["S"] typing information
            d = boto3.dynamodb.types.TypeDeserializer()
            item = {k: d.deserialize(v) for k, v in item_data.items()}
            return item
        except Exception as e:
            logger.exception("failed to deserialize '%s' [%s]", str(item_data), str(e))
            return None

    def get_items_by_prefix(self, item_key: dict, count_only=False):
        """Fetch all records where SK starts with a given prefix.

        Args:
            pk_value (str): The exact PK value to match.
            sk_prefix (str): The SK prefix to match.

        Returns:
            list[dict]: List of matching items.
        """
        table = self.dynamodb.Table(self.table_name)
        condition = Key("PK").eq(item_key["PK"]) & Key("SK").begins_with(item_key["SK"])
        select = "COUNT" if count_only else "ALL_ATTRIBUTES"

        try:
            response = table.query(KeyConditionExpression=condition, Select=select)
        except (BotoCoreError, ClientError) as error:
            logger.error(f"Error fetching items by prefix: {error}")
            return 0 if count_only else []

        if count_only:
            return response.get("Count", 0)
        else:
            return response.get("Items", [])

    #
    # item interface
    #
    def save(self, item, **kwargs):
        """saves the current object's data to DynamoDB."""
        try:
            self.upsert_item(item.pk_pattern, item.sk_pattern, **kwargs)
        except TypeError as e:
            logger.exception("failed to pass PK=%s, SK=%s, '%s' [%s]", item.pk_pattern, item.sk_pattern, str(kwargs), str(e))
            raise

    def read(self, item_cls, **kwargs):
        """reads an item from DynamoDB and returns a new instance."""
        item_key = self.create_item_key(item_cls.pk_pattern, item_cls.sk_pattern, **kwargs)
        item_data = self.get_item_from_db(item_key)

        if not item_data:
            raise ValueError(f"No item found for key: {item_key}")

        return item_cls(**item_data)
