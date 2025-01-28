import logging

import boto3
from botocore.exceptions import BotoCoreError, ClientError


from .dbitemmeta import DBItemMeta
from .dbitem import DBItem


logger = logging.getLogger(__name__)


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

    def __init__(self, db_item_class, endpoint_url=None):
        """
        Initializes the DynamoDB wrapper.

        Args:
            db_item_class (type): A `DBItem` subclass whose table and patterns to use.
        """
        assert issubclass(
            db_item_class, DBItem
        ), "db_item_class must be a subclass of DBItem"
        self.table_name = db_item_class.table_name
        self.access_patterns = DBItemMeta.get_access_patterns()

        self.dynamodb = boto3.resource("dynamodb", endpoint_url=endpoint_url)
        self.client = boto3.client("dynamodb", endpoint_url=endpoint_url)

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
        pk_name: str,
        sk_name: str,
        item: dict,
        table_name: str = None,
        condition_expression=None,
    ):
        """Upserts an item into the DynamoDB table.

        the PK and SK are generated and added to the `item` dict
        `item` is then written to the database
        """
        assert pk_name in self.access_patterns
        assert sk_name in self.access_patterns
        item_key = self.create_item_key(pk_name, sk_name, **item)
        item.update(item_key)
        self._insert_item_base(item)

    def get_item_from_db(self, item_key: dict, table_name=None):
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
