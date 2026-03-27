"""DynamoDB backend implementation."""

import json
import logging

from .base import DBBackend

try:
    from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
except ImportError:
    TypeDeserializer = None
    TypeSerializer = None


logger = logging.getLogger(__name__)


class DynamoDBBackend(DBBackend):
    """DynamoDB backend using boto3 low-level client.

    This backend uses the boto3 client (not resource) interface.

    Example:
        client = boto3.client('dynamodb')
        backend = DynamoDBBackend(client)
        backend.save('my_table', story)
    """

    def __init__(self, client):
        """Initialize with a boto3 DynamoDB client.

        Args:
            client: boto3.client('dynamodb') instance
        """
        if TypeSerializer is None:
            raise ImportError("boto3 is required for DynamoDB backend")

        self.client = client
        self._serializer = TypeSerializer()
        self._deserializer = TypeDeserializer()

    def _serialize_item(self, item_dict):
        """Convert Python dict to DynamoDB wire format."""
        return {k: self._serializer.serialize(v) for k, v in item_dict.items()}

    def _deserialize_item(self, dynamo_item):
        """Convert DynamoDB wire format to Python dict."""
        return {k: self._deserializer.deserialize(v) for k, v in dynamo_item.items()}

    def save(self, table_name, item):
        """Save an item to DynamoDB."""
        # Get plain dict from item
        data = item.to_dict()

        # Generate keys
        key = item.create_item_key(**data)

        # Merge data and keys
        full_item = {**data, **key}

        # Serialize to DynamoDB wire format
        dynamo_item = self._serialize_item(full_item)

        self.client.put_item(TableName=table_name, Item=dynamo_item)
        logger.debug(f"Saved item with PK: {key['PK']}, SK: {key['SK']}")

    def get(self, table_name, item_class, **kwargs):
        """Get a single item by key."""
        key = item_class.create_item_key(**kwargs)
        key_serialized = self._serialize_item(key)

        response = self.client.get_item(TableName=table_name, Key=key_serialized)
        item_data = response.get("Item")

        if not item_data:
            return None

        # Deserialize from DynamoDB wire format
        plain_dict = self._deserialize_item(item_data)

        # Remove PK/SK from data
        plain_dict.pop("PK", None)
        plain_dict.pop("SK", None)

        # Create item from plain dict
        return item_class.from_dict(plain_dict)

    def query(
        self, table_name, item_class, limit=0, reverse=False, on_error="warn", **kwargs
    ):
        """Query items by PK and optional SK prefix."""
        try:
            pk_str = item_class.format_key(item_class.pk_pattern, **kwargs)
        except KeyError as e:
            raise ValueError(f"Cannot resolve PK for query: {e}")

        key_expr = "PK = :pk_val"
        expr_values = {":pk_val": {"S": pk_str}}

        # Attempt to resolve SK prefix
        sk_prefix = item_class.partial_key_prefix(item_class.sk_pattern, **kwargs)

        if sk_prefix:
            key_expr += " AND begins_with(SK, :sk_prefix)"
            expr_values[":sk_prefix"] = {"S": sk_prefix}

        query_params = {
            "TableName": table_name,
            "KeyConditionExpression": key_expr,
            "ExpressionAttributeValues": expr_values,
            "ScanIndexForward": not reverse,
        }

        if limit > 0:
            query_params["Limit"] = limit

        paginator = self.client.get_paginator("query")

        count = 0

        for page in paginator.paginate(**query_params):
            for dynamo_item in page.get("Items", []):
                try:
                    # Deserialize from DynamoDB wire format
                    plain_dict = self._deserialize_item(dynamo_item)

                    # Remove PK/SK
                    plain_dict.pop("PK", None)
                    plain_dict.pop("SK", None)

                    # Create item from plain dict
                    yield item_class.from_dict(plain_dict)
                    count += 1
                    if limit > 0 and count >= limit:
                        return
                except Exception as e:
                    if on_error == "warn":
                        logger.warning(
                            "Failed to parse item as %s [%s]", str(item_class), str(e)
                        )
                    elif on_error == "skip":
                        continue
                    elif on_error == "raise":
                        raise
                    else:
                        raise NotImplementedError(
                            f"'{on_error}' error mode not supported"
                        )

    def delete(self, table_name, item):
        """Delete an item from DynamoDB."""
        data = item.to_dict()
        key = item.create_item_key(**data)
        key_serialized = self._serialize_item(key)

        self.client.delete_item(TableName=table_name, Key=key_serialized)
        logger.debug(f"Deleted item with PK: {key['PK']}, SK: {key['SK']}")

    def batch_write(self, table_name, items, batch_size=25):
        """Write multiple items in batches."""
        for i in range(0, len(items), batch_size):
            batch = items[i : i + batch_size]

            put_requests = []
            for item in batch:
                data = item.to_dict()
                key = item.create_item_key(**data)
                full_item = {**data, **key}
                dynamo_item = self._serialize_item(full_item)
                put_requests.append({"PutRequest": {"Item": dynamo_item}})

            request_items = {table_name: put_requests}

            self.client.batch_write_item(RequestItems=request_items)
            logger.debug(f"Batch wrote {len(batch)} items")

    def from_stream_record(self, record, item_class):
        """Construct a DBItem instance from a DynamoDB stream record.

        Raises ValueError if the record PK/SK does not match the item_class
        pattern, making it safe to call on mixed-type streams without branching.

        Example:
            backend = DynamoDBBackend(client)
            obj = backend.from_stream_record(record, UserProfile)
            obj.handle_stream_event(record["eventName"])
        """
        raw = self._deserialize_item(record["dynamodb"]["NewImage"])
        pk = raw.pop("PK", None)
        sk = raw.pop("SK", None)

        if pk is None or sk is None:
            raise ValueError("Missing PK or SK in stream record.")

        if not raw:
            raise ValueError("Record contains only PK and SK, no data fields.")

        if not item_class.is_match(pk, sk):
            raise ValueError(f"Record PK/SK does not match {item_class.__name__} pattern.")

        return item_class.from_dict(raw)
