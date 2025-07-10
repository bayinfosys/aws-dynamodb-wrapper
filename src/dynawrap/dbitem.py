import logging
import string

from parse import parse

try:
    from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
except ImportError:
    TypeDeserializer = None
    TypeSerializer = None


logger = logging.getLogger(__name__)


class DBItem:
    """Base class for a DynamoDB row item.

    Designed as a mixin for pydantic.BaseModel subclasses.

    Subclasses must define:
        - pk_pattern: primary key pattern string.
        - sk_pattern: sort key pattern string.

    Example:
        class Story(DBItem):
            pk_pattern = "USER#{owner}#STORY#{story_id}"
            sk_pattern = "STORY#{story_id}"

        # Writing an item
        story = Story(owner="johndoe", story_id="1234", title="My Story")
        table.put_item(Item=story.to_dynamo_item())

        # Reading an item
        story = Story.read("StoriesTable", owner="johndoe", story_id="1234")
        print(story.title)
    """

    pk_pattern = None
    sk_pattern = None

    @classmethod
    def format_key(cls, key_pattern, **kwargs):
        """Format a key string using the provided pattern and kwargs."""
        try:
            return key_pattern.format(**kwargs)
        except KeyError as e:
            raise KeyError(f"Missing key for pattern: {e}")

    @classmethod
    def partial_key_prefix(cls, key_pattern: str, **kwargs):
        """Generate a prefix SK by trimming the last unresolved placeholder.

        Args:
            key_pattern (str): Key pattern with placeholders.
            **kwargs: Partially supplied key-value pairs.

        Returns:
            str: Resolved prefix key.
        """
        formatter = string.Formatter()
        parsed_fields = list(formatter.parse(key_pattern))

        prefix_parts = []
        for literal_text, field_name, format_spec, _ in parsed_fields:
            prefix_parts.append(literal_text)
            if field_name:
                if field_name in kwargs:
                    prefix_parts.append(str(kwargs[field_name]))
                else:
                    break  # Stop at the first missing key

        return "".join(prefix_parts)

    @classmethod
    def create_item_key(cls, **kwargs):
        """Generate the full PK and SK for this item using the class patterns.
        NB: PK must always be fully specified, but SK can be partial.
        """
        pk = cls.format_key(cls.pk_pattern, **kwargs)
        try:
            sk = cls.format_key(cls.sk_pattern, **kwargs)
        except KeyError:
            sk = cls.partial_key_prefix(cls.sk_pattern, **kwargs)
        return {"PK": pk, "SK": sk}

    @classmethod
    def is_match(cls, pk: str, sk: str) -> bool:
        """Check if a given PK/SK matches this class's pattern."""
        return (
            parse(cls.pk_pattern, pk) is not None
            and parse(cls.sk_pattern, sk) is not None
        )

    @staticmethod
    def deserialize_db_item(item_data):
        """Convert DynamoDB-annotated item into a standard Python dict."""
        if TypeDeserializer is None:
            raise NotImplementedError("boto3 not installed, TypeDeserializer not available")

        deserializer = TypeDeserializer()
        try:
            return {k: deserializer.deserialize(v) for k, v in item_data.items()}
        except Exception as e:
            logger.exception("unable to deserialize '%s' [%s]", str(item_data), str(e))
            raise e

    @staticmethod
    def serialize_db_item(item_data: dict):
        """convrt a dict to a dynamodb-annotated item"""
        if TypeSerializer is None:
            raise NotImplementedError("boto3 not installed, TypeSerializer not available")

        serializer = TypeSerializer()
        try:
            return {k: serializer.serialize(v) for k, v in item_data.items()}
        except Exception as e:
            logger.exception("unable to serialize '%s' [%s]", str(item_data), str(e))
            raise e

    @classmethod
    def from_stream_record(cls, record: dict):
        """Construct an instance from a DynamoDB stream record."""
        raw_item = cls.deserialize_db_item(record["dynamodb"]["NewImage"])
        pk = raw_item.pop("PK", None)
        sk = raw_item.pop("SK", None)

        if pk is None or sk is None:
            raise ValueError("Missing PK or SK in stream record.")

        if not raw_item:
            raise ValueError("Record only contains PK, SK")

        if not cls.is_match(pk, sk):
            raise ValueError("Record does not match pattern")

        return cls(**raw_item)

    @classmethod
    def from_dynamo_item(cls, item: dict) -> "DBItem":
        """Instantiate class from a raw DynamoDB item."""
        return cls(**{k: v for k, v in item.items() if k not in ("PK", "SK")})

    def to_dynamo_item(self) -> dict:
        """Convert the instance into a full DynamoDB item dictionary."""
        item_data = self.model_dump()
        key_data = self.create_item_key(**item_data)
        item = {**item_data, **key_data}
        return self.serialize_db_item(item)

    def handle_stream_event(self, event_type: str):
        """Optional hook for handling stream events."""
        pass

    @classmethod
    def read(cls, table, **kwargs) -> "DBItem":
        """Read an item from DynamoDB and return an instance of this class.

        Args:
            table (dynamodb table resource): The DynamoDB table.
            **kwargs: Arguments to generate the item's PK/SK.

        Returns:
            DBItem: Instance containing the retrieved data.
        """
        key = cls.create_item_key(**kwargs)

        response = table.get_item(Key=key)
        item = response.get("Item")

        if not item:
            raise KeyError(f"Item not found with key: {key}")

        return cls.from_dynamo_item(item)

    @classmethod
    def query(cls, table, **kwargs):
        """Query the table with a PK and (assumed) partial SK definition.
        Yields all objects returned by the query operation.
        """
        from boto3.dynamodb.conditions import Key

        paginator = table.meta.client.get_paginator("query")
        key = cls.create_item_key()  # only PK will be resolved
        pk_value = key["PK"]

        for page in paginator.paginate(
            TableName=table.name,
            KeyConditionExpression=Key("PK").eq(pk_value),
        ):
            for item in page.get("Items", []):
                yield cls.from_dynamo_item(item)

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.to_dynamo_item()}>"
