import string
import logging

from parse import parse

from boto3.dynamodb.types import TypeDeserializer


logger = logging.getLogger(__name__)


class DBItem:
    """Base class for a DynamoDB row item.
    To be used as a mixin with pydantic.BaseModel.

    Subclasses define their specific `pk_pattern`, and `sk_pattern`.

    Attributes:
        pk_pattern (str): The primary key pattern for the item.
        sk_pattern (str): The sort key pattern for the item.

    Example:

        class Story(DBItem):
            pk_pattern = "USER#{owner}#STORY#{story_id}"
            sk_pattern = "STORY#{story_id}"

        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table()

        # Save a story
        story = Story()
        story.data = {"owner": "johndoe", "story_id": "1234", "title": "My Story"}
        table.put_item(Item=story.to_dynamo_item())

        # Read a story
        story_key = Story.create_item_key(owner="johndoe", story_id="1234")
        retrieved_story = table.get_item(Item=story_key)
        print(retrieved_story.data)
    """

    pk_pattern = None
    sk_pattern = None

    @classmethod
    def key(cls, key_pattern, **kwargs):
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

    @classmethod
    def prefix_key(cls, key_pattern: str, **kwargs):
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
            return cls.key(key_pattern, **kwargs)

        # TODO: starting from the end, work backwards until we find a placeholder in kwargs
        # NB: this is a little hardcoded for integer format strings
        last_placeholder = ":".join(
            list(parsed_fields)[-1][1:3]
        )  # last placeholder wuth format string

        # Check if the last placeholder is missing in kwargs
        if last_placeholder not in kwargs:
            prefix_pattern = key_pattern.split(f"{{{last_placeholder}}}")[0]
            return cls.key(prefix_pattern, **kwargs)
        else:
            logger.warning("all placeholders found in kwargs")

        # If all placeholders are present, format normally
        return cls.key(key_pattern, **kwargs)

    @classmethod
    def create_item_key(cls, **kwargs):
        """Generates PK and SK keys based on specified access patterns.

        if the kwargs for sk are not all present, the method attempts to build a prefix sk

        Args:
            pk_pattern_name (str): Name of the PK access pattern.
            sk_pattern_name (str): Name of the SK access pattern.
            **kwargs: Values to replace placeholders in the patterns.

        Returns:
            dict: A dictionary containing the generated PK and SK keys.
        """
        pk = cls.key(cls.pk_pattern, **kwargs)

        # if the sk is not fully define, attempt to build a prefix sk
        try:
            sk = cls.key(cls.sk_pattern, **kwargs)
        except KeyError:
            sk = cls.prefix_key(cls.sk_pattern, **kwargs)

        return {"PK": pk, "SK": sk}

    @classmethod
    def is_match(cls, pk: str, sk: str) -> bool:
        """return True if pk and sk can be parsed into pk_pattern and sk_pattern
        False otherwise
        """
        return (
            parse(cls.pk_pattern, pk) is not None
            and parse(cls.sk_pattern, sk) is not None
        )

    @classmethod
    def deserialize_db_item(cls, item_data):
        """convert the db annotated item to python dict
        ref: https://boto3.amazonaws.com/v1/documentation/api/latest/_modules/boto3/dynamodb/types.html
        """
        d = TypeDeserializer()

        return {k: d.deserialize(v) for k, v in item_data.items()}

    @classmethod
    def from_stream_record(cls, record: dict):
        """parse this dbitem from a dynamodb stream"""
        raw_item = cls.deserialize_db_item(record["dynamodb"]["NewImage"])

        if "PK" not in raw_item:
            raise ValueError("Expected 'PK' in '%s'" % str(raw_item))

        if "SK" not in raw_item:
            raise ValueError("Expected 'SK' in '%s'" % str(raw_item))

        pk = raw_item.pop("PK")
        sk = raw_item.pop("SK")

        if not raw_item:
            raise ValueError("Record only contains PK, SK")

        if not cls.is_match(pk, sk):
            raise ValueError("Record does not match pattern")

        fields = cls.deserialise_db_item(raw_item)

        return cls(**fields)

    @classmethod
    def from_dynamo_item(cls, item: dict) -> "DBItem":
        """ebuild a typed object from a full DynamoDB item
        NB: this assumes the response is from a boto3 table resource, not a table client.
            table client type information can be removed with cls.deserialize_db_item
        """
        return cls(**{k: v for k, v in item.items() if k not in ("PK", "SK")})

    def to_dynamo_item(self, kv_fn="model_dump") -> dict:
        """convert the object to dynamodb Item dict

        kv_fn_name: function name to extract the key/values as a dict
                    defaults to "model_dump" for pydantic usage.

        returns: a dict which can be used in table.put_item(Item=item)
        """
        item_data = self.model_dump()
        key_data = self.create_item_key(**item_data)
        # merge the two dicts to form the final Item representation
        return {**item_data, **key_data}

    def handle_stream_event(self, event_type: str):
        """optional event handler for streams"""
        pass
