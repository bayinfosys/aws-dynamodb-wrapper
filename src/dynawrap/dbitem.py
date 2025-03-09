import string
import logging


logger = logging.getLogger(__name__)


class DBItem:
    """Base class for a DynamoDB row item.

    Subclasses define their table-specific `table_name`, `pk_pattern`, and `sk_pattern`.

    Attributes:
        table_name (str): The name of the DynamoDB table associated with this item.
        pk_pattern (str): The primary key pattern for the item.
        sk_pattern (str): The sort key pattern for the item.

    Example:

        class Story(DBItem):
            table_name = "StoryTable"
            pk_pattern = "USER#{owner}#STORY#{story_id}"
            sk_pattern = "STORY#{story_id}"

        db_wrapper = DynamodbWrapper("StoryTable")

        # Save a story
        story = Story()
        story.data = {"owner": "johndoe", "story_id": "1234", "title": "My Story"}
        db_wrapper.save(story)

        # Read a story
        retrieved_story = db_wrapper.read(Story, owner="johndoe", story_id="1234")
        print(retrieved_story.data)
    """

    table_name = None  # Specify the table name here
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
        last_placeholder = ":".join(list(parsed_fields)[-1][1:3])  # last placeholder wuth format string

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
