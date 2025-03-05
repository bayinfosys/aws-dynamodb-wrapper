class DBItem:
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
        """returns the table name for this item."""
        return cls.table_name

    def save(self, **kwargs):
        """saves the current object's data to DynamoDB."""
        #self.db_wrapper.upsert_item(self.pk_name, self.sk_name, **kwargs)
        self.db_wrapper.upsert_item(self.pk_pattern, self.sk_pattern, **kwargs)

    @classmethod
    def read(cls, db_wrapper, **kwargs):
        """reads an item from DynamoDB and returns a new instance."""
        #item_key = db_wrapper.create_item_key(cls.pk_name, cls.sk_name, **kwargs)
        item_key = db_wrapper.create_item_key(cls.pk_pattern, cls.sk_pattern, **kwargs)
        item_data = db_wrapper.get_item_from_db(item_key)

        if not item_data:
            raise ValueError(f"No item found for key: {item_key}")

        instance = cls(db_wrapper)
        instance.data = item_data
        return instance
