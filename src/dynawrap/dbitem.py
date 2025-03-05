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
