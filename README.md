# aws-dynamodb-wrapper

A lightweight wrapper to manage access patterns and object-oriented interactions with AWS DynamoDB tables. This library simplifies the management of access patterns for keys (`PK`, `SK`) and provides an intuitive way to save, read, and update items in DynamoDB.

## Features

- Simplify read/write operations with object-oriented classes.
- Automatically register and validate table-specific key patterns.
- Provides low-level DynamoDB operations via `boto3`.

## Installation

Install via pip:
```bash
pip install dynawrap
```

## Usage

### Define Access Patterns
Define your DynamoDB schema using classes that inherit from `DBItem`. Provide the table name, primary key (`PK`) pattern, and sort key (`SK`) pattern.

```python
from dynawrap import DynamodbWrapper, DBItem

class Story(DBItem):
    table_name = "StoryTable"
    pk_pattern = "USER#{owner}#STORY#{story_id}"
    sk_pattern = "STORY#{story_id}"
```

### Save Items
Save an item to DynamoDB by populating its attributes and calling `save()`.

```python
db_wrapper = DynamodbWrapper()
story = Story(db_wrapper)
story_data = {"owner": "johndoe", "story_id": "1234", "title": "Test Story"}
story.save(story_data)
```

### Read Items
Read items by providing the required key attributes.

```python
retrieved_story = Story.read(db_wrapper, owner="johndoe", story_id="1234")
print(retrieved_story.data)
```

### Update Items
Update items with new attributes by saving them again.

```python
story.data['title'] = "Updated Story Title"
story.save(story.data)
```

### Advanced Operations
- **Custom Access Patterns:** Create multiple `AccessPattern` instances for advanced queries.
- **Global Secondary Indexes (GSI):** Define GSIs for alternative query patterns (future feature).

## Example with Prefect
Use Dynawrap to log Prefect task and flow metadata to DynamoDB:
```python
from dynawrap import DynamodbWrapper, DBItem

class PrefectMetadata(DBItem):
    table_name = "PrefectMetadata"
    pk_pattern = "FLOW#{flow_id}"
    sk_pattern = "TASK#{task_id}"

db_wrapper = DynamodbWrapper(PrefectMetadata)

# Save a flow state
flow_metadata = {
    "flow_id": "1234",
    "task_id": "5678",
    "state": "Completed",
}
PrefectMetadata(db_wrapper).save(flow_metadata)
```

## License
This project is licensed under the MIT License.
