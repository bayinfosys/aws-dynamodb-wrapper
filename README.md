# aws-dynamodb-wrapper

A lightweight Python library to manage access patterns and object-oriented interactions with AWS DynamoDB. This wrapper uses Pydantic for schema validation and simplifies the handling of partition and sort keys (`PK`, `SK`) in DynamoDB tables.

## Features

- Object-oriented interface for defining and managing DynamoDB items.
- Clean separation of key formatting logic and data modeling.
- Supports stream record deserialization and event handling.
- Compatible with native `boto3` DynamoDB resources.

## Installation

```bash
pip install dynawrap
````

## Quickstart

### 1. Define Your Model

Create a class that inherits from `DBItem` and `pydantic.BaseModel`. Define your primary and sort key formats.

```python
from dynawrap import DBItem
from pydantic import BaseModel

class Story(DBItem, BaseModel):
    pk_pattern = "USER#{owner}#STORY#{story_id}"
    sk_pattern = "STORY#{story_id}"

    owner: str
    story_id: str
    title: str
```

### 2. Save to DynamoDB

Use the standard `boto3` table resource:

```python
import boto3

table = boto3.resource("dynamodb").Table("StoryTable")

story = Story(owner="johndoe", story_id="1234", title="Test Story")
table.put_item(Item=story.to_dynamo_item())
```

### 3. Read from DynamoDB

```python
item_data = table.get_item(Key=Story.create_item_key(owner="johndoe", story_id="1234")).get("Item")

if item_data:
    story = Story.from_dynamo_item(item_data)
    print(story.title)
```

### 4. DynamoDB Streams Integration

You can construct typed instances directly from stream records and handle events:

```python
class UserProfile(DBItem, BaseModel):
    pk_pattern = "USER#{user_id}"
    sk_pattern = "PROFILE"

    user_id: str
    email: str

    def handle_stream_event(self, event_type: str):
        if event_type == "INSERT":
            send_welcome_email(self.email)

def lambda_handler(event, context):
    for record in event["Records"]:
        try:
            obj = UserProfile.from_stream_record(record)
            obj.handle_stream_event(record["eventName"])
        except Exception as e:
            logger.warning("Failed to process record: %s", e)
```

---

## Advanced Features

### Partial Key Queries

```python
prefix = Story.partial_key_prefix("STORY#{story_id}", story_id="1234")
# e.g., STORY#1234
```

Use with `begins_with()` for queries:

```python
from boto3.dynamodb.conditions import Key

response = table.query(
    KeyConditionExpression=Key("PK").eq("USER#johndoe#STORY#1234") & Key("SK").begins_with("STORY#")
)
```

---

## License

This project is licensed under the MIT License.
