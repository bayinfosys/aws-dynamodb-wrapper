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

**NOTE**: the low-level client must be used to avoid the auto-deserialization of the dynamodb resource.

```python
import boto3

# Use the low-level client
dynamodb = boto3.client("dynamodb")
story = Story(owner="johndoe", story_id="1234", title="Test Story")
dynamodb.put_item(TableName="StoryTable", Item=story.to_dynamo_item())
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

### Schema Versioning

Add a `schema_version` field to your derived classes.
Dynawrap will compute a hash of the objects `PK`, `SK` and fields to populate this field.
This simplifies filtering objects in migration scripts.

```python
# add to your Pydantic model for migration support
class Story(DBItem, BaseModel):
    pk_pattern = "USER#{owner}#STORY#{story_id}"
    sk_pattern = "STORY#{story_id}"

    # Recommended for tracking schema changes
    schema_version: str = ""

    owner: str
    story_id: str
    title: str

# automatic versioning based on model structure
print(Story._class_schema_version)  # e.g., "a4d8c2b1..."
```

## Common Patterns

### Querying with the built-in helper
```python
# Query all stories by a user
for story in Story.query(dynamodb, "StoryTable", owner="johndoe"):
    print(story.title)

# Query with partial SK matching
for story in Story.query(dynamodb, "StoryTable", owner="johndoe", story_id="partial"):
    print(story.title)
```
---

# Integration Note: Compatibility with boto3.client Only

**Important**: `to_dynamo_item()` returns fully-serialized DynamoDB wire format, using `boto3.dynamodb.types.TypeSerializer`. This is only compatible with the low-level `boto3.client("dynamodb")` interface, not the high-level `boto3.resource("dynamodb")`.

If you use `.put_item(Item=...)` on a `Table` object from `boto3.resource`, it will fail because it expects Python-native types (e.g. str, int, dict) and tries to re-serialise them - which breaks when passed already-encoded wire-format types.

Even `table.meta.client` (from a `boto3.resource` object) will not work reliably for this purpose, because it inherits context that bypasses or modifies type behaviour.

**Always** use `boto3.client("dynamodb").put_item(...)` with output `from to_dynamo_item()`.

The point also stands for deserialization using the `.from_dynamo_item()` and `.from_stream_record()` methods.

---

## AI Code Generation Guide

When using this library with AI assistants:

1. **Always use `boto3.client("dynamodb")`**, never `boto3.resource("dynamodb")`
2. **Define both `pk_pattern` and `sk_pattern`** as ClassVars
3. **Add `schema_version: str = ""`** to all models for migration support
4. **Use descriptive key patterns** like `"{domain}#{resource}#{type}"` for clear partitioning
5. **Consider access patterns** when designing SK patterns for range queries

Common patterns:
- Time-based: `sk_pattern = "{type}#{timestamp}"`
- Hierarchical: `pk_pattern = "{parent}#{child}#{type}"`
- Denormalized: Multiple models with same data, different key patterns

---

## License

This project is licensed under the MIT License.
