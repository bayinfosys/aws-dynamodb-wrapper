# aws-dynamodb-wrapper

Lightweight wrapper to handle access pattern management to AWS DynamoDB tables.

Access keys (`PK`, `SK`, etc) are defined as python template strings with placeholder variables.
Runtime read/write/update of rows is handled by passing `kwargs` corresponding to these placeholders.
The module then creates the appropriate item key for updating a dynamodb table.

## Features

- Define access patterns for DynamoDB keys
- Use object-oriented methods to save and read items

## Installation

```bash
pip install dynawrap
```

```python
from dynawrap import DynamodbWrapper, DBItem

class Story(DBItem):
    table_name = "StoryTable"
    pk_pattern = "USER#{owner}#STORY#{story_id}"
    sk_pattern = "STORY#{story_id}"

db_wrapper = DynamodbWrapper(Story)
story = Story(db_wrapper)
story.data = {"owner": "johndoe", "story_id": "1234", "title": "Test Story"}
story.save()
```
