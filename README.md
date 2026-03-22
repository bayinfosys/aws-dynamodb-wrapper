# dynawrap

A lightweight Python library for object-oriented DynamoDB access. Define
your key patterns once on the model class; dynawrap handles PK/SK
construction, serialisation, and query building.

Works with pydantic BaseModel and dataclasses.

## Installation

    pip install dynawrap

## Requirements

- Python 3.10+
- boto3
- pydantic >= 2.0 (optional; dataclasses also supported)

## Critical: always use boto3.client, never boto3.resource

`to_dynamo_item()` returns DynamoDB wire-format (TypeSerializer output).
This is only compatible with `boto3.client("dynamodb")`. Using
`boto3.resource("dynamodb")` or a Table object will fail because the
resource layer attempts to re-serialise already-encoded values.

Always construct your client as:

    dynamodb = boto3.client("dynamodb")

## Quickstart

### Define a model

    from dynawrap import DBItem
    from pydantic import BaseModel

    class Story(DBItem, BaseModel):
        pk_pattern = "USER#{owner}#STORY#{story_id}"
        sk_pattern = "STORY#{story_id}"

        schema_version: str = ""

        owner: str
        story_id: str
        title: str

Both `pk_pattern` and `sk_pattern` are required ClassVar strings.
Placeholders use Python str.format() syntax: `{field_name}`.

`schema_version` is optional but recommended. dynawrap auto-computes a
hash of the patterns and field names and stores it on every item, which
simplifies migration scripts.

### Write an item

    import boto3

    dynamodb = boto3.client("dynamodb")

    story = Story(owner="johndoe", story_id="1234", title="Test Story")
    dynamodb.put_item(TableName="stories", Item=story.to_dynamo_item())

### Read an item

`read()` raises KeyError if the item does not exist.

    try:
        story = Story.read(dynamodb, "stories", owner="johndoe", story_id="1234")
        print(story.title)
    except KeyError:
        print("not found")

### Query items

`query()` is a generator. It resolves the PK fully from kwargs and builds
an SK prefix from any kwargs that can be resolved against the SK pattern.

    # all stories by a user
    for story in Story.query(dynamodb, "stories", owner="johndoe"):
        print(story.title)

    # stories by a user with a specific story_id prefix
    for story in Story.query(dynamodb, "stories", owner="johndoe", story_id="12"):
        print(story.title)

Query options:

    Story.query(dynamodb, "stories", owner="johndoe",
        limit=10,        # max items to return (0 = no limit)
        reverse=True,    # scan index in descending SK order
        on_error="warn", # "warn" | "skip" | "raise" on parse failure
    )

### Update an item

dynawrap does not have a partial update method. Use `model_copy` (pydantic)
or `dataclasses.replace` (dataclasses) to produce a new instance, then
write it back:

    updated = story.model_copy(update={"title": "New Title"})
    dynamodb.put_item(TableName="stories", Item=updated.to_dynamo_item())

### DynamoDB Streams

Construct a typed instance directly from a stream record:

    class UserProfile(DBItem, BaseModel):
        pk_pattern = "USER#{user_id}"
        sk_pattern = "PROFILE"

        schema_version: str = ""
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
                logger.warning("failed to process record: %s", e)

`from_stream_record()` raises ValueError if the record PK/SK does not
match the class pattern, which makes it safe to call on a mixed-type
stream without branching.

## Key utilities

### create_item_key

Returns the raw `{"PK": ..., "SK": ...}` dict for a given set of kwargs.
PK must be fully resolvable. SK may be partial (returns a prefix).

    key = Story.create_item_key(owner="johndoe", story_id="1234")
    # {"PK": "USER#johndoe#STORY#1234", "SK": "STORY#1234"}

    prefix = Story.create_item_key(owner="johndoe")
    # {"PK": "USER#johndoe#STORY#...", "SK": "STORY#"} -- partial SK

### partial_key_prefix

Resolves a key pattern as far as the supplied kwargs allow, stopping at
the first unresolved placeholder:

    Story.partial_key_prefix("STORY#{story_id}#RUN#{run_id}", story_id="1234")
    # "STORY#1234#RUN#"

### is_match

Returns True if a given PK/SK pair matches the class pattern:

    Story.is_match("USER#johndoe#STORY#1234", "STORY#1234")  # True
    Story.is_match("USER#johndoe", "PROFILE")                # False

### to_dict / from_dict

Backend-agnostic serialisation to/from plain Python dicts. Works for
both pydantic models and dataclasses:

    d = story.to_dict()
    story2 = Story.from_dict(d)

## Schema versioning

dynawrap auto-computes `_class_schema_version` as an MD5 of the pk/sk
patterns and sorted field names. This is written to `schema_version` on
every item at construction time.

Use it in migration scripts to identify items written by an older version
of a model:

    for story in Story.query(dynamodb, "stories", owner="johndoe"):
        if story.schema_version != Story._class_schema_version:
            migrated = migrate(story)
            dynamodb.put_item(TableName="stories", Item=migrated.to_dynamo_item())

## AI code generation guide

1. Always use `boto3.client("dynamodb")`, never `boto3.resource`
2. Define both `pk_pattern` and `sk_pattern` as ClassVar strings
3. Add `schema_version: str = ""` to all models
4. `query()` is a generator -- wrap in `list()` if you need random access
5. `read()` raises KeyError on miss -- always handle it
6. Updates are read-modify-write: `model_copy(update={...})` then `put_item`
7. `to_dynamo_item()` and `read()`/`query()` require `boto3.client`, not resource
