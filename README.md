# dynawrap

A lightweight Python library for structured DynamoDB access. Define key
patterns once on the model class; dynawrap handles PK/SK construction,
serialisation, and query building.

Works with pydantic BaseModel and dataclasses. Supports DynamoDB and
PostgreSQL backends with an identical interface.

## Installation

    pip install dynawrap

For DynamoDB:

    pip install dynawrap[dynamodb]

For PostgreSQL:

    pip install dynawrap[postgres]

## Requirements

- Python 3.10+
- pydantic >= 2.0 (optional; dataclasses also supported)
- boto3 (DynamoDB backend)
- psycopg2 >= 2.9 (PostgreSQL backend)

---

## Architecture

dynawrap separates three concerns:

**Models** define the key structure and fields. They are backend-agnostic
and work with any backend without modification.

**Backends** handle storage. The DynamoDB backend uses the boto3 low-level
client. The PostgreSQL backend uses a single table with `pk`, `sk`,
`schema_version`, and `data` (JSONB) columns. Both backends expose the
same interface: `save`, `get`, `query`, `delete`, `batch_write`.

**Key patterns** are f-string-like class variables on the model. dynawrap
resolves them to construct PK/SK values and supports prefix queries via
partial resolution.

The same model class and application code work with either backend. The
backend is selected at construction time, which makes local development
against PostgreSQL and production deployment to DynamoDB a configuration
change rather than a code change.

---

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
        content: str = ""

Both `pk_pattern` and `sk_pattern` are required ClassVar strings.
Placeholders use Python str.format() syntax: `{field_name}`.

`schema_version` is optional but recommended. dynawrap auto-computes a
hash of the patterns and field names and stores it on every item, which
simplifies migration scripts.

---

## DynamoDB backend

### Setup

    import boto3
    from dynawrap.backends.dynamodb import DynamoDBBackend

    client = boto3.client("dynamodb")
    backend = DynamoDBBackend(client)

Always use `boto3.client("dynamodb")`, never `boto3.resource`. The backend
uses DynamoDB wire format which is incompatible with the resource layer.

### Write

    story = Story(owner="johndoe", story_id="1234", title="Test Story")
    backend.save("stories", story)

### Read

    story = backend.get("stories", Story, owner="johndoe", story_id="1234")
    if story is None:
        print("not found")

### Query

    # All stories by a user
    for story in backend.query("stories", Story, owner="johndoe"):
        print(story.title)

    # Stories with SK prefix match
    for story in backend.query("stories", Story, owner="johndoe", story_id="12"):
        print(story.title)

    # With options
    results = list(backend.query(
        "stories", Story,
        owner="johndoe",
        limit=10,
        reverse=True,
        on_error="warn",    # "warn" | "skip" | "raise"
    ))

### Update

dynawrap has no partial update. Use `model_copy` (pydantic) or
`dataclasses.replace` (dataclasses) then save:

    updated = story.model_copy(update={"title": "New Title"})
    backend.save("stories", updated)

### Delete

    backend.delete("stories", story)

### Batch write

    stories = [Story(owner="johndoe", story_id=str(i), title=f"Story {i}") for i in range(50)]
    backend.batch_write("stories", stories)

---

## PostgreSQL backend

The PostgreSQL backend stores items in a fixed-schema table:

    CREATE TABLE stories (
        pk TEXT NOT NULL,
        sk TEXT NOT NULL,
        schema_version TEXT,
        data JSONB NOT NULL,
        PRIMARY KEY (pk, sk)
    );

All model fields are stored in `data`. No migrations are ever required.

### Use cases

- Local development and testing before deploying to AWS
- Projects that do not require DynamoDB
- Environments where AWS credentials are not available

### Setup

    import psycopg2
    from dynawrap.backends.postgres import PostgresBackend

    conn = psycopg2.connect(dsn)
    PostgresBackend.create_table(conn, "stories")  # idempotent
    backend = PostgresBackend(conn)

`create_table` is safe to call on every application startup.

The backend accepts a psycopg2 connection and manages its own cursors.
Each operation commits immediately, mirroring DynamoDB's per-call
semantics. Connection lifecycle is the caller's responsibility.

### Usage

The interface is identical to the DynamoDB backend:

    story = Story(owner="johndoe", story_id="1234", title="Test Story")
    backend.save("stories", story)

    story = backend.get("stories", Story, owner="johndoe", story_id="1234")
    if story is None:
        print("not found")

    for story in backend.query("stories", Story, owner="johndoe"):
        print(story.title)

    backend.delete("stories", story)

    backend.batch_write("stories", list_of_stories)

### Table names

Table names are passed as strings to every operation. Each model class can
use a dedicated table or share one with other models, as in DynamoDB
single-table design. Table names are interpolated directly into SQL and
must be code-defined constants, not user input.

---

## Switching backends

The model class is identical in both cases. Only the backend construction
changes:

    # Local / PostgreSQL
    conn = psycopg2.connect(dsn)
    backend = PostgresBackend(conn)

    # Production / DynamoDB
    client = boto3.client("dynamodb")
    backend = DynamoDBBackend(client)

    # Application code is unchanged
    backend.save("stories", story)
    story = backend.get("stories", Story, owner="johndoe", story_id="1234")

---

## DynamoDB Streams

`from_stream_record` is a method on `DynamoDBBackend`. It constructs a
typed DBItem instance from a DynamoDB stream record, deserialising the
wire format and validating the PK/SK pattern.

Raises `ValueError` if the record does not match the item class pattern,
making it safe to call on mixed-type streams without branching.

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
        client = boto3.client("dynamodb")
        backend = DynamoDBBackend(client)

        for record in event["Records"]:
            try:
                obj = backend.from_stream_record(record, UserProfile)
                obj.handle_stream_event(record["eventName"])
            except ValueError:
                pass  # record does not match this model, skip
            except Exception as e:
                logger.warning("failed to process record: %s", e)

---

## Key utilities

### create_item_key

Returns the raw `{"PK": ..., "SK": ...}` dict. PK must be fully
resolvable. SK may be partial.

    key = Story.create_item_key(owner="johndoe", story_id="1234")
    # {"PK": "USER#johndoe#STORY#1234", "SK": "STORY#1234"}

    prefix = Story.create_item_key(owner="johndoe")
    # {"PK": "USER#johndoe#STORY#", "SK": "STORY#"}

### partial_key_prefix

Resolves a key pattern as far as the supplied kwargs allow:

    Story.partial_key_prefix("STORY#{story_id}#RUN#{run_id}", story_id="1234")
    # "STORY#1234#RUN#"

### is_match

Returns True if a PK/SK pair matches the class pattern:

    Story.is_match("USER#johndoe#STORY#1234", "STORY#1234")  # True
    Story.is_match("USER#johndoe", "PROFILE")                # False

### to_dict / from_dict

Backend-agnostic serialisation to/from plain Python dicts:

    d = story.to_dict()
    story2 = Story.from_dict(d)

---

## Schema versioning

dynawrap auto-computes `_class_schema_version` as an MD5 of the pk/sk
patterns and sorted field names. This is stored on every item at
construction time.

Use it in migration scripts to find items written by an older model version:

    for story in backend.query("stories", Story, owner="johndoe"):
        if story.schema_version != Story._class_schema_version:
            migrated = migrate(story)
            backend.save("stories", migrated)

---

## AI code generation guide

1. Define both `pk_pattern` and `sk_pattern` as ClassVar strings on every model
2. Add `schema_version: str = ""` to all models
3. Construct a backend by passing a boto3 client (DynamoDB) or psycopg2 connection (PostgreSQL)
4. For DynamoDB: always use `boto3.client("dynamodb")`, never `boto3.resource`
5. For PostgreSQL: call `PostgresBackend.create_table(conn, table_name)` once before use
6. `backend.get()` returns None on miss -- check the return value before use
7. `backend.query()` is a generator -- wrap in `list()` if you need random access
8. Updates are read-modify-write: fetch, `model_copy(update={...})`, then `save`
9. All model code is identical across backends -- only the backend constructor differs
10. Table names must be code-defined constants, not user input
