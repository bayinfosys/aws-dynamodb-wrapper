import hashlib
import json
import logging
import string
from typing import ClassVar, Optional

from parse import parse


logger = logging.getLogger(__name__)


class DBItem:
    """Base class for a DynamoDB row item.

    Designed as a mixin for pydantic.BaseModel subclasses.

    Subclasses must define:
        - pk_pattern: primary key pattern string.
        - sk_pattern: sort key pattern string.

    Subclasses should define:
        - schema_version: to record schema version information

    Example:
        class Story(DBItem, BaseModel):
            pk_pattern = "USER#{owner}#STORY#{story_id}"
            sk_pattern = "STORY#{story_id}"

            schema_version: str = ""

            owner: str
            story_id: str
            title: str

        client = boto3.client("dynamodb")
        backend = DynamoDBBackend(client)

        story = Story(owner="johndoe", story_id="1234", title="My Story")
        backend.save("stories", story)

        retrieved = backend.get("stories", Story, owner="johndoe", story_id="1234")
    """

    pk_pattern: ClassVar[str]
    sk_pattern: ClassVar[str]

    _class_schema_version: ClassVar[Optional[str]] = None
    schema_version: str = ""  # version of this instance

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # auto-generate schema hash from patterns and field info
        schema_data = {
            "pk_pattern": cls.pk_pattern,
            "sk_pattern": cls.sk_pattern,
        }

        if hasattr(cls, "model_fields"):  # Pydantic model
            schema_data.update({"fields": sorted(cls.model_fields.keys())})

        schema_str = json.dumps(schema_data, sort_keys=True)
        cls._class_schema_version = hashlib.md5(schema_str.encode()).hexdigest()

    def __init__(self, **data):
        super().__init__(**data)
        if not self.schema_version:
            self.schema_version = self._class_schema_version

    @classmethod
    def format_key(cls, key_pattern, **kwargs):
        """Format a key string using the provided pattern and kwargs."""
        try:
            return key_pattern.format(**kwargs)
        except KeyError as e:
            raise KeyError(f"Missing key for pattern: {e}")

    @classmethod
    def partial_key_prefix(cls, key_pattern: str, **kwargs):
        """Generate a prefix SK by trimming the last unresolved placeholder.

        Args:
            key_pattern: Key pattern with placeholders.
            **kwargs: Partially supplied key-value pairs.

        Returns:
            str: Resolved prefix key.
        """
        formatter = string.Formatter()
        parsed_fields = list(formatter.parse(key_pattern))

        prefix_parts = []
        for literal_text, field_name, format_spec, _ in parsed_fields:
            prefix_parts.append(literal_text)
            if field_name:
                if field_name in kwargs:
                    prefix_parts.append(str(kwargs[field_name]))
                else:
                    break  # Stop at the first missing key

        return "".join(prefix_parts)

    @classmethod
    def is_match(cls, pk: str, sk: str) -> bool:
        """Check if a given PK/SK matches this class's pattern."""
        return (
            parse(cls.pk_pattern, pk) is not None
            and parse(cls.sk_pattern, sk) is not None
        )

    @classmethod
    def create_item_key(cls, **kwargs):
        """Generate the full PK and SK for this item using the class patterns.
        NB: PK must always be fully specified, but SK can be partial.
        """
        pk = cls.format_key(cls.pk_pattern, **kwargs)
        try:
            sk = cls.format_key(cls.sk_pattern, **kwargs)
        except KeyError:
            sk = cls.partial_key_prefix(cls.sk_pattern, **kwargs)
        return {"PK": pk, "SK": sk}

    def to_dict(self):
        """Convert instance to plain Python dict (backend-agnostic)."""
        import dataclasses
        try:
            from pydantic import BaseModel
        except ImportError:
            BaseModel = None

        if dataclasses.is_dataclass(self):
            return dataclasses.asdict(self)
        if BaseModel is not None and isinstance(self, BaseModel):
            return self.model_dump()
        raise TypeError(
            f"{type(self).__name__} must be either a dataclass or a "
            "pydantic BaseModel to use DBItem serialisation."
        )

    @classmethod
    def from_dict(cls, data):
        """Create instance from plain Python dict (backend-agnostic).

        Args:
            data: Dictionary of field values (without PK/SK)

        Returns:
            DBItem: New instance
        """
        return cls(**data)

    def handle_stream_event(self, event_type: str):
        """Optional hook for handling stream events.

        Override this method to add custom event handling logic.

        Args:
            event_type: DynamoDB event type (INSERT, MODIFY, REMOVE)
        """
        pass

    def __repr__(self):
        data = self.to_dict()
        key = self.create_item_key(**data)
        return f"<{self.__class__.__name__} PK={key['PK']} SK={key['SK']}>"
