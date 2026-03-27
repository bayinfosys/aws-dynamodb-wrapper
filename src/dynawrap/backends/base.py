"""Base backend for database operations."""


class DBBackend:
    """Base interface for database backends.

    All backends should inherit from this and implement these methods.
    """

    def save(self, table_name, item):
        """Save an item to the database.

        Args:
            table_name: Name of the table to save to
            item: DBItem instance to save
        """
        raise NotImplementedError("save() must be implemented by backend")

    def get(self, table_name, item_class, **kwargs):
        """Retrieve a single item by key.

        Args:
            table_name: Name of the table
            item_class: DBItem subclass to instantiate
            **kwargs: Key fields to construct PK/SK

        Returns:
            DBItem instance or None if not found

        Raises:
            ValueError: If the key cannot be fully resolved from kwargs.
        """
        raise NotImplementedError("get() must be implemented by backend")

    def query(self, table_name, item_class, limit=0, reverse=False, **kwargs):
        """Query items by partition key and optional sort key prefix.

        Args:
            table_name: Name of the table
            item_class: DBItem subclass to instantiate
            limit: Maximum number of items (0 = unlimited)
            reverse: Whether to reverse sort order
            **kwargs: Key fields for PK and optional SK prefix

        Yields:
            DBItem instances matching the query
        """
        raise NotImplementedError("query() must be implemented by backend")

    def delete(self, table_name, item):
        """Delete an item from the database.

        Args:
            table_name: Name of the table
            item: DBItem instance to delete
        """
        raise NotImplementedError("delete() must be implemented by backend")

    def batch_write(self, table_name, items):
        """Write multiple items in a batch.

        Args:
            table_name: Name of the table
            items: List of DBItem instances to write
        """
        raise NotImplementedError("batch_write() must be implemented by backend")
