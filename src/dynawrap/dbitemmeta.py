from .access_pattern import AccessPattern


class DBItemMeta(type):
    """Metaclass for DBItem to automatically register AccessPatterns.
    injects the pk_name and sk_name variables into the sub-class
    """

    _access_patterns = {}

    def __new__(cls, name, bases, dct):
        pk_pattern = dct.get("pk_pattern")
        sk_pattern = dct.get("sk_pattern")
        table_name = dct.get("table_name")

        # Register patterns and set names
        if pk_pattern:
            pk_name = f"{name}_pk"
            DBItemMeta._access_patterns[pk_name] = AccessPattern(
                name=pk_name, pattern=pk_pattern
            )
            dct["pk_name"] = pk_name

        if sk_pattern:
            sk_name = f"{name}_sk"
            DBItemMeta._access_patterns[sk_name] = AccessPattern(
                name=sk_name, pattern=sk_pattern
            )
            dct["sk_name"] = sk_name

        if name != "DBItem" and not table_name:
            raise ValueError(f"Class {name} must define a table_name.")

        return super().__new__(cls, name, bases, dct)

    @classmethod
    def get_access_patterns(cls):
        """Retrieves all registered AccessPatterns."""
        return cls._access_patterns
