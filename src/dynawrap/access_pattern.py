import logging


logger = logging.getLogger(__name__)


class AccessPattern:
    """A named access pattern for a PK/SK in a DynamoDB row.

    The pattern is a string with named placeholders, which are replaced by
    values from keyword arguments at runtime.

    Attributes:
        name (str): The name of the access pattern.
        pattern (str): The pattern string with placeholders.

    Methods:
        generate(**kwargs): Generates a key string by replacing placeholders.
    """

    def __init__(self, name, pattern):
        self.name = name
        self.pattern = pattern

    def generate(self, **kwargs):
        """Generates a key string by replacing placeholders in the pattern.

        Args:
            **kwargs: Keyword arguments for replacing placeholders.

        Returns:
            str: The generated key string.
        """
        return self.pattern.format(**kwargs)
