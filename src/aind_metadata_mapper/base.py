""" Base class for all mappers"""


class Mapper:
    """Base class for all mappers"""

    def transform(self, metadata: dict):
        """Transforms raw metadata into a complete model."""
        raise NotImplementedError(
            "This method should be overridden in subclass"
        )
