class ValohaiException(Exception):
    """Base exception for all Valohai processes."""


class MissingDatumException(ValohaiException):
    """There is missing Datum File."""


class VersionNotCreatedException(ValohaiException):
    """Version can not be created."""


class AliasNotCreatedException(ValohaiException):
    """Alias can not be created."""
