from enum import IntEnum


class ExitCode(IntEnum):
    OK = 0
    DUPLICATES = 2
    RUNTIME_ERROR = 3
    VALIDATION_ERROR = 4