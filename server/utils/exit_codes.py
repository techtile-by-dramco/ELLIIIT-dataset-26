from enum import IntEnum


class ExitCode(IntEnum):
    SUCCESS = 0
    CAL_FILE_NOT_FOUND = 1
    CAL_FILE_PARSE_ERROR = 2
    CAL_FILE_UNEXPECTED_ERROR = 3
    WEIGHTS_NOT_FOUND = 4
    YAML_PARSING_ERROR = 5
    GENERAL_FAILURE = 6


__all__ = ["ExitCode"]
