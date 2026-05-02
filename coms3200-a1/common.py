"""! @file common.py
@author Darren
@ai Inspiration
@ai Wrote Code
@aitool ChatGPT
@aidetails ChatGPT was used to help understand the assignment specification,
plan shared validation helpers, and draft the initial helper functions for
identifier, topic, message, endpoint, and filter validation. The code was
reviewed and adapted by the student.
"""

import re


ID_PATTERN = re.compile(r"^[A-Za-z0-9]{2,32}$")
TOPIC_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9 /]*$")
FILTER_PATTERN = re.compile(r"^(<=|>=|==|!=|<|>)\s*([+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)$")


def is_valid_id(value: str) -> bool:
    """Return True if value is a valid client/server ID."""
    return bool(ID_PATTERN.fullmatch(value))


def is_valid_topic(value: str) -> bool:
    """Return True if value is a valid topic string."""
    return bool(TOPIC_PATTERN.fullmatch(value))


def is_printable_message(value: str) -> bool:
    """Return True if value contains only printable characters."""
    return value.isprintable()


def parse_endpoint(endpoint: str) -> tuple[str, str]:
    """
    Parse [server]:port.

    If server is omitted, localhost is used.
    Raises ValueError if format is invalid or port is empty.
    """
    if endpoint.count(":") != 1:
        raise ValueError("endpoint must contain exactly one colon")

    host, port = endpoint.split(":", 1)

    if port == "":
        raise ValueError("port must not be empty")

    if host == "":
        host = "localhost"

    return host, port


def normalised_endpoint_for_error(endpoint: str) -> str:
    """
    Return endpoint with localhost inserted when server part is omitted.

    Example:
    :3200 -> localhost:3200
    """
    host, port = parse_endpoint(endpoint)
    return f"{host}:{port}"


def parse_filter(filter_string: str) -> tuple[str, float]:
    """
    Parse a subscription filter.

    Valid examples:
    >5
    >= 10
    ==1.0
    != 2e3

    Returns:
    (operator, numeric_value)

    Raises:
    ValueError if invalid.
    """
    match = FILTER_PATTERN.fullmatch(filter_string)
    if not match:
        raise ValueError("invalid filter")

    operator = match.group(1)
    value = float(match.group(2))
    return operator, value


def filter_matches_message(message: str, operator: str, value: float) -> bool:
    """Return True if a message satisfies a numeric filter."""
    try:
        number = float(message.strip())
    except ValueError:
        return False

    if operator == "<":
        return number < value
    if operator == "<=":
        return number <= value
    if operator == ">":
        return number > value
    if operator == ">=":
        return number >= value
    if operator == "==":
        return number == value
    if operator == "!=":
        return number != value

    return False