import datetime
from typing import overload


@overload
def sanitize_datetimes(data: dict) -> dict: ...
@overload
def sanitize_datetimes(data: datetime.datetime) -> str: ...
@overload
def sanitize_datetimes(data: list) -> list: ...
def sanitize_datetimes(data):
    """Sanitize datetimes.

    Given a dictionary, recursively find and replace all datetime objects
    with isoformat string, so that it does not cause problems for
    JSON later on."""

    if isinstance(data, datetime.datetime):
        return data.isoformat()
    if isinstance(data, dict):
        for key in data:
            data[key] = sanitize_datetimes(data[key])
    elif isinstance(data, list):
        data = [sanitize_datetimes(element) for element in data]
    return data


def get_element(dictionary: dict, element: str):
    """Get an element from a nested dictionary.

    Given a nested dictionary and a dot-separated string representing
    an element, e.g. 'fmu.case.uuid', traverse the dictionary and
    return the value of the element if it exists, else return None.

    Currently, the function does not support elements that contain lists,
    e.g. 'masterdata.smda.fields[0].name'. In this case, the function will
    return None, since the element 'fields[0]' does not exist in the dictionary.
    """

    keys = element.split(".")
    value = dictionary

    for key in keys:
        if not isinstance(value, dict) or key not in value:
            return None
        value = value[key]
    return value
