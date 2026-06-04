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


def get_field_from_metadata(metadata: dict, field_path: str):
    """Get field from metadata.

    Given a metadata dictionary and a dot-separated field path,
    e.g. 'fmu.case.uuid', traverse the nested dictionary and return the value
    of the field if it exists, else return None.

    Currently, the function does not support fields paths that contains lists,
    e.g. 'masterdata.smda.fields[0].name'. In this case, the function will
    return None, since the field 'fields[0]' does not exist in the dictionary.
    """

    fields = field_path.split(".")
    value = metadata

    for field in fields:
        if not isinstance(value, dict) or field not in value:
            return None
        value = value[field]
    return value
