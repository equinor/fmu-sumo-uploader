import datetime
import os
from typing import overload

import yaml


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


def get_host_and_domain_names():
    nodename = os.uname().nodename
    nameparts = nodename.split(".", 1)
    host_name = nameparts[0]
    domain_name = nameparts[1] if len(nameparts) > 1 else ""
    return host_name, domain_name


def parse_yaml(path):
    """From path, parse file as yaml, return data"""

    with open(path, "r") as stream:
        data = yaml.safe_load(stream)
    return data


def file_to_byte_string(path):
    """
    Given an path to a file, read as bytes, return byte string.
    """

    with open(path, "rb") as f:
        byte_string = f.read()

    return byte_string
