"""

The FileOnDisk class objectifies a file as it appears
on the disk. A file in this context refers to a data/metadata
pair (technically two files).

"""

from __future__ import annotations

import base64
import hashlib
import os
from typing import TYPE_CHECKING, Any

import yaml

from fmu.sumo.uploader._logger import get_uploader_logger
from fmu.sumo.uploader._sumofile import SumoFile, _path_to_yaml_path

if TYPE_CHECKING:
    from pathlib import Path

try:
    from ._version import version
except (ImportError, AttributeError):
    version = "0.0.0"

# pylint: disable=C0103 # allow non-snake case variable names

logger = get_uploader_logger()


def parse_yaml(path: str | Path) -> Any:
    """From path, parse file as yaml, return data"""
    with open(path, "r") as stream:
        data = yaml.safe_load(stream)
    return data


def file_to_byte_string(path: str | Path) -> bytes:
    """
    Given an path to a file, read as bytes, return byte string.
    """

    with open(path, "rb") as f:
        byte_string = f.read()

    return byte_string


class FileOnDisk(SumoFile):
    def __init__(
        self,
        path: str,
        metadata_path: str | Path | None = None,
        verbosity: int | str = "WARNING",
    ) -> None:
        """
        path (str): Path to file
        metadata_path (str): Path to metadata file. If not provided,
                             path will be derived from file path.
        """

        logger.setLevel(level=verbosity)

        self.metadata_path = (
            metadata_path if metadata_path else _path_to_yaml_path(path)
        )
        self.path = path
        self.metadata = parse_yaml(self.metadata_path)

        self._size = os.path.getsize(self.path)

        self.basename = os.path.basename(self.path)
        self.dir_name = os.path.dirname(self.path)

        self.sumo_object_id = None

        self.metadata["_sumo"] = {}

        self.byte_string = file_to_byte_string(path)
        self.metadata["_sumo"]["blob_size"] = len(self.byte_string)
        digester = hashlib.md5(self.byte_string)
        self.metadata["_sumo"]["blob_md5"] = base64.b64encode(
            digester.digest()
        ).decode("utf-8")
        self.metadata["_sumo"]["uploader"] = version

    def __repr__(self) -> str:
        if not self.metadata:
            return f"\n# {self.__class__} \n# No metadata"

        s = f"\n# {self.__class__}"
        s += f"\n# Disk path: {self.path}"
        s += f"\n# Basename: {self.basename}"
        if self.byte_string is not None:
            s += f"\n# Byte string length: {len(self.byte_string)}"

        if self.sumo_object_id is not None:
            s += f"\n# Uploaded to Sumo. Sumo_ID: {self.sumo_object_id}"

        return s
