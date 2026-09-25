"""

The FileOnDisk class objectifies a file as it appears
on the disk. A file in this context refers to a data/metadata
pair (technically two files).

"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

import yaml

from fmu.sumo.uploader._sumofile import SumoFile, _path_to_yaml_path

if TYPE_CHECKING:
    from pathlib import Path


def parse_yaml(path: str | Path) -> dict[str, Any]:
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
        path: str | Path,
    ) -> None:
        """Initialize FileOnDisk.

        Args:
            path: Path to the data file. The companion metadata file is
                derived from it.
        """
        metadata_path = _path_to_yaml_path(path)
        metadata = parse_yaml(metadata_path)
        byte_string = file_to_byte_string(path)
        super().__init__(metadata=metadata, byte_string=byte_string)
        self.path = path

    def __repr__(self) -> str:
        if not self.metadata:
            return f"\n# {self.__class__} \n# No metadata"

        s = f"\n# {self.__class__}"
        s += f"\n# Disk path: {self.path}"
        s += f"\n# Basename: {os.path.basename(self.path)}"
        if self.byte_string is not None:
            s += f"\n# Byte string length: {len(self.byte_string)}"

        if self.sumo_object_id is not None:
            s += f"\n# Uploaded to Sumo. Sumo_ID: {self.sumo_object_id}"

        return s
