"""

The FileOnDisk class objectifies a file as it appears
on the disk. A file in this context refers to a data/metadata
pair (technically two files).

"""

from typing import Any

from fmu.sumo.uploader._logger import get_uploader_logger
from fmu.sumo.uploader._sumofile import SumoFile

try:
    from ._version import version
except (ImportError, AttributeError):
    version = "0.0.0"

# pylint: disable=C0103 # allow non-snake case variable names

logger = get_uploader_logger()


class FileOnJob(SumoFile):
    def __init__(self, byte_string: bytes, metadata: dict[str, Any]) -> None:
        """
        byte_string (bytes): The content of the file.
        metadata (dict[str, Any]): The metadata associated with the file.
        """
        super().__init__(metadata=metadata, byte_string=byte_string)
        self.metadata["file"]["checksum_md5"] = self.blob_md5_hex
