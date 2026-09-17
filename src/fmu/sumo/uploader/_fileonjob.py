"""

The FileOnDisk class objectifies a file as it appears
on the disk. A file in this context refers to a data/metadata
pair (technically two files).

"""

import base64
import copy
import hashlib

from fmu.sumo.uploader._logger import get_uploader_logger
from fmu.sumo.uploader._sumofile import SumoFile

try:
    from ._version import version
except (ImportError, AttributeError):
    version = "0.0.0"

# pylint: disable=C0103 # allow non-snake case variable names

logger = get_uploader_logger()


class FileOnJob(SumoFile):
    def __init__(self, byte_string: str, metadata: dict):
        """
        byte_string (str): The content of the file as a byte string
        metadata (dict): The metadata associated with the file
        """
        self._size = None
        self.sumo_object_id = None

        metadata["_sumo"] = {}

        self.byte_string = byte_string
        metadata["_sumo"]["blob_size"] = len(self.byte_string)
        digester = hashlib.md5(self.byte_string)
        metadata["_sumo"]["blob_md5"] = base64.b64encode(
            digester.digest()
        ).decode("utf-8")
        metadata["file"]["checksum_md5"] = digester.hexdigest()
        metadata["_sumo"]["uploader"] = version

        super().__init__(metadata)
