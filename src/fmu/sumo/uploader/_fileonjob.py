"""

The FileOnJob class objectifies a file that only exists in memory,
as produced by a job. A file in this context refers to a byte string
and its accompanying metadata.

"""

from typing import Any

from fmu.sumo.uploader._sumofile import SumoFile


class FileOnJob(SumoFile):
    def __init__(self, byte_string: bytes, metadata: dict[str, Any]) -> None:
        """Initialize FileOnJob.

        Args:
            byte_string: The content of the file.
            metadata: The metadata associated with the file.
        """
        super().__init__(metadata=metadata, byte_string=byte_string)
        self.metadata["file"]["checksum_md5"] = self.blob_md5_hex
