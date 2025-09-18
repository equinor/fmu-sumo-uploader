"""Objectify an FMU case (results) as it appears on the disk."""

import logging
import os
import time
import warnings
from pathlib import Path

import httpx
import yaml

from fmu.sumo.uploader._fileondisk import FileOnDisk
from fmu.sumo.uploader._logger import get_uploader_logger
from fmu.sumo.uploader._sumocase import SumoCase

logger = get_uploader_logger()

# pylint: disable=C0103 # allow non-snake case variable names


class CaseOnDisk(SumoCase):
    """
    Class to hold information about an ERT run on disk.

    The CaseOnDisk object is a representation of files belonging to an FMU case,
    as they are stored on the Scratch disk.

    A Case in this context is a set of metadata describing this particular case,
    and an arbitrary number of files belonging to this case. Each file is in reality
    a file pair, consisting of a data file (could be any file type) and a metadata file
    (yaml formatted, according) to FMU standards.

    Example for initialization:
        >>> from fmu import sumo

        >>> env = 'dev'
        >>> case_metadata_path = 'path/to/case_metadata.yaml'
        >>> casepath = 'path/to/casepath/'

        >>> sumoclient = sumo.wrapper.SumoClient(env=env)
        >>> case = sumo.CaseOnDisk(
                case_metadata_path=case_metadata_path,
                casepath=casepath,
                sumoclient=sumoclient)

        After initialization, files must be explicitly indexed into the CaseOnDisk object:

        >>> case.add_files()

        When initialized, the case can be uploaded to Sumo:

        >>> case.upload()

    Args:
        case_metadata_path (str): Path to the case_metadata file for the case
        casepath (str): Path to the case
        sumoclient (sumo.wrapper.SumoClient): SumoConnection object


    """

    def __init__(
        self,
        case_metadata_path: str,
        sumoclient,
        verbosity=logging.WARNING,
        sumo_mode="copy",
        config_path="fmuconfig/output/global_variables.yml",
        parameters_path="parameters.txt",
        casepath=None,
    ):
        """Initialize CaseOnDisk.

        Args:
            case_metadata_path (str): Path to case_metadata for case
            sumoclient (sumo.wrapper.SumoClient): Connection to Sumo.
            verbosity (str): Python logging level.
        """

        self.verbosity = verbosity
        logger.setLevel(level=verbosity)

        logger.debug("case metadata path: %s", case_metadata_path)
        self._case_metadata_path = Path(case_metadata_path)
        case_metadata = _load_case_metadata(case_metadata_path)
        super().__init__(
            case_metadata,
            sumoclient,
            verbosity,
            sumo_mode,
            config_path,
            parameters_path,
            casepath,
        )

        self._sumo_logger = sumoclient.getLogger("fmu-sumo-uploader")
        self._sumo_logger.setLevel(logging.INFO)
        # Avoid that logging to sumo-server also is visible in local logging:
        self._sumo_logger.propagate = False
        self._sumo_logger.info(
            "Initializing Sumo upload for case with sumo_parent_id: "
            + str(self._sumo_parent_id),
            extra={"objectUuid": self._sumo_parent_id},
        )

    def __str__(self):
        s = f"{self.__class__}, {len(self._files)} files."

        if self._sumo_parent_id is not None:
            s += f"\nInitialized on Sumo. Sumo_ID: {self._sumo_parent_id}"
        else:
            s += "\nNot initialized on Sumo."

        return s

    def __repr__(self):
        return str(self.__str__)

    @property
    def sumo_parent_id(self):
        """Return the sumo parent ID"""
        return self._sumo_parent_id

    @property
    def fmu_case_uuid(self):
        """Return the fmu_case_uuid"""
        return self._fmu_case_uuid

    @property
    def files(self):
        """Return the files"""
        return self._files

    def add_files(self):
        """Add files to the case, based on dataio export manifest file"""

        file_paths = self._find_file_paths()

        for file_path in file_paths:
            try:
                file = FileOnDisk(path=file_path, verbosity=self.verbosity)
                self._files.append(file)
                logger.info("File appended: %s", file_path)

            except Exception as err:
                warnings.warn(f"No metadata, skipping file: {err}")

    def register(self):
        """Register this case on Sumo.

        Assumptions: If registering an already existing case, it will be overwritten.
        ("register" might be a bad word for this...)

        Returns:
            sumo_parent_id (uuid4): Unique ID for this case on Sumo
        """

        try:
            sumo_parent_id = self._upload_case_metadata(self.case_metadata)
            self._sumo_parent_id = sumo_parent_id

            # Give Sumo some time to make the case object searchable.
            time.sleep(3)

            try:
                self.sumoclient.create_shared_access_key_for_case(
                    self._fmu_case_uuid
                )
            except Exception as ex:
                logger.warn(f"Unable to create shared access key: {ex}")
                pass

            logger.info("Case registered. SumoID: {}".format(sumo_parent_id))

            return sumo_parent_id
        except Exception as err:
            print(
                "\n\033[31m"
                "Error during registering case on Sumo. "
                "\nFile uploads will also fail. "
                "\033[0m"
            )
            error_string = f"Registering case on Sumo failed: error details: {err} {type(err)}"
            if isinstance(err, httpx.HTTPStatusError):
                if err.response.status_code == 401:
                    print(
                        "\033[31m"
                        "Please verify that you are logged in to Sumo, "
                        "by running sumo_login in a Unix terminal window"
                        " \033[0m"
                    )
                if err.response.status_code == 403:
                    print(
                        "\033[31m"
                        "Please verify that you have write access"
                        " to Sumo (AccessIT)"
                        "\033[0m"
                    )
                error_string = f"{error_string} {err.response.text}"
            error_string = f"{error_string} Case metadata file path: {self._case_metadata_path}"
            print(error_string)
            warnings.warn(error_string)
            return "0"

    def _upload_case_metadata(self, case_metadata: dict):
        """Upload case metadata to Sumo."""

        response = self.sumoclient.post(path="/objects", json=case_metadata)

        returned_object_id = response.json().get("objectid")

        return returned_object_id

    def _find_file_paths(self):
        """Find files and return as list of FileOnDisk instances."""

        manifest = self._load_export_manifest()
        sumo_uploads = self._load_sumo_uploads()
        next_index = self._get_next_index(manifest, sumo_uploads)

        logger.info("Finding files to upload.")
        if next_index > len(manifest) - 1:
            files = []
        else:
            logger.info(
                f"Upload will start from index {next_index} in manifest."
            )
            files = [
                f["absolute_path"]
                for f in manifest[next_index:]
                if os.path.isfile(f["absolute_path"])
            ]

        if len(files) == 0:
            warnings.warn("No files found!")

        return files

    def _get_next_index(self, manifest, sumo_uploads):
        "Determine the start uploading index in manifest"

        if not sumo_uploads or not manifest:
            return 0

        last_uploaded_index = sumo_uploads[-1]["last_index_manifest"]
        ts_uploads = sumo_uploads[-1]["timestamp"]

        try:
            if manifest[last_uploaded_index]["exported_at"] == ts_uploads:
                return last_uploaded_index + 1
        except KeyError as e:
            logger.debug(f"KeyError while accessing manifest: {e}")
        except IndexError as e:
            logger.debug(f"IndexError while accessing manifest: {e}")

        # When the manifest and sumo uploads log has a mismatch, like manifest is overwritten, reupload from index 0.
        return 0


def _load_case_metadata(case_metadata_path: str):
    """Load the case metadata."""

    if not os.path.isfile(case_metadata_path):
        warnings.warn(
            f"Invalid metadata: file does not exist {case_metadata_path}"
        )
        return {}

    try:
        with open(case_metadata_path, "r") as stream:
            yaml_data = yaml.safe_load(stream)
        return yaml_data
    except Exception:
        warnings.warn(f"Invalid metadata in yml file {case_metadata_path}")
        return {}
