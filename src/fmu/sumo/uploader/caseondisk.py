"""Objectify an FMU case (results) as it appears on the disk."""

from __future__ import annotations

import json
import logging
import os
import statistics
import time
import warnings
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeAlias

import httpx
import yaml
from fmu.dataio.manifest import get_manifest_path

from fmu.sumo.uploader._fileondisk import FileOnDisk
from fmu.sumo.uploader._logger import get_uploader_logger
from fmu.sumo.uploader._upload_files import upload_files
from fmu.sumo.uploader._utils import (
    get_element,
    get_host_and_domain_names,
    sanitize_datetimes,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sumo.wrapper import SumoClient

    from fmu.sumo.uploader._sumofile import SumoFile

UploadResult: TypeAlias = dict[str, Any]
"""The outcome of uploading a single file, as returned by upload_files."""

logger = get_uploader_logger()

try:
    uploader_version = version("fmu-sumo-uploader")
except PackageNotFoundError:
    uploader_version = "0.0.0"


class CaseOnDisk:
    """Representation of an FMU case and its files, as they appear on disk.

    A case in this context is a set of metadata describing one particular FMU
    case, and an arbitrary number of files belonging to that case. Each file is
    in reality a file pair, consisting of a data file (could be any file type)
    and a metadata file (yaml formatted, according to FMU standards).

    Example:
        >>> from fmu import sumo

        >>> sumoclient = sumo.wrapper.SumoClient(env="dev")
        >>> case = sumo.CaseOnDisk(
                case_metadata_path="path/to/case_metadata.yaml",
                casepath="path/to/casepath/",
                sumoclient=sumoclient)

        After initialization, files must be explicitly indexed into the
        CaseOnDisk object, before they can be uploaded to Sumo:

        >>> case.add_files()
        >>> case.upload()
    """

    def __init__(
        self,
        case_metadata_path: str | Path,
        sumoclient: SumoClient,
        verbosity: int | str = logging.WARNING,
        sumo_mode: str = "copy",
        config_path: str = "fmuconfig/output/global_variables.yml",
        casepath: str | Path | None = None,
    ) -> None:
        """Initialize CaseOnDisk.

        Args:
            case_metadata_path: Path to the case metadata file for the case.
            sumoclient: Connection to Sumo.
            verbosity: Python logging level.
            sumo_mode: Either "copy" or "move", deciding whether files are
                kept on disk after a successful upload.
            config_path: Path to the global variables file for the case.
            casepath: Path to the case root on disk.
        """

        logger.setLevel(level=verbosity)

        self.sumoclient = sumoclient
        self.sumo_mode = sumo_mode
        self.config_path = config_path
        self.casepath = casepath

        logger.debug("case metadata path: %s", case_metadata_path)
        self._case_metadata_path = Path(case_metadata_path)
        self.case_metadata = sanitize_datetimes(
            _load_case_metadata(self._case_metadata_path)
        )

        self._files: list[SumoFile] = []
        self._fmu_case_uuid = get_element(self.case_metadata, "fmu.case.uuid")
        logger.debug("self._fmu_case_uuid is %s", self._fmu_case_uuid)
        self._sumo_parent_id = self._fmu_case_uuid
        logger.debug("self._sumo_parent_id is %s", self._sumo_parent_id)

        self._ensemble_name = os.environ.get(
            "_ERT_ENSEMBLE_NAME", "default_ensemble"
        )
        realization_number = os.environ.get("_ERT_REALIZATION_NUMBER")
        self._realization_id: int | None = (
            int(realization_number) if realization_number is not None else None
        )

        self._sumo_logger = sumoclient.getLogger("fmu-sumo-uploader")
        self._sumo_logger.setLevel(logging.INFO)
        # Avoid that logging to sumo-server also is visible in local logging:
        self._sumo_logger.propagate = False
        logger.info(
            "Initializing Sumo upload for case with sumo_parent_id: "
            + str(self._sumo_parent_id),
            extra={"objectUuid": self._sumo_parent_id},
        )

    def __str__(self) -> str:
        s = f"{self.__class__}, {len(self._files)} files."

        if self._sumo_parent_id is not None:
            s += f"\nInitialized on Sumo. Sumo_ID: {self._sumo_parent_id}"
        else:
            s += "\nNot initialized on Sumo."

        return s

    def __repr__(self) -> str:
        return self.__str__()

    @property
    def sumo_parent_id(self) -> str:
        """Return the sumo parent ID"""
        return self._sumo_parent_id

    @property
    def fmu_case_uuid(self) -> str:
        """Return the fmu_case_uuid"""
        return self._fmu_case_uuid

    @property
    def files(self) -> list[SumoFile]:
        """Return the files"""
        return self._files

    def add_files(self) -> None:
        """Add files to the case, based on dataio export manifest file"""

        file_paths = self._find_file_paths()

        for file_path in file_paths:
            try:
                file = FileOnDisk(path=file_path)
                self._files.append(file)
                logger.info("File appended: %s", file_path)

            except Exception as err:
                warnings.warn(f"No metadata, skipping file: {err}")

    def register(self) -> str:
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
                logger.warning(f"Unable to create shared access key: {ex}")

            logger.info(f"Case registered. SumoID: {sumo_parent_id}")

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
                        "Please verify that you completed the Equinor "
                        "Azure login in the browser window that was "
                        "opened, before the login request expired."
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

    def upload(self) -> list[UploadResult]:
        """Upload all indexed files to Sumo.

        Returns the results for the files that were uploaded successfully.
        Files that failed or were rejected are logged, both locally and to
        Sumo."""

        if not self.files:
            logger.warning("No files to upload.")
            return []

        files_to_upload = list(self.files)
        logger.debug("files_to_upload: %s", files_to_upload)

        sumoclient = self.sumoclient.client_for_case(self._sumo_parent_id)

        start_time = datetime.now(tz=UTC).isoformat()
        _t0 = time.perf_counter()
        upload_results = upload_files(
            files_to_upload,
            self._sumo_parent_id,
            sumoclient,
            self.sumo_mode,
            self.config_path,
        )
        _dt = time.perf_counter() - _t0
        end_time = datetime.now(tz=UTC).isoformat()

        ok_uploads = upload_results["ok_uploads"]
        failed_uploads = upload_results["failed_uploads"]
        rejected_uploads = upload_results["rejected_uploads"]

        # Files rejected during validation never reach the metadata upload
        # stage, so they have no "metadata_upload" entry.
        if any(
            res.get("metadata_upload") is not None
            and res["metadata_upload"].statuscode == 404
            for res in rejected_uploads
        ):
            warnings.warn("Case is not registered on Sumo")
            logger.info(
                "Case was not found on Sumo. If you are in the FMU context "
                "something may have gone wrong with the case registration "
                "or you have not specified that the case shall be uploaded."
                "A warning will be issued, and the script will stop. "
                "If you are NOT in the FMU context, you can specify that "
                "this script also registers the case by passing "
                "register=True. This should not be done in the FMU context."
            )

        md_retries, blob_retries = _get_retries(
            ok_uploads, failed_uploads, rejected_uploads
        )

        if md_retries or blob_retries:
            self._sumo_logger.warning(
                "UploadRetries: Some uploads required retries. Case %s, Ensemble %s, Realization %s. Metadata retries: %d, Blob retries: %d",
                self._fmu_case_uuid,
                self._ensemble_name,
                self._realization_id,
                len(md_retries),
                len(blob_retries),
                extra={
                    "objectUuid": self._sumo_parent_id,
                    "details": {
                        "metadata_retries": _get_stats(md_retries),
                        "blob_retries": _get_stats(blob_retries),
                    },
                },
            )

        upload_statistics: dict[str, Any] = {}
        total_bytes_uploaded = 0
        if ok_uploads:
            upload_statistics = _calculate_upload_stats(ok_uploads)
            total_bytes_uploaded = sum(
                u["file_size_bytes"] for u in ok_uploads
            )
            logger.info(upload_statistics)
            self._update_sumo_uploads()

        self._log_upload_issues("rejected", rejected_uploads)
        self._log_upload_issues("failed", failed_uploads)

        logger.info("Summary:")
        logger.info("Total files count: %s", str(len(files_to_upload)))
        logger.info("OK: %s", str(len(ok_uploads)))
        logger.info("Failed: %s", str(len(failed_uploads)))
        logger.info("Rejected: %s", str(len(rejected_uploads)))
        logger.info(f"Wall time: {_dt:.2f} sec")
        logger.info(f"Sumo mode: {self.sumo_mode}")

        host_name, domain_name = get_host_and_domain_names()
        # Per-file upload rate
        bytes_per_sec = (
            round(total_bytes_uploaded / _dt / len(ok_uploads), 2)
            if _dt > 0 and ok_uploads
            else 0
        )

        details_mapping = {
            "case_uuid": self._fmu_case_uuid,
            "ert_ensemble_name": self._ensemble_name,
            "asset": get_element(self.case_metadata, "access.asset.name"),
            "host_name": host_name,
            "domain_name": domain_name,
            "uploader_version": uploader_version,
            "total_files_count": len(files_to_upload),
            "ok_files": len(ok_uploads),
            "failed_files": len(failed_uploads),
            "rejected_files": len(rejected_uploads),
            "total_bytes_uploaded": total_bytes_uploaded,
            "start_time": start_time,
            "end_time": end_time,
            "wall_time_seconds": _dt,
            "upload_statistics": upload_statistics,
            "upload_rate_bytes_per_sec": bytes_per_sec,
            "sumo_mode": self.sumo_mode,
            "realization_id": self._realization_id,
        }

        details = {
            k: v for k, v in details_mapping.items() if not _is_empty(v)
        }

        self._sumo_logger.info(
            "Upload summary",
            extra={"objectUuid": self._fmu_case_uuid, "details": details},
        )

        return ok_uploads

    def _log_upload_issues(
        self, outcome: str, uploads: list[UploadResult]
    ) -> None:
        """Log the first few problematic uploads, locally and to Sumo."""

        if not uploads:
            return

        logger.info(
            f"\n\n{len(uploads)} files {outcome} by Sumo. First 5 {outcome} files:"
        )

        for upload in uploads[:5]:
            log_msg = _get_log_msg(self._sumo_parent_id, upload)
            logger.info(log_msg)
            self._sumo_logger.error(
                log_msg, extra={"objectUuid": self._sumo_parent_id}
            )

    def _upload_case_metadata(self, case_metadata: dict[str, Any]) -> str:
        """Upload case metadata to Sumo."""

        response = self.sumoclient.post(path="/objects", json=case_metadata)

        returned_object_id = response.json().get("objectid")

        return returned_object_id

    def _find_file_paths(self) -> list[str]:
        """Return the paths of the files that are not yet uploaded."""

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

        return files

    def _get_next_index(
        self,
        manifest: list[dict[str, Any]],
        sumo_uploads: list[dict[str, Any]],
    ) -> int:
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

    @property
    def _sumo_uploads_path(self) -> Path:
        """Path to the log of previous uploads for this case."""
        return get_manifest_path(self.casepath).parent / ".sumo_uploads.json"

    def _load_export_manifest(self) -> list[dict[str, Any]]:
        """Load the dataio export manifest from file."""

        manifest_path = get_manifest_path(self.casepath)
        logger.info(f"Loading export manifest from {manifest_path}")

        if not manifest_path.is_file():
            raise FileNotFoundError(
                f"Export manifest file not found at {manifest_path}"
            )

        with open(manifest_path, "r", encoding="utf-8") as manifest_json:
            return json.load(manifest_json)

    def _load_sumo_uploads(self) -> list[dict[str, Any]]:
        """Load the log of previous uploads from file, if it exists."""

        uploads_path = self._sumo_uploads_path

        if not uploads_path.is_file():
            return []

        with open(uploads_path, "r", encoding="utf-8") as uploads_json:
            return json.load(uploads_json)

    def _update_sumo_uploads(self) -> None:
        """Append an entry to the log of previous uploads."""

        manifest = self._load_export_manifest()
        uploads_path = self._sumo_uploads_path
        sumo_uploads = self._load_sumo_uploads()
        new_entry = {
            "last_index_manifest": len(manifest) - 1,
            "timestamp": manifest[-1]["exported_at"],
        }
        sumo_uploads.append(new_entry)

        with open(uploads_path, "w", encoding="utf-8") as uploads_json:
            json.dump(sumo_uploads, uploads_json, indent=4)

        logger.info(
            f"Sumo log {uploads_path} updated with new entry: {new_entry}"
        )


def _load_case_metadata(case_metadata_path: Path) -> dict[str, Any]:
    """Load the case metadata."""

    if not case_metadata_path.is_file():
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


def _is_empty(value: Any) -> bool:
    """Return True for None or an empty str/dict/list/tuple/set, but not for 0."""

    if value is None:
        return True
    if isinstance(value, (str, dict, list, tuple, set)):
        return len(value) == 0
    return False


def _get_log_msg(sumo_parent_id: str, status: UploadResult) -> str:
    """Return a suitable logging for upload issues."""

    obj = {
        "upload_issue": {
            "case_uuid": str(sumo_parent_id),
            "filepath": str(status.get("blob_file_path")),
        }
    }
    if "blob_upload" in status:
        obj["upload_issue"]["blob"] = status["blob_upload"].errinfo()
    elif "metadata_upload" in status:
        obj["upload_issue"]["metadata"] = status["metadata_upload"].errinfo()
    elif "validation" in status:
        obj["upload_issue"]["validation"] = status["validation"].errinfo()
    return json.dumps(obj)


def _get_stats(values: Sequence[float]) -> dict[str, float]:
    if not values:
        return {"count": 0}

    return {
        "mean": statistics.mean(values),
        "max": max(values),
        "min": min(values),
        "sum": sum(values),
        "std": statistics.stdev(values) if len(values) > 1 else 0.0,
    }


def _calculate_upload_stats(uploads: list[UploadResult]) -> dict[str, Any]:
    """Calculate upload statistics.

    Given a list of results from file upload, calculate and return
    timing statistics for uploads."""

    return {
        "blob": {
            "upload_time": _get_stats(
                [u["blob_upload"].elapsed for u in uploads]
            ),
            "upload_retries": _get_stats(
                [u["blob_upload"].retries for u in uploads]
            ),
        },
        "metadata": {
            "upload_time": _get_stats(
                [u["metadata_upload"].elapsed for u in uploads]
            ),
            "upload_retries": _get_stats(
                [u["metadata_upload"].retries for u in uploads]
            ),
        },
    }


def _get_retries(
    ok_uploads: list[UploadResult],
    failed_uploads: list[UploadResult],
    rejected_uploads: list[UploadResult],
) -> tuple[list[int], list[int]]:
    """Get retries for uploads.

    Given lists of ok, failed and rejected uploads, return the non-zero
    retry counts for metadata and blob uploads."""

    all_uploads = ok_uploads + failed_uploads + rejected_uploads

    md_retries = [
        u["metadata_upload"].retries
        for u in all_uploads
        if "metadata_upload" in u
    ]
    blob_retries = [
        u["blob_upload"].retries for u in all_uploads if "blob_upload" in u
    ]

    return [r for r in md_retries if r > 0], [r for r in blob_retries if r > 0]
