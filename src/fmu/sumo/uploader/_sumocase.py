"""

Base class for CaseOnJob and CaseOnDisk classes.

"""

import json
import os
import statistics
import time
import warnings

from fmu.dataio.manifest import get_manifest_path
from fmu.sumo.uploader._logger import get_uploader_logger
from fmu.sumo.uploader._upload_files import upload_files
from fmu.sumo.uploader._utils import (
    get_field_from_metadata,
    sanitize_datetimes,
)

# pylint: disable=C0103 # allow non-snake case variable names

logger = get_uploader_logger()


class SumoCase:
    def __init__(
        self,
        case_metadata: dict,
        sumoclient,
        verbosity="WARNING",
        sumo_mode="copy",
        config_path="fmuconfig/output/global_variables.yml",
        casepath=None,
    ):
        logger.setLevel(verbosity)
        self.sumoclient = sumoclient
        self.case_metadata = sanitize_datetimes(case_metadata)
        self.casepath = casepath
        self._fmu_case_uuid = get_field_from_metadata(
            self.case_metadata, "fmu.case.uuid"
        )
        self._ensemble_uuid = os.environ.get(
            "_ERT_ENSEMBLE_ID", "default_ensemble"
        )
        self._realization_id = int(
            os.environ.get("_ERT_REALIZATION_NUMBER", 0)
        )
        logger.debug("self._fmu_case_uuid is %s", self._fmu_case_uuid)
        self._sumo_parent_id = self._fmu_case_uuid
        self.config_path = config_path
        logger.debug("self._sumo_parent_id is %s", self._sumo_parent_id)
        self._files = []
        self.sumo_mode = sumo_mode

        return

    def _load_export_manifest(self):
        """Load export manifest from file."""

        manifest_path = get_manifest_path(self.casepath)
        logger.info(f"Loading export manifest from {manifest_path}")

        if not os.path.exists(manifest_path):
            raise FileNotFoundError(
                f"Export manifest file not found at {manifest_path}"
            )

        with open(manifest_path, "r", encoding="utf-8") as file:
            return json.load(file)

    def _load_sumo_uploads(self):
        """Load sumo uploads log from file."""

        uploads_path = (
            get_manifest_path(self.casepath).parent / ".sumo_uploads.json"
        )

        if not os.path.exists(uploads_path):
            return []

        with open(uploads_path, "r", encoding="utf-8") as uploads_json:
            return json.load(uploads_json)

    def upload(self):
        """Trigger upload of files.

        Upload all indexed files. Collect the files that have been uploaded OK, the
        ones that have failed and the ones that have been rejected.

        Retry the failed uploads X times."""

        if not self.files:
            err_msg = "No files to upload."
            logger.warning(err_msg)
            return {}

        ok_uploads = []
        failed_uploads = []
        rejected_uploads = []
        files_to_upload = list(self.files)

        _t0 = time.perf_counter()

        logger.debug("files_to_upload: %s", files_to_upload)

        sumoclient = self.sumoclient.client_for_case(self._sumo_parent_id)

        upload_results = upload_files(
            files_to_upload,
            self._sumo_parent_id,
            sumoclient,
            self.sumo_mode,
            self.config_path,
        )
        ok_uploads += upload_results.get("ok_uploads", [])
        failed_uploads += upload_results.get("failed_uploads", [])
        rejected_uploads += upload_results.get("rejected_uploads", [])

        if rejected_uploads and any(
            res.get("metadata_upload").statuscode == 404
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

        _dt = time.perf_counter() - _t0

        md_retries, blob_retries = _get_retries(
            ok_uploads, failed_uploads, rejected_uploads
        )
        if len(md_retries) > 0 or len(blob_retries) > 0:
            self._sumo_logger.warning(
                "UploadRetries: Some uploads required retries. Case %s, Ensemble %s, Realization %d. Metadata retries: %d, Blob retries: %d",
                self._fmu_case_uuid,
                self._ensemble_uuid,
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

        upload_statistics = ""
        if len(ok_uploads) > 0:
            upload_statistics = _calculate_upload_stats(ok_uploads)
            logger.info(upload_statistics)
            self._update_sumo_uploads()

        if rejected_uploads:
            logger.info(
                f"\n\n{len(rejected_uploads)} files rejected by Sumo. First 5 rejected files:"
            )

            for u in rejected_uploads[0:4]:
                logger.info(_get_log_msg(self.sumo_parent_id, u))
                self._sumo_logger.error(
                    _get_log_msg(self.sumo_parent_id, u),
                    extra={"objectUuid": self._sumo_parent_id},
                )

        if failed_uploads:
            logger.info(
                f"\n\n{len(failed_uploads)} files failed by Sumo. First 5 failed files:"
            )

            for u in failed_uploads[0:4]:
                logger.info(_get_log_msg(self.sumo_parent_id, u))
                self._sumo_logger.error(
                    _get_log_msg(self.sumo_parent_id, u),
                    extra={"objectUuid": self._sumo_parent_id},
                )

        logger.info("Summary:")
        logger.info("Total files count: %s", str(len(files_to_upload)))
        logger.info("OK: %s", str(len(ok_uploads)))
        logger.info("Failed: %s", str(len(failed_uploads)))
        logger.info("Rejected: %s", str(len(rejected_uploads)))
        logger.info(f"Wall time: {_dt:.2f} sec")
        logger.info(f"Sumo mode: {str(self.sumo_mode)}")

        details = {
            "case_uuid": self._fmu_case_uuid,
            "total_files_count": len(self.files),
            "ok_files": len(ok_uploads),
            "failed_files": len(failed_uploads),
            "rejected_files": len(rejected_uploads),
            "wall_time_seconds": _dt,
            "upload_statistics": upload_statistics,
            "sumo_mode": self.sumo_mode,
        }

        self._sumo_logger.info(
            "Upload completed for case with fmu_case_uuid: %s",
            self._fmu_case_uuid,
            extra={"objectUuid": self._fmu_case_uuid, "details": details},
        )

        return ok_uploads

    pass

    def _update_sumo_uploads(self):
        """Update sumo uploads log."""

        manifest = self._load_export_manifest()
        uploads_path = (
            get_manifest_path(self.casepath).parent / ".sumo_uploads.json"
        )
        sumo_uploads = self._load_sumo_uploads()
        new_entry = {
            "last_index_manifest": len(manifest) - 1,
            "timestamp": manifest[-1]["exported_at"],
        }
        sumo_uploads.append(new_entry)

        with open(uploads_path, "w") as file:
            json.dump(sumo_uploads, file, indent=4)

        logger.info(
            f"Sumo log {uploads_path} updated with new entry: {new_entry}"
        )


def _get_log_msg(sumo_parent_id, status):
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


def _get_stats(values):
    return (
        {
            "count": len(values),
            "mean": statistics.mean(values),
            "max": max(values),
            "min": min(values),
            "sum": sum(values),
            "std": statistics.stdev(values) if len(values) > 1 else 0.0,
        }
        if len(values) > 0
        else {"count": 0}
    )


def _calculate_upload_stats(uploads):
    """Calculate upload statistics.

    Given a list of results from file upload, calculate and return
    timing statistics for uploads."""

    blob_upload_times = [u["blob_upload"].elapsed for u in uploads]
    blob_upload_retries = [u["blob_upload"].retries for u in uploads]
    metadata_upload_times = [u["metadata_upload"].elapsed for u in uploads]
    metdata_upload_retries = [u["metadata_upload"].retries for u in uploads]

    stats = {
        "blob": {
            "upload_time": _get_stats(blob_upload_times),
            "upload_retries": _get_stats(blob_upload_retries),
        },
        "metadata": {
            "upload_time": _get_stats(metadata_upload_times),
            "upload_retries": _get_stats(metdata_upload_retries),
        },
    }

    return stats


def _get_retries(ok_uploads, failed_uploads, rejected_uploads):
    """Get retries for uploads.

    Given lists of ok, failed and rejected uploads, return the retries for
    metadata and blob uploads."""

    md_retries = [
        u["metadata_upload"].retries
        for u in ok_uploads + failed_uploads + rejected_uploads
        if "metadata_upload" in u
    ]
    blob_retries = [
        u["blob_upload"].retries
        for u in ok_uploads + failed_uploads + rejected_uploads
        if "blob_upload" in u
    ]

    return [r for r in md_retries if r > 0], [r for r in blob_retries if r > 0]
