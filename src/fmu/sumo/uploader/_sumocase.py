"""

Base class for CaseOnJob and CaseOnDisk classes.

"""

import datetime
import statistics
import time
import warnings

from fmu.sumo.uploader._logger import get_uploader_logger
from fmu.sumo.uploader._upload_files import upload_files

# pylint: disable=C0103 # allow non-snake case variable names

logger = get_uploader_logger()


class SumoCase:
    def __init__(
        self,
        case_metadata: str,
        sumoclient,
        verbosity="WARNING",
        sumo_mode="copy",
        config_path="fmuconfig/output/global_variables.yml",
        parameters_path="parameters.txt",
    ):
        logger.setLevel(verbosity)
        self.sumoclient = sumoclient
        self.case_metadata = _sanitize_datetimes(case_metadata)
        self._fmu_case_uuid = self._get_fmu_case_uuid()
        logger.debug("self._fmu_case_uuid is %s", self._fmu_case_uuid)
        self._sumo_parent_id = self._fmu_case_uuid
        self.config_path = config_path
        self.parameters_path = parameters_path
        logger.debug("self._sumo_parent_id is %s", self._sumo_parent_id)
        self._files = []
        self.sumo_mode = sumo_mode

        return

    def _get_fmu_case_uuid(self):
        """Return case_id from case_metadata."""
        fmu_case_uuid = None
        try:
            if (
                self.case_metadata
                and self.case_metadata.get("fmu")
                and self.case_metadata.get("fmu").get("case")
            ):
                fmu_case_uuid = (
                    self.case_metadata.get("fmu").get("case").get("uuid")
                )
            if not fmu_case_uuid:
                err_msg = "Invalid metadata: Could not get fmu.case.uuid from case metadata"
                warnings.warn(err_msg)
                return None
            else:
                return fmu_case_uuid
        except Exception as err:
            err_msg = f"Invalid metadata: Could not get fmu.case.uuid from case metadata: {err} {type(err)}"
            warnings.warn(err_msg)
            return None

    def upload(self, threads=4):
        """Trigger upload of files.

        Upload all indexed files. Collect the files that have been uploaded OK, the
        ones that have failed and the ones that have been rejected.

        Retry the failed uploads X times."""

        if not self.files:
            err_msg = "No files to upload. Check search string."
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
            threads,
            self.sumo_mode,
            self.config_path,
            self.parameters_path,
        )

        ok_uploads += upload_results.get("ok_uploads")
        rejected_uploads += upload_results.get("rejected_uploads")
        failed_uploads = upload_results.get("failed_uploads")

        if rejected_uploads and any(
            res.get("metadata_upload_response_status_code") in [404]
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

        upload_statistics = ""
        if len(ok_uploads) > 0:
            upload_statistics = _calculate_upload_stats(ok_uploads)
            logger.info(upload_statistics)

        if rejected_uploads:
            logger.info(
                f"\n\n{len(rejected_uploads)} files rejected by Sumo. First 5 rejected files:"
            )

            for u in rejected_uploads[0:4]:
                logger.info("\n" + "=" * 50)

                logger.info(f"Filepath: {u.get('blob_file_path')}")
                logger.info(
                    f"Metadata: [{u.get('metadata_upload_response_status_code')}] "
                    f"{u.get('metadata_upload_response_text')}"
                )
                logger.info(
                    f"Blob: [{u.get('blob_upload_response_status_code')}] "
                    f"{u.get('blob_upload_response_status_text')}"
                )
                self._sumo_logger.info(
                    _get_log_msg(self.sumo_parent_id, u),
                    extra={"objectUuid": self._sumo_parent_id},
                )

        if failed_uploads:
            logger.info(
                f"\n\n{len(failed_uploads)} files failed by Sumo. First 5 failed files:"
            )

            for u in failed_uploads[0:4]:
                logger.info("\n" + "=" * 50)

                logger.info(f"Filepath: {u.get('blob_file_path')}")
                logger.info(
                    f"Metadata: [{u.get('metadata_upload_response_status_code')}] "
                    f"{u.get('metadata_upload_response_text')}"
                )
                logger.info(
                    f"Blob: [{u.get('blob_upload_response_status_code')}] "
                    f"{u.get('blob_upload_response_status_text')}"
                )
                self._sumo_logger.info(
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

        summary = {
            "upload_summary": {
                "parent_id": self.sumo_parent_id,
                "total_files_count": str(len(self.files)),
                "ok_files": str(len(ok_uploads)),
                "failed_files": str(len(failed_uploads)),
                "rejected_files": str(len(rejected_uploads)),
                "wall_time_seconds": str(_dt),
                "upload_statistics": upload_statistics,
                "sumo_mode": self.sumo_mode,
            }
        }
        self._sumo_logger.info(
            str(summary), extra={"objectUuid": self._sumo_parent_id}
        )

        return ok_uploads

    pass


def _get_log_msg(sumo_parent_id, status):
    """Return a suitable logging for upload issues."""

    json = {
        "upload_issue": {
            "case_uuid": str(sumo_parent_id),
            "filepath": str(status.get("blob_file_path")),
            "metadata": {
                "status_code": str(
                    status.get("metadata_upload_response_status_code")
                ),
                "response_text": status.get("metadata_upload_response_text"),
            },
            "blob": {
                "status_code": str(
                    status.get("blob_upload_response_status_code")
                ),
                "response_text": (
                    status.get("blob_upload_response_status_text")
                ),
            },
        }
    }
    return json


def _calculate_upload_stats(uploads):
    """Calculate upload statistics.

    Given a list of results from file upload, calculate and return
    timing statistics for uploads."""

    blob_upload_times = [u["blob_upload_time_elapsed"] for u in uploads]
    metadata_upload_times = [
        u["metadata_upload_time_elapsed"] for u in uploads
    ]

    def _get_stats(values):
        return {
            "mean": statistics.mean(values),
            "max": max(values),
            "min": min(values),
            "std": statistics.stdev(values) if len(values) > 1 else 0.0,
        }

    stats = {
        "blob": {
            "upload_time": _get_stats(blob_upload_times),
        },
        "metadata": {
            "upload_time": _get_stats(metadata_upload_times),
        },
    }

    return stats


def _sanitize_datetimes(data):
    """Sanitize datetimes.

    Given a dictionary, find and replace all datetime objects
    with isoformat string, so that it does not cause problems for
    JSON later on."""

    if isinstance(data, datetime.datetime):
        return data.isoformat()

    if isinstance(data, dict):
        for key in data:
            data[key] = _sanitize_datetimes(data[key])

    elif isinstance(data, list):
        data = [_sanitize_datetimes(element) for element in data]

    return data
