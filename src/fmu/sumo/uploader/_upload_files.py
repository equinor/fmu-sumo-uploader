"""

The function that uploads files.

"""

from __future__ import annotations

import asyncio
import os
from copy import deepcopy
from typing import TYPE_CHECKING, Any, TypedDict

import httpx

from fmu.sumo.uploader._logger import get_uploader_logger
from fmu.sumo.uploader._utils import get_host_and_domain_names

if TYPE_CHECKING:
    from sumo.wrapper import SumoClient

    from fmu.sumo.uploader._sumofile import SumoFile, UploadResult

# pylint: disable=C0103 # allow non-snake case variable names


logger = get_uploader_logger()

# On these on-premise domains, parallel uploads are counter-productive.
_SINGLE_UPLOAD_DOMAINS = frozenset({"rio.statoil.no", "stjohn.statoil.no"})


class UploadResults(TypedDict):
    """Results of a batch of file uploads, grouped by outcome."""

    ok_uploads: list[UploadResult]
    failed_uploads: list[UploadResult]
    rejected_uploads: list[UploadResult]


def get_ert_env(name: str) -> str | None:
    return os.getenv(f"_ERT_{name}")


def _base_object_metadata(base_metadata: dict[str, Any]) -> dict[str, Any]:
    """Strip data-object fields to prepare realization/ensemble metadata"""
    metadata = deepcopy(base_metadata)
    del metadata["data"]
    del metadata["file"]
    del metadata["display"]
    metadata["_sumo"] = {}
    # Realization and Ensemble objects should always be internal
    metadata["access"]["classification"] = "internal"
    return metadata


def _get_existing_classes(
    sumoclient: SumoClient, uuids: list[str]
) -> set[str]:
    """Return the classes of the objects that already exist on Sumo."""
    hits = sumoclient.post(
        "/search",
        json={
            "query": {"ids": {"values": uuids}},
            "_source": ["class"],
        },
    ).json()["hits"]["hits"]

    return {hit["_source"]["class"] for hit in hits}


def maybe_upload_realization_and_ensemble(
    sumoclient: SumoClient, base_metadata: dict[str, Any]
) -> None:
    realization_uuid = base_metadata["fmu"]["realization"]["uuid"]
    ensemble_uuid = base_metadata["fmu"]["ensemble"]["uuid"]

    classes = _get_existing_classes(
        sumoclient, [realization_uuid, ensemble_uuid]
    )

    if "realization" in classes:
        return

    realization_metadata = _base_object_metadata(base_metadata)
    del realization_metadata["fmu"]["entity"]
    realization_metadata["class"] = "realization"
    realization_metadata["fmu"]["context"]["stage"] = "realization"

    case_uuid = realization_metadata["fmu"]["case"]["uuid"]

    if "ensemble" not in classes:
        ensemble_metadata = deepcopy(realization_metadata)
        del ensemble_metadata["fmu"]["realization"]
        ensemble_metadata["class"] = "ensemble"
        ensemble_metadata["fmu"]["context"]["stage"] = "ensemble"
        ensemble_metadata["_sumo"]["status"] = "scratch"
        sumoclient.post(f"/objects('{case_uuid}')", json=ensemble_metadata)

    sumoclient.post(f"/objects('{case_uuid}')", json=realization_metadata)


def maybe_upload_ensemble(
    sumoclient: SumoClient, base_metadata: dict[str, Any]
) -> None:
    ensemble_uuid = base_metadata["fmu"]["ensemble"]["uuid"]

    classes = _get_existing_classes(sumoclient, [ensemble_uuid])

    if "ensemble" in classes:
        return

    ensemble_metadata = _base_object_metadata(base_metadata)
    ensemble_metadata["class"] = "ensemble"
    ensemble_metadata["fmu"]["context"]["stage"] = "ensemble"
    ensemble_metadata["_sumo"]["status"] = "scratch"

    case_uuid = ensemble_metadata["fmu"]["case"]["uuid"]
    sumoclient.post(f"/objects('{case_uuid}')", json=ensemble_metadata)


def _log_context_upload_exception(err: Exception) -> None:
    """Log a failure to create the realization/ensemble objects."""
    err = err.with_traceback(None)

    if isinstance(err, httpx.HTTPStatusError):
        error_string = (
            str(err.response.status_code)
            + err.response.reason_phrase
            + err.response.text
        )
        logger.warning(
            f"Metadata upload status error exception: {error_string}"
        )
    else:
        logger.warning(f"Metadata upload exception {err} {type(err)}")


def _maybe_upload_context_objects(
    sumoclient: SumoClient, base_metadata: dict[str, Any]
) -> None:
    """Create the realization and/or ensemble the files belong to, if needed.

    Failures are logged rather than raised, so that a missing context object
    does not stop the file uploads.
    """

    # Use environment variables to get context
    real_num = get_ert_env("REALIZATION_NUMBER")
    ensemble_id = get_ert_env("ENSEMBLE_ID")

    try:
        # Realization context
        if real_num is not None:
            maybe_upload_realization_and_ensemble(sumoclient, base_metadata)

        # Ensemble context. Ensembles are associated with an iteration but may
        # lack an ERT iteration number env var depending on when this function
        # is called in the workflow. For example, when this function is called
        # before simulation start, the iteration number env var is not yet
        # defined. Ensembles always have an ensemble_id env var.
        elif ensemble_id is not None:
            maybe_upload_ensemble(sumoclient, base_metadata)
    except Exception as err:
        _log_context_upload_exception(err)


def _get_batch_size() -> int:
    _, domain_name = get_host_and_domain_names()
    return 1 if domain_name in _SINGLE_UPLOAD_DOMAINS else 10


async def _upload_files(
    files: list[SumoFile],
    sumoclient: SumoClient,
    sumo_parent_id: str,
    sumo_mode: str = "copy",
) -> list[UploadResult]:
    """
    Upload realization and ensemble objects if they do not exist
    Create threads and call _upload in each thread
    """
    batch_size = _get_batch_size()
    logger.info(f"batch_size={batch_size}")

    if files:
        _maybe_upload_context_objects(sumoclient, files[0].metadata)

    all_results: list[UploadResult] = []
    for i in range(0, len(files), batch_size):
        batch = files[i : i + batch_size]
        tasks = [
            _upload_file(file, sumoclient, sumo_parent_id, sumo_mode)
            for file in batch
        ]
        results = await asyncio.gather(*tasks)
        all_results.extend(results)

    return all_results


async def _upload_file(
    file: SumoFile, sumoclient: SumoClient, sumo_parent_id: str, sumo_mode: str
) -> UploadResult:
    """Upload a file"""

    result = await file.upload_to_sumo(
        sumoclient=sumoclient,
        sumo_parent_id=sumo_parent_id,
        sumo_mode=sumo_mode,
    )

    result["file"] = file

    return result


def upload_files(
    files: list[SumoFile],
    sumo_parent_id: str,
    sumoclient: SumoClient,
    sumo_mode: str = "copy",
) -> UploadResults:
    """
    Upload files

    files: list of FileOnDisk objects
    sumo_parent_id: sumo_parent_id for the parent case

    Upload is kept outside classes to use multithreading.
    """

    results = asyncio.run(
        _upload_files(
            files,
            sumoclient,
            sumo_parent_id,
            sumo_mode,
        )
    )

    grouped: UploadResults = {
        "ok_uploads": [],
        "failed_uploads": [],
        "rejected_uploads": [],
    }

    for result in results:
        status = result.get("status")

        if not status:
            raise ValueError(
                'File upload result returned with no "status" attribute'
            )

        if status == "ok":
            grouped["ok_uploads"].append(result)
        elif status == "rejected":
            grouped["rejected_uploads"].append(result)
        else:
            grouped["failed_uploads"].append(result)

    return grouped
