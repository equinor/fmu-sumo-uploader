"""

The function that uploads files.

"""

import asyncio
import os
from copy import deepcopy

import httpx

from fmu.sumo.uploader._logger import get_uploader_logger

# pylint: disable=C0103 # allow non-snake case variable names


logger = get_uploader_logger()


def _base_object_metadata(base_metadata):
    """Strip data-object fields to prepare realization/ensemble metadata"""
    metadata = deepcopy(base_metadata)
    del metadata["data"]
    del metadata["file"]
    del metadata["display"]
    metadata["_sumo"] = {}
    # Realization and Ensemble objects should always be internal
    metadata["access"]["classification"] = "internal"
    return metadata


def maybe_upload_realization_and_ensemble(sumoclient, base_metadata):
    realization_uuid = base_metadata["fmu"]["realization"]["uuid"]
    ensemble_uuid = base_metadata["fmu"]["ensemble"]["uuid"]

    hits = sumoclient.post(
        "/search",
        json={
            "query": {"ids": {"values": [realization_uuid, ensemble_uuid]}},
            "_source": ["class"],
        },
    ).json()["hits"]["hits"]

    classes = [hit["_source"]["class"] for hit in hits]

    if "realization" not in classes:
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
            sumoclient.post(f"/objects('{case_uuid}')", json=ensemble_metadata)

        sumoclient.post(f"/objects('{case_uuid}')", json=realization_metadata)


def maybe_upload_ensemble(sumoclient, base_metadata):
    ensemble_uuid = base_metadata["fmu"]["ensemble"]["uuid"]

    hits = sumoclient.post(
        "/search",
        json={
            "query": {"ids": {"values": [ensemble_uuid]}},
            "_source": ["class"],
        },
    ).json()["hits"]["hits"]

    classes = [hit["_source"]["class"] for hit in hits]

    if "ensemble" not in classes:
        ensemble_metadata = _base_object_metadata(base_metadata)
        ensemble_metadata["class"] = "ensemble"
        ensemble_metadata["fmu"]["context"]["stage"] = "ensemble"

        case_uuid = ensemble_metadata["fmu"]["case"]["uuid"]
        sumoclient.post(f"/objects('{case_uuid}')", json=ensemble_metadata)


def _get_batch_size():
    nodename = os.uname().nodename
    nameparts = nodename.split(".", 1)
    domainname = nameparts[1] if len(nameparts) > 1 else ""
    if domainname in ["rio.statoil.no", "stjohn.statoil.no"]:
        batch_size = 1
    else:
        batch_size = 10
    return batch_size


async def _upload_files(
    files,
    sumoclient,
    sumo_parent_id,
    sumo_mode="copy",
    config_path="fmuconfig/output/global_variables.yml",
):
    """
    Upload realization and ensemble objects if they do not exist
    Create threads and call _upload in each thread
    """
    batch_size = _get_batch_size()
    logger.info(f"batch_size={batch_size}")

    for file in files:
        if "fmu" in file.metadata and "realization" in file.metadata["fmu"]:
            try:
                maybe_upload_realization_and_ensemble(
                    sumoclient, file.metadata
                )
            except httpx.HTTPStatusError as err:
                err = err.with_traceback(None)
                error_string = (
                    str(err.response.status_code)
                    + err.response.reason_phrase
                    + err.response.text
                )
                logger.warning(
                    f"Metadata upload status error exception: {error_string}"
                )
                pass
            except Exception as err:
                err = err.with_traceback(None)
                logger.warning(f"Metadata upload exception {err} {type(err)}")
                pass

            break
    else:
        for file in files:
            if "fmu" in file.metadata and "ensemble" in file.metadata["fmu"]:
                try:
                    maybe_upload_ensemble(sumoclient, file.metadata)
                except httpx.HTTPStatusError as err:
                    err = err.with_traceback(None)
                    error_string = (
                        str(err.response.status_code)
                        + err.response.reason_phrase
                        + err.response.text
                    )
                    logger.warning(
                        f"Metadata upload status error exception: {error_string}"
                    )
                    pass
                except Exception as err:
                    err = err.with_traceback(None)
                    logger.warning(
                        f"Metadata upload exception {err} {type(err)}"
                    )
                    pass

                break
    all_results = []
    for i in range(0, len(files), batch_size):
        batch = files[i : i + batch_size]
        tasks = [
            _upload_file((file, sumoclient, sumo_parent_id, sumo_mode))
            for file in batch
        ]
        results = await asyncio.gather(*tasks)
        all_results.extend(results)

    return all_results


async def _upload_file(args):
    """Upload a file"""

    file, sumoclient, sumo_parent_id, sumo_mode = args

    result = await file.upload_to_sumo(
        sumoclient=sumoclient,
        sumo_parent_id=sumo_parent_id,
        sumo_mode=sumo_mode,
    )

    result["file"] = file

    return result


def upload_files(
    files: list,
    sumo_parent_id: str,
    sumoclient,
    sumo_mode="copy",
    config_path="fmuconfig/output/global_variables.yml",
):
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
            config_path,
        )
    )

    ok_uploads = []
    failed_uploads = []
    rejected_uploads = []

    for r in results:
        status = r.get("status")

        if not status:
            raise ValueError(
                'File upload result returned with no "status" attribute'
            )

        if status == "ok":
            ok_uploads.append(r)

        elif status == "rejected":
            rejected_uploads.append(r)

        else:
            failed_uploads.append(r)

    return {
        "ok_uploads": ok_uploads,
        "failed_uploads": failed_uploads,
        "rejected_uploads": rejected_uploads,
    }
