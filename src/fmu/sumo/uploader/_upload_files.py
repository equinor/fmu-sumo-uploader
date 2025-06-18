"""

The function that uploads files.

"""

import json
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

import yaml

from fmu.dataio._utils import read_parameters_txt
from fmu.dataio.dataio import ExportData
from fmu.sumo.uploader._fileonjob import FileOnJob
from fmu.sumo.uploader._logger import get_uploader_logger

# pylint: disable=C0103 # allow non-snake case variable names


logger = get_uploader_logger()


def get_parameter_file(parameters_path, config_path):
    """Return a parameters object from the parameters.txt file

    Args:
        case_uuid (str): parent uuid for case
        realization_id (str): the id of the realization
        parameters_path (str): path to the parameters.txt file
        config_path (str): path to the fmu config file
        sumoclient (SumoClient): Initialized sumo client for performing query

    Returns:
        SumoFile: parameters ready for upload, or None
    """

    bytestring = None
    metadata = None

    try:
        with open(config_path, "r", encoding="utf-8") as variables_yml:
            global_config = yaml.safe_load(variables_yml)
    except FileNotFoundError:
        logger.warning(
            "No fmu config to read at %s, cannot generate metadata to upload parameters",
            config_path,
        )
        return None

    try:
        parameters = read_parameters_txt(parameters_path)
    except FileNotFoundError:
        logger.warning(
            "No parameters file to read at %s, no file to upload.",
            parameters_path,
        )
        return None

    exd = ExportData(
        config=global_config, content="parameters", name="parameters"
    )
    metadata = exd.generate_metadata(parameters)

    if "fmu" not in metadata:
        logger.warning("No fmu section in metadata. Cannot upload parameters.")
        return None

    bytestring = json.dumps(parameters).encode("utf-8")
    paramfile = FileOnJob(bytestring, metadata)
    paramfile.metadata_path = ""
    paramfile.path = ""
    paramfile.size = len(bytestring)
    return paramfile


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
        realization_metadata = deepcopy(base_metadata)
        del realization_metadata["data"]
        del realization_metadata["file"]
        del realization_metadata["display"]
        realization_metadata["_sumo"] = {}
        realization_metadata["class"] = "realization"
        realization_metadata["fmu"]["context"]["stage"] = "realization"

        case_uuid = realization_metadata["fmu"]["case"]["uuid"]

        if "ensemble" not in classes:
            ensemble_metadata = deepcopy(realization_metadata)
            del ensemble_metadata["fmu"]["realization"]
            del ensemble_metadata["fmu"]["entity"]
            ensemble_metadata["class"] = "ensemble"
            ensemble_metadata["fmu"]["context"]["stage"] = "ensemble"
            sumoclient.post(f"/objects('{case_uuid}')", json=ensemble_metadata)

        sumoclient.post(f"/objects('{case_uuid}')", json=realization_metadata)


def _upload_files(
    files,
    sumoclient,
    sumo_parent_id,
    threads=4,
    sumo_mode="copy",
    config_path="fmuconfig/output/global_variables.yml",
    parameters_path="parameters.txt",
):
    """
    Upload realization and ensemble objects if they do not exist
    Upload parameters file if it does not exist or it has changed
    Create threads and call _upload in each thread
    """

    for file in files:
        if "fmu" in file.metadata and "realization" in file.metadata["fmu"]:
            realization_id = file.metadata["fmu"]["realization"]["uuid"]

            try:
                maybe_upload_realization_and_ensemble(
                    sumoclient, file.metadata
                )
            except Exception as e:
                logger.error(
                    "Failed to upload realization and ensemble objects: %s",
                    e.with_traceback(None),
                )

            paramfile = get_parameter_file(parameters_path, config_path)
            if paramfile is not None:
                query = f"fmu.case.uuid:{sumo_parent_id} AND fmu.realization.uuid:{realization_id} AND data.content:parameters"
                search_res = sumoclient.get(
                    "/search", {"$query": query, "$select": "_sumo.blob_md5"}
                ).json()
                # Check if the parameters file does not exist or has changed
                if (
                    search_res["hits"]["total"]["value"] == 0
                    or search_res["hits"]["hits"][0]["_source"]["_sumo"][
                        "blob_md5"
                    ]
                    != paramfile.metadata["_sumo"]["blob_md5"]
                ):
                    files.append(paramfile)
                    logger.info("Parameters file will be uploaded")

            break

    with ThreadPoolExecutor(threads) as executor:
        results = executor.map(
            _upload_file,
            [(file, sumoclient, sumo_parent_id, sumo_mode) for file in files],
        )

    return results


def _upload_file(args):
    """Upload a file"""

    file, sumoclient, sumo_parent_id, sumo_mode = args

    result = file.upload_to_sumo(
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
    threads=4,
    sumo_mode="copy",
    config_path="fmuconfig/output/global_variables.yml",
    parameters_path="parameters.txt",
):
    """
    Upload files

    files: list of FileOnDisk objects
    sumo_parent_id: sumo_parent_id for the parent case

    Upload is kept outside classes to use multithreading.
    """

    results = _upload_files(
        files,
        sumoclient,
        sumo_parent_id,
        threads,
        sumo_mode,
        config_path,
        parameters_path,
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
