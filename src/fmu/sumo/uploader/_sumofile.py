"""

Base class for FileOnJob and FileOnDisk classes.

"""

import functools
import math
import os
import re
import subprocess
import sys
import time
import warnings

import httpx
import tenacity as tn
from azure.storage.blob import BlobClient, ContentSettings

from fmu.sumo.uploader._logger import get_uploader_logger

_max_single_put_size = 4 * 1024 * 1024

# pylint: disable=C0103 # allow non-snake case variable names

logger = get_uploader_logger()


def is_seismic(metadata):
    return (
        metadata.get("data")
        and metadata.get("data").get("format")
        and metadata.get("data").get("format") in ["openvds", "segy"]
    )


class ResponseInfo:
    def __init__(self, result, err, statuscode, t0, t1):
        self.result = result
        self.err = err
        self.statuscode = statuscode
        self.t0 = t0
        self.elapsed = t1 - t0

    def ok(self):
        return self.result is not None and self.err is None

    def errinfo(self):
        return {"err": self.err, "statuscode": self.statuscode}


def upload_response(func):
    """Decorator to wrap upload functions and return a consistent response format"""

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        t0 = time.perf_counter()
        try:
            result = await func(*args, **kwargs)
            return ResponseInfo(result, None, 0, t0, time.perf_counter())
        except (httpx.TimeoutException, httpx.ConnectError) as err:
            err = err.with_traceback(None)
            logger.error(
                f"HTTP connect/timeout error during upload: {err} {type(err)}"
            )
            return ResponseInfo(None, str(err), 500, t0, time.perf_counter())
        except httpx.HTTPStatusError as err:
            err = err.with_traceback(None)
            logger.error("HTTP status error during upload: {err} {type(err)}")
            return ResponseInfo(
                None,
                str(err),
                err.response.status_code,
                t0,
                time.perf_counter(),
            )
        except Exception as err:
            err = err.with_traceback(None)
            logger.error(f"Error during upload: {err} {type(err)}")
            return ResponseInfo(None, str(err), 500, t0, time.perf_counter())

    return wrapper


@upload_response
async def upload_metadata(sumoclient, sumo_parent_id, metadata):
    """Upload metadata to Sumo and return a consistent response format"""
    path = f"/objects('{sumo_parent_id}')"
    response = await sumoclient.post_async(path=path, json=metadata)
    response.raise_for_status()
    return response.json()


def get_blob_client(blob_url):
    blobclient = BlobClient.from_blob_url(
        blob_url,
        connection_timeout=600,
        read_timeout=600,
        max_single_put_size=_max_single_put_size,
    )
    return blobclient


@upload_response
@tn.retry(
    stop=tn.stop_after_attempt(6),
    wait=tn.wait_exponential(multiplier=1, exp_base=2),
)
async def upload_blob(blob_url, byte_string):
    """Upload blob to Azure and return a consistent response format"""
    blobclient = get_blob_client(blob_url)
    content_settings = ContentSettings(content_type="application/octet-stream")
    # set a timeout of 10s per megabyte, and at least 30s
    timeout = max(math.ceil(len(byte_string) / (1024 * 1024) * 10), 30)
    blobclient.upload_blob(
        byte_string,
        blob_type="BlockBlob",
        length=len(byte_string),
        overwrite=True,
        content_settings=content_settings,
        timeout=timeout,
    )
    # response has the form {'etag': '"0x8DCDC8EED1510CC"', 'last_modified': datetime.datetime(2024, 9, 24, 11, 49, 20, tzinfo=datetime.timezone.utc), 'content_md5': bytearray(b'\x1bPM3(\xe1o\xdf(\x1d\x1f\xb9Qm\xd9\x0b'), 'client_request_id': '08c962a4-7a6b-11ef-8710-acde48001122', 'request_id': 'f459ad2b-801e-007d-1977-0ef6ee000000', 'version': '2024-11-04', 'version_id': None, 'date': datetime.datetime(2024, 9, 24, 11, 49, 19, tzinfo=datetime.timezone.utc), 'request_server_encrypted': True, 'encryption_key_sha256': None, 'encryption_scope': None}
    # ... which is not what the caller expects, so we return something reasonable.
    return True


@upload_response
async def validate(parent_id, metadata):
    """Validate metadata and return a consistent response format"""
    if not parent_id:
        raise Exception("Validation failed: Missing case/sumo_parent_id")
    # ELSE
    file_case_uuid = metadata["fmu"]["case"]["uuid"]
    if parent_id != file_case_uuid:
        raise Exception(
            "Validation failed: File case.uuid does not match parent case.uuid"
        )
    # ELSE
    if is_seismic(metadata) and "vertical_domain" not in metadata["data"]:
        raise Exception(
            "Validation failed: This is a seismic data object but it does not have a value for data.vertical_domain."
        )
    # ELSE
    return True


@functools.cache
def get_path_to_segyimport():
    segy_command = "SEGYImport"
    if sys.platform.startswith("win"):
        segy_command = segy_command + ".exe"
    python_path = os.path.dirname(sys.executable)
    # The SEGYImport folder location is not fixed
    locations = [
        os.path.join(python_path, "bin"),
        os.path.join(python_path, "..", "bin"),
        os.path.join(python_path, "..", "shims"),
        "/home/vscode/.local/bin",
        "/usr/local/bin",
    ]
    for loc in locations:
        path = os.path.join(loc, segy_command)
        if os.path.isfile(path):
            _path_to_segyimport = path
            break
    if _path_to_segyimport is None:
        raise Exception("Could not find OpenVDS executables folder location")
    return _path_to_segyimport


def get_segyimport_cmd(blob_url, object_id, file_path, sample_unit):
    """Return the command string for running OpenVDS SEGYImport"""
    if isinstance(blob_url, str):
        baseuri, auth = blob_url.split("?")
    else:
        baseuri, auth = blob_url["baseuri"], blob_url["auth"]
    url = re.sub("^http(:?s):", "azureSAS:", baseuri)
    url_conn = "Suffix=?" + auth

    persistent_id = object_id

    path_to_executable = get_path_to_segyimport()

    cmd = [
        path_to_executable,
        "--compression-method",
        "RLE",
        "--brick-size",
        "64",
        "--sample-unit",
        sample_unit,
        "--url",
        url,
        "--url-connection",
        url_conn,
        "--persistentID",
        persistent_id,
        file_path,
    ]

    return cmd


@upload_response
async def upload_seismic_blob(object_id, path, metadata, blob_url):
    if sys.platform.startswith("darwin"):
        # OpenVDS does not support Mac/darwin directly
        # Outer code expects and interprets http error codes
        raise Exception(
            "Can not perform SEGY upload since OpenVDS does not support Mac"
        )
    # ELSE - attempt to upload as OpenVDS SEGYImport command
    if metadata["data"]["vertical_domain"] == "depth":
        sample_unit = "m"
    else:
        sample_unit = "ms"  # aka time domain

    cmd_str = get_segyimport_cmd(blob_url, object_id, path, sample_unit)
    try:
        cmd_result = subprocess.run(
            cmd_str, capture_output=True, text=True, shell=False
        )
        if cmd_result.returncode == 0:
            return True
        else:
            # Outer code expects and interprets http error codes
            logger.warning(
                "Seismic upload failed with returncode",
                cmd_result.returncode,
            )
            raise Exception(
                "FAILED SEGY upload as OpenVDS command " + cmd_result.stderr
            )
    except Exception as err:
        err = err.with_traceback(None)
        logger.warning(f"Seismic upload exception {err} {type(err)}")
        raise Exception(
            "FAILED SEGY upload as OpenVDS exception "
            + str(err)
            + " "
            + str(type(err))
        )


class SumoFile:
    def __init__(self):
        return

    async def _delete_metadata(self, sumoclient, object_id):
        logger.warning("Deleting metadata object: %s", object_id)
        path = f"/objects('{object_id}')"
        response = await sumoclient.delete_async(path=path)
        return response

    async def upload_to_sumo(self, sumo_parent_id, sumoclient, sumo_mode):
        """Upload this file to Sumo"""
        # We need these included even if returning before blob upload
        result = {"blob_file_path": self.path, "blob_file_size": self._size}

        result["validation"] = await validate(sumo_parent_id, self.metadata)
        if not result["validation"].ok():
            result["status"] = "rejected"
            return result

        if is_seismic(self.metadata):
            self.metadata["data"]["format"] = (
                "openvds"  # we will upload seismic as openvds format, even if originally segy
            )

        result["metadata_upload"] = await upload_metadata(
            sumoclient, sumo_parent_id, self.metadata
        )
        if not result["metadata_upload"].ok():
            result["status"] = (
                "rejected"
                if result["metadata_upload"].statuscode in range(400, 500)
                else "failed"
            )
            return result

        self.sumo_parent_id = sumo_parent_id
        self.sumo_object_id = result["metadata_upload"].result.get("objectid")

        blob_url = result["metadata_upload"].result.get("blob_url")

        # UPLOAD BLOB

        if is_seismic(self.metadata):
            logger.info(
                "This is a seismic file, will attempt to upload as OpenVDS"
            )
            result["blob_upload"] = await upload_seismic_blob(
                self.sumo_object_id, self.path, self.metadata, blob_url
            )
        else:  # non-seismic blob
            result["blob_upload"] = await upload_blob(
                blob_url, self.byte_string
            )

        if not result["blob_upload"].ok():
            logger.warning(
                "Deleting metadata since data-upload failed on object uuid "
                + self.sumo_object_id
            )
            result["status"] = "failed"
            await self._delete_metadata(sumoclient, self.sumo_object_id)
        else:
            result["status"] = "ok"
            if sumo_mode.lower() == "move":
                file_path = self.path
                metadatafile_path = _path_to_yaml_path(file_path)
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                        logger.debug(
                            "Deleted file after successful upload: %s",
                            file_path,
                        )
                    if os.path.exists(metadatafile_path):
                        os.remove(metadatafile_path)
                        logger.debug(
                            "Deleted metadatafile after successful upload: %s",
                            metadatafile_path,
                        )
                except Exception as err:
                    err = err.with_traceback(None)
                    err_msg = (
                        f"Error deleting file after upload: {err} {type(err)}"
                    )
                    warnings.warn(err_msg)

        return result


def _path_to_yaml_path(path):
    """
    Given a path, return the corresponding yaml file path
    according to FMU standards.
    /my/path/file.txt --> /my/path/.file.txt.yaml
    """

    dir_name = os.path.dirname(path)
    basename = os.path.basename(path)

    return os.path.join(dir_name, f".{basename}.yml")
