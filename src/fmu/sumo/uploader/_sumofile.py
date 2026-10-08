"""

Base class for FileOnJob and FileOnDisk classes.

"""

from __future__ import annotations

import base64
import functools
import hashlib
import logging
import math
import os
import re
import subprocess
import sys
import time
import warnings
from typing import TYPE_CHECKING, Any, Literal, ParamSpec, TypedDict, cast

import httpx
import tenacity
from azure.storage.blob import BlobClient, ContentSettings
from sumo.wrapper import RetryStrategy

from fmu.sumo.uploader._logger import get_uploader_logger
from fmu.sumo.uploader._utils import get_element

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Coroutine
    from pathlib import Path

    from sumo.wrapper import SumoClient

try:
    from ._version import version
except (ImportError, AttributeError):
    version = "0.0.0"
_max_single_put_size = 4 * 1024 * 1024

P = ParamSpec("P")

UploadStatus = Literal["ok", "rejected", "failed"]
"""The outcome of a single file upload.

"rejected" means Sumo refused the file, "failed" means the upload itself did
not complete."""

# pylint: disable=C0103 # allow non-snake case variable names

logger = get_uploader_logger()


def _get_sumo_logger(sumoclient: SumoClient) -> logging.Logger:
    sumo_logger = sumoclient.getLogger("fmu-sumo-uploader")
    sumo_logger.setLevel(logging.INFO)
    sumo_logger.propagate = False
    return sumo_logger


def is_seismic(metadata: dict[str, Any]) -> bool:
    return get_element(metadata, "data.format") in [
        "openvds",
        "segy",
    ]


class ResponseInfo:
    """The outcome and timing of a single request to Sumo or Azure."""

    def __init__(
        self,
        result: Any,
        err: str | None,
        statuscode: int,
        t0: float,
        t1: float,
    ) -> None:
        self.result = result
        self.err = err
        self.statuscode = statuscode
        self.t0 = t0
        self.elapsed = t1 - t0
        self.retries = 0

    @classmethod
    def success(cls, result: Any, t0: float) -> ResponseInfo:
        """Return the outcome of a request that completed."""
        return cls(result, None, 0, t0, time.perf_counter())

    @classmethod
    def failure(
        cls, err: Exception, statuscode: int, t0: float
    ) -> ResponseInfo:
        """Return the outcome of a request that raised."""
        return cls(None, str(err), statuscode, t0, time.perf_counter())

    def ok(self) -> bool:
        return self.result is not None and self.err is None

    def errinfo(self) -> dict[str, Any]:
        return {"err": self.err, "statuscode": self.statuscode}

    def json(self) -> dict[str, Any]:
        return {
            "result": self.result,
            "err": self.err,
            "statuscode": self.statuscode,
            "elapsed": self.elapsed,
            "retries": self.retries,
        }


class _RetryCounter:
    """Counts the attempts made by a retrying uploader.

    An instance is passed as the "before_sleep" callback of a retryer, which
    invokes it before each retry."""

    def __init__(self) -> None:
        self.count = 0

    def __call__(self, retry_state: tenacity.RetryCallState) -> None:
        self.count = retry_state.attempt_number


class UploadResult(TypedDict, total=False):
    """The outcome of uploading a single file.

    Every key is optional: an upload can stop at any stage, and only the
    stages that were reached are present. "blob_file_path" and
    "file_size_bytes" are always set, and "status" is set before the result
    is handed back to the caller.
    """

    blob_file_path: str | Path
    file_size_bytes: int | None
    validation: ResponseInfo
    metadata_upload: ResponseInfo
    blob_upload: ResponseInfo
    status: UploadStatus
    file: SumoFile


def upload_response(
    func: Callable[P, Awaitable[Any]],
) -> Callable[P, Coroutine[Any, Any, ResponseInfo]]:
    """Decorator to wrap upload functions and return a consistent response format"""

    @functools.wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> ResponseInfo:
        t0 = time.perf_counter()
        try:
            return ResponseInfo.success(await func(*args, **kwargs), t0)
        except (httpx.TimeoutException, httpx.ConnectError) as err:
            err = err.with_traceback(None)
            logger.error(
                f"HTTP connect/timeout error during upload: {err} {type(err)}"
            )
            return ResponseInfo.failure(err, 500, t0)
        except httpx.HTTPStatusError as err:
            err = err.with_traceback(None)
            logger.error(f"HTTP status error during upload: {err} {type(err)}")
            return ResponseInfo.failure(err, err.response.status_code, t0)
        except Exception as err:
            err = err.with_traceback(None)
            logger.error(f"Error during upload: {err} {type(err)}")
            return ResponseInfo.failure(err, 500, t0)

    return wrapper


@upload_response
async def upload_metadata(
    sumoclient: SumoClient,
    sumo_parent_id: str,
    metadata: dict[str, Any],
    retry_strategy: RetryStrategy,
) -> dict[str, Any]:
    """Upload metadata to Sumo and return a consistent response format"""
    path = f"/objects('{sumo_parent_id}')"
    response = await sumoclient.post_async(
        path=path, json=metadata, retry_strategy=retry_strategy
    )
    response.raise_for_status()
    return response.json()


def get_blob_client(blob_url: str) -> BlobClient:
    blobclient = BlobClient.from_blob_url(
        blob_url,
        connection_timeout=600,
        read_timeout=600,
        max_single_put_size=_max_single_put_size,
    )
    return blobclient


async def _upload_blob(blob_url: str, byte_string: bytes) -> None:
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


@upload_response
async def upload_blob(
    blob_url: str, byte_string: bytes, retryer: RetryStrategy
) -> bool:
    """Upload blob to Azure and return a consistent response format"""

    async def doit() -> None:
        await _upload_blob(blob_url, byte_string)

    await retryer(doit)
    # response has the form {'etag': '"0x8DCDC8EED1510CC"', 'last_modified': datetime.datetime(2024, 9, 24, 11, 49, 20, tzinfo=datetime.UTC), 'content_md5': bytearray(b'\x1bPM3(\xe1o\xdf(\x1d\x1f\xb9Qm\xd9\x0b'), 'client_request_id': '08c962a4-7a6b-11ef-8710-acde48001122', 'request_id': 'f459ad2b-801e-007d-1977-0ef6ee000000', 'version': '2024-11-04', 'version_id': None, 'date': datetime.datetime(2024, 9, 24, 11, 49, 19, tzinfo=datetime.UTC), 'request_server_encrypted': True, 'encryption_key_sha256': None, 'encryption_scope': None}
    # ... which is not what the caller expects, so we return something reasonable.
    return True


@upload_response
async def validate(parent_id: str, metadata: dict[str, Any]) -> bool:
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
def get_path_to_segyimport() -> str:
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
            return path

    raise Exception("Could not find OpenVDS executables folder location")


def get_segyimport_cmd(
    blob_url: str | dict[str, str],
    object_id: str,
    file_path: str | Path,
    sample_unit: str,
) -> list[str]:
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
        str(file_path),
    ]

    return cmd


@upload_response
async def upload_seismic_blob(
    object_id: str,
    path: str | Path,
    metadata: dict[str, Any],
    blob_url: str | dict[str, str],
) -> bool:
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
        cmd_result = subprocess.run(  # noqa: PLW1510 ASYNC221
            cmd_str, capture_output=True, text=True, shell=False
        )
        if cmd_result.returncode == 0:
            return True
        else:
            # Outer code expects and interprets http error codes
            logger.warning(
                "Seismic upload failed with returncode %s",
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
    metadata: dict[str, Any]
    byte_string: bytes
    sumo_object_id: str | None
    blob_md5_hex: str
    # Declared, but deliberately not assigned: this is set by FileOnDisk, and
    # by the caller for FileOnJob. A default here would mask an unset value.
    path: str | Path

    def __init__(
        self,
        metadata: dict[str, Any],
        byte_string: bytes,
    ) -> None:
        self.metadata = metadata
        self.byte_string = byte_string
        self.sumo_object_id = None
        digester = hashlib.md5(self.byte_string)
        self.blob_md5_hex = digester.hexdigest()
        self.metadata["_sumo"] = {}
        self.metadata["_sumo"]["blob_size"] = len(self.byte_string)
        self.metadata["_sumo"]["blob_md5"] = base64.b64encode(
            digester.digest()
        ).decode("utf-8")
        self.metadata["_sumo"]["uploader"] = version

    def _warn_on_blob_size_mismatch(
        self, file_size_bytes: int | None, sumo_logger: logging.Logger
    ) -> None:
        sumo_blob_size = get_element(self.metadata, "_sumo.blob_size")

        if file_size_bytes is not None and file_size_bytes != sumo_blob_size:
            file_path = get_element(self.metadata, "file.absolute_path")
            case_uuid = get_element(self.metadata, "fmu.case.uuid")

            sumo_logger.warning(
                "FileSizeDiscrepancy: file.size_bytes (%s) differs from blob size (%s) for %s",
                file_size_bytes,
                sumo_blob_size,
                file_path,
                extra={"objectUuid": case_uuid},
            )

    async def _delete_metadata(
        self, sumoclient: SumoClient, object_id: str
    ) -> httpx.Response:
        logger.warning("Deleting metadata object: %s", object_id)
        path = f"/objects('{object_id}')"
        response = await sumoclient.delete_async(path=path)
        return response

    async def upload_to_sumo(
        self, sumo_parent_id: str, sumoclient: SumoClient, sumo_mode: str
    ) -> UploadResult:
        """Upload this file to Sumo"""
        file_size_bytes = get_element(self.metadata, "file.size_bytes")

        # We need these included even if returning before blob upload
        result: UploadResult = {
            "blob_file_path": self.path,
            "file_size_bytes": file_size_bytes,
        }

        self._warn_on_blob_size_mismatch(
            file_size_bytes, _get_sumo_logger(sumoclient)
        )

        result["validation"] = await validate(sumo_parent_id, self.metadata)
        if not result["validation"].ok():
            result["status"] = "rejected"
            return result

        if is_seismic(self.metadata):
            self.metadata["data"]["format"] = (
                "openvds"  # we will upload seismic as openvds format, even if originally segy
            )

        metadata_upload = await self._perform_metadata_upload(
            sumoclient, sumo_parent_id
        )
        result["metadata_upload"] = metadata_upload

        if not metadata_upload.ok():
            result["status"] = (
                "rejected"
                if metadata_upload.statuscode in range(400, 500)
                else "failed"
            )
            return result

        object_id: str = metadata_upload.result.get("objectid")
        self.sumo_object_id = object_id
        blob_url = metadata_upload.result.get("blob_url")

        blob_upload = await self._perform_blob_upload(object_id, blob_url)
        result["blob_upload"] = blob_upload

        if not blob_upload.ok():
            logger.warning(
                "Deleting metadata since data-upload failed on object uuid "
                + object_id
            )
            result["status"] = "failed"
            await self._delete_metadata(sumoclient, object_id)
            return result

        result["status"] = "ok"
        if sumo_mode.lower() == "move":
            self._delete_local_files()

        return result

    async def _perform_metadata_upload(
        self, sumoclient: SumoClient, sumo_parent_id: str
    ) -> ResponseInfo:
        """Upload the metadata, recording how many retries it took."""

        retry_counter = _RetryCounter()
        response = await upload_metadata(
            sumoclient,
            sumo_parent_id,
            self.metadata,
            retry_strategy=RetryStrategy(before_sleep=retry_counter),
        )
        response.retries = retry_counter.count
        return response

    async def _perform_blob_upload(
        self, object_id: str, blob_url: str | dict[str, str]
    ) -> ResponseInfo:
        """Upload the blob, as OpenVDS for seismic and as-is for the rest.

        Sumo returns the blob url as a string, but the OpenVDS path also
        accepts it pre-split into "baseuri" and "auth"."""

        if is_seismic(self.metadata):
            logger.info(
                "This is a seismic file, will attempt to upload as OpenVDS"
            )
            return await upload_seismic_blob(
                object_id, self.path, self.metadata, blob_url
            )

        retry_counter = _RetryCounter()

        def return_last_value(retry_state: tenacity.RetryCallState) -> Any:
            return retry_state.outcome.result()  # type: ignore[union-attr]

        retryer = tenacity.AsyncRetrying(
            stop=tenacity.stop_after_attempt(1),
            wait=(
                tenacity.wait_exponential(multiplier=0.5, exp_base=2)
                + tenacity.wait_random_exponential(multiplier=0.5, exp_base=2)
            ),
            retry_error_callback=return_last_value,
            before_sleep=retry_counter,
        )
        response = await upload_blob(
            cast("str", blob_url), self.byte_string, retryer
        )
        response.retries = retry_counter.count
        return response

    def _delete_local_files(self) -> None:
        """Delete the file and its metadata file, after a "move" upload."""

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
            warnings.warn(
                f"Error deleting file after upload: {err} {type(err)}"
            )


def _path_to_yaml_path(path: str | Path) -> str:
    """
    Given a path, return the corresponding yaml file path
    according to FMU standards.
    /my/path/file.txt --> /my/path/.file.txt.yaml
    """

    dir_name = os.path.dirname(path)
    basename = os.path.basename(path)

    return os.path.join(dir_name, f".{basename}.yml")
