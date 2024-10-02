"""

Base class for FileOnJob and FileOnDisk classes.

"""

import os
import sys
import time
import subprocess
import warnings
import httpx
from azure.storage.blob import BlobClient, ContentSettings
from fmu.sumo.uploader._logger import get_uploader_logger


# pylint: disable=C0103 # allow non-snake case variable names

logger = get_uploader_logger()


def _get_segyimport_cmdstr(blob_url, object_id, file_path, sample_unit):
    """Return the command string for running OpenVDS SEGYImport"""
    try:
        url = "azureSAS:" + blob_url["baseuri"][6:]
        url_conn = "Suffix=?" + blob_url["auth"]
    except KeyError:
        url = (
            "azureSAS" + blob_url.split(object_id)[0][5:]
        )  # SEGYImport expects url to container
        url_conn = "Suffix=?" + blob_url.split("?")[1]

    persistent_id = object_id

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
    path_to_executable = None
    for loc in locations:
        path = os.path.join(loc, segy_command)
        if os.path.isfile(path):
            path_to_executable = path
            break
    if path_to_executable is None:
        logger.error("Could not find OpenVDS executables folder location")
    logger.info("Path to OpenVDS executable: " + path_to_executable)

    cmdstr = [
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

    return cmdstr


class SumoFile:
    def __init__(self):
        return

    def _upload_metadata(self, sumoclient, sumo_parent_id):
        path = f"/objects('{sumo_parent_id}')"
        response = sumoclient.post(path=path, json=self.metadata)
        return response

    def _upload_byte_string(self, sumoclient, object_id, blob_url):
        blobclient = BlobClient.from_blob_url(blob_url)
        content_settings = ContentSettings(
            content_type="application/octet-stream"
        )
        response = blobclient.upload_blob(
            self.byte_string,
            blob_type="BlockBlob",
            length=len(self.byte_string),
            overwrite=True,
            content_settings=content_settings,
        )
        # response has the form {'etag': '"0x8DCDC8EED1510CC"', 'last_modified': datetime.datetime(2024, 9, 24, 11, 49, 20, tzinfo=datetime.timezone.utc), 'content_md5': bytearray(b'\x1bPM3(\xe1o\xdf(\x1d\x1f\xb9Qm\xd9\x0b'), 'client_request_id': '08c962a4-7a6b-11ef-8710-acde48001122', 'request_id': 'f459ad2b-801e-007d-1977-0ef6ee000000', 'version': '2024-11-04', 'version_id': None, 'date': datetime.datetime(2024, 9, 24, 11, 49, 19, tzinfo=datetime.timezone.utc), 'request_server_encrypted': True, 'encryption_key_sha256': None, 'encryption_scope': None}
        # ... which is not what the caller expects, so we return something reasonable.
        return httpx.Response(201)

    def _delete_metadata(self, sumoclient, object_id):
        logger.warning("Deleting metadata object: %s", object_id)
        path = f"/objects('{object_id}')"
        response = sumoclient.delete(path=path)
        return response

    def upload_to_sumo(self, sumo_parent_id, sumoclient, sumo_mode):
        """Upload this file to Sumo"""

        logger.debug("Starting upload_to_sumo()")

        # We need these included even if returning before blob upload
        result = {"blob_file_path": self.path, "blob_file_size": self._size}

        if not sumo_parent_id:
            err_msg = f"File upload cannot be attempted, missing case/sumo_parent_id. Got: {sumo_parent_id}"
            result.update(
                {
                    "status": "rejected",
                    "metadata_upload_response_status_code": 500,
                    "metadata_upload_response_text": err_msg,
                }
            )
            return result

        _t0_metadata = time.perf_counter()

        # Uploader converts segy-files to OpenVDS:
        if (
            self.metadata.get("data")
            and self.metadata.get("data").get("format")
            and self.metadata.get("data").get("format") in ["openvds", "segy"]
        ):
            self.metadata["data"]["format"] = "openvds"

        try:
            response = self._upload_metadata(
                sumoclient=sumoclient, sumo_parent_id=sumo_parent_id
            )

            _t1_metadata = time.perf_counter()

            result.update(
                {
                    "metadata_upload_response_status_code": response.status_code,
                    "metadata_upload_response_text": response.text,
                    "metadata_upload_time_start": _t0_metadata,
                    "metadata_upload_time_end": _t1_metadata,
                    "metadata_upload_time_elapsed": _t1_metadata
                    - _t0_metadata,
                    "metadata_file_path": self.metadata_path,
                    "metadata_file_size": self._size,
                }
            )
            pass
        except (httpx.TimeoutException, httpx.ConnectError) as err:
            err = err.with_traceback(None)
            logger.warning(
                f"Metadata upload timeout/connection exception {err} {type(err)}"
            )
            result.update(
                {
                    "status": "failed",
                    "metadata_upload_response_status_code": 500,
                    "metadata_upload_response_text": str(err),
                }
            )
            pass
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
            result.update(
                {
                    "status": "rejected",
                    "metadata_upload_response_status_code": err.response.status_code,
                    "metadata_upload_response_text": error_string[
                        : min(250, len(error_string))
                    ],
                }
            )
            pass
        except Exception as err:
            err = err.with_traceback(None)
            logger.warning(f"Metadata upload exception {err} {type(err)}")
            result.update(
                {
                    "status": "failed",
                    "metadata_upload_response_status_code": 500,
                    "metadata_upload_response_text": str(err),
                }
            )
            pass

        if result["metadata_upload_response_status_code"] not in [200, 201]:
            logger.warning(
                "Metadata upload unsuccessful, returning "
                + str(result["metadata_upload_response_status_code"])
            )
            return result

        self.sumo_parent_id = sumo_parent_id
        self.sumo_object_id = response.json().get("objectid")

        blob_url = response.json().get("blob_url")

        # UPLOAD BLOB

        _t0_blob = time.perf_counter()
        upload_response = {}

        if (
            self.metadata.get("data")
            and self.metadata.get("data").get("format")
            and self.metadata.get("data").get("format") in ["openvds", "segy"]
        ):
            if sys.platform.startswith("darwin"):
                # OpenVDS does not support Mac/darwin directly
                # Outer code expects and interprets http error codes
                upload_response.update(
                    {
                        "status_code": 418,
                        "text": "Can not perform SEGY upload since OpenVDS does not support Mac",
                    }
                )
            else:
                if self.metadata["data"].get("vertical_domain") == "depth":
                    sample_unit = "m"
                else:
                    sample_unit = "ms"  # aka time domain

                cmd_str = _get_segyimport_cmdstr(
                    blob_url, self.sumo_object_id, self.path, sample_unit
                )
                try:
                    cmd_result = subprocess.run(
                        cmd_str, capture_output=True, text=True, shell=False
                    )
                    if cmd_result.returncode == 0:
                        upload_response.update(
                            {
                                "status_code": 200,
                                "text": "SEGY uploaded as OpenVDS.",
                            }
                        )
                    else:
                        # Outer code expects and interprets http error codes
                        logger.warning(
                            "Seismic upload failed with returncode",
                            cmd_result.returncode,
                        )
                        upload_response.update(
                            {
                                "status_code": 418,
                                "text": "FAILED SEGY upload as OpenVDS command "
                                + cmd_result.stderr,
                            }
                        )
                        pass
                    pass
                except Exception as err:
                    err = err.with_traceback(None)
                    logger.warning(
                        f"Seismic upload exception {err} {type(err)}"
                    )
                    upload_response.update(
                        {
                            "status_code": 418,
                            "text": "FAILED SEGY upload as OpenVDS "
                            + str(err)
                            + " "
                            + str(type(err)),
                        }
                    )
        else:  # non-seismic blob
            try:
                response = self._upload_byte_string(
                    sumoclient=sumoclient,
                    object_id=self.sumo_object_id,
                    blob_url=blob_url,
                )
                upload_response.update(
                    {
                        "status_code": response.status_code,
                        "text": response.text,
                    }
                )
                pass
            except (httpx.TimeoutException, httpx.ConnectError) as err:
                err = err.with_traceback(None)
                logger.warning(
                    f"Blob upload failed on timeout/connect {err} {type(err)}"
                )
                upload_response.update(
                    {
                        "status": "failed",
                        "status_code": 500,
                        "text": str(err),
                        "blob_upload_response_status_code": 500,
                        "blob_upload_response_text": str(err),
                    }
                )
                pass
            except httpx.HTTPStatusError as err:
                err = err.with_traceback(None)
                logger.warning(
                    f"Blob upload failed on status {err} {type(err)} {err.response.text}"
                )
                upload_response.update(
                    {
                        "status": "failed",
                        "status_code": err.response.status_code,
                        "text": str(err),
                        "blob_upload_response_status_code": err.response.status_code,
                        "blob_upload_response_text": err.response.reason_phrase,
                    }
                )
                pass
            except Exception as err:
                err = err.with_traceback(None)
                logger.warning(
                    f"Blob upload failed on exception {err} {type(err)}"
                )
                upload_response.update(
                    {
                        "status": "failed",
                        "status_code": 500,
                        "text": str(err),
                        "blob_upload_response_status_code": 500,
                        "blob_upload_response_text": str(err),
                    }
                )

        _t1_blob = time.perf_counter()

        result.update(
            {
                "blob_upload_response_status_code": upload_response[
                    "status_code"
                ],
                "blob_upload_response_status_text": upload_response["text"],
                "blob_upload_time_start": _t0_blob,
                "blob_upload_time_end": _t1_blob,
                "blob_upload_time_elapsed": _t1_blob - _t0_blob,
            }
        )

        if "status_code" not in upload_response or upload_response[
            "status_code"
        ] not in [200, 201]:
            logger.warning(
                "Deleting metadata since data-upload failed on object uuid "
                + self.sumo_object_id
            )
            result["status"] = "failed"
            self._delete_metadata(sumoclient, self.sumo_object_id)
        else:
            result["status"] = "ok"
            file_path = self.path
            metadatafile_path = _path_to_yaml_path(file_path)
            if sumo_mode.lower() == "move":
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
