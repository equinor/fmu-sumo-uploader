import os
import sys
import pytest
import time
from pathlib import Path
import logging

from fmu.sumo import uploader

# run the tests from the root dir
TEST_DIR = Path(__file__).parent / "../"
os.chdir(TEST_DIR)

ENV = "dev"

logger = logging.getLogger(__name__)
logger.setLevel(level="DEBUG")


class SumoConnection:
    def __init__(self, env, token=None):
        self.env = env
        self._connection = None
        self.token = token

    @property
    def connection(self):
        if self._connection is None:
            self._connection = uploader.SumoConnection(env=self.env, token=self.token)
        return self._connection


def _remove_cached_case_id():
    try:
        os.remove("tests/data/test_case_080/sumo_parent_id.yml")
    except FileNotFoundError:
        pass


### TESTS ###


def test_initialization(token):
    """Assert that the CaseOnDisk object can be initialized"""
    sumo_connection = SumoConnection(env=ENV, token=token).connection

    case = uploader.CaseOnDisk(
        case_metadata_path="tests/data/test_case_080/case.yml",
        sumo_connection=sumo_connection,
    )


def test_pre_teardown(token):
    """Run teardown first to remove remnants from other test runs."""
    test_teardown(token)


def test_upload_without_registration(token):
    """Assert that attempting to upload to a non-existing case gives warning."""
    sumo_connection = uploader.SumoConnection(env=ENV, token=token)

    _remove_cached_case_id()

    case = uploader.CaseOnDisk(
        case_metadata_path="tests/data/test_case_080/case.yml",
        sumo_connection=sumo_connection,
        verbosity="DEBUG",
    )
    case.add_files("tests/data/test_case_080/surface.bin")

    with pytest.warns(UserWarning, match="Case is not registered"):
        case.upload(threads=1)


def test_case(token):
    """Assert that after uploading case to Sumo, the case is there and the only one."""
    sumo_connection = uploader.SumoConnection(env=ENV, token=token)

    _remove_cached_case_id()

    logger.debug("initialize CaseOnDisk")
    e = uploader.CaseOnDisk(
        case_metadata_path="tests/data/test_case_080/case.yml",
        sumo_connection=sumo_connection,
    )

    query = f"fmu.case.uuid:{e.fmu_case_uuid}"

    # assert that it is not there in the first place
    logger.debug("Asserting that the test case is not already there")
    search_results = sumo_connection.api.get(
        "/searchroot", query=query, size=100, **{"from": 0}
    )
    logger.debug("search results: %s", str(search_results))
    if not search_results:
        raise ValueError("No search results returned")
    hits = search_results.get("hits").get("hits")
    assert len(hits) == 0

    # register it
    e.register()

    # assert that it is there now
    time.sleep(3)  # wait 3 seconds
    search_results = sumo_connection.api.get(
        "/searchroot", query=query, size=100, **{"from": 0}
    )
    hits = search_results.get("hits").get("hits")
    logger.debug(search_results.get("hits"))
    assert len(hits) == 1


def test_one_file(token):
    """Upload one file to Sumo. Assert that it is there."""

    sumo_connection = uploader.SumoConnection(env=ENV, token=token)

    _remove_cached_case_id()

    e = uploader.CaseOnDisk(
        case_metadata_path="tests/data/test_case_080/case.yml",
        sumo_connection=sumo_connection,
    )
    e.register()
    e.add_files("tests/data/test_case_080/surface.bin")
    e.upload()

    time.sleep(4)

    query = f"{e.fmu_case_uuid}"
    search_results = sumo_connection.api.get(
        "/search", query=query, size=100, **{"from": 0}
    )
    total = search_results.get("hits").get("total").get("value")
    assert total == 2


def test_missing_metadata(token):
    """
    Try to upload files where one does not have metadata. Assert that warning is given
    and that upload commences with the other files. Check that the children are present.
    """
    sumo_connection = uploader.SumoConnection(env=ENV, token=token)

    _remove_cached_case_id()

    e = uploader.CaseOnDisk(
        case_metadata_path="tests/data/test_case_080/case.yml",
        sumo_connection=sumo_connection,
    )

    # Assert that expected warning was given
    with pytest.warns(
        UserWarning
    ) as warnings_record:  # testdata contains one file with missing metadata
        e.add_files("tests/data/test_case_080/surface_no_metadata.bin")
        for _ in warnings_record:
            assert len(warnings_record) == 1, warnings_record
            assert (
                warnings_record[0]
                .message.args[0]
                .endswith("No metadata, skipping file.")
            )

    # Assert children is on Sumo
    query = f"{e.fmu_case_uuid}"
    search_results = sumo_connection.api.get(
        "/search", query=query, size=100, **{"from": 0}
    )
    total = search_results.get("hits").get("total").get("value")
    assert total == 2


def test_wrong_metadata(token):
    """
    Try to upload files where one does have metadata with error. Assert that warning is given
    and that upload commences with the other files. Check that the children are present.
    """
    sumo_connection = uploader.SumoConnection(env=ENV, token=token)

    _remove_cached_case_id()

    e = uploader.CaseOnDisk(
        case_metadata_path="tests/data/test_case_080/case.yml",
        sumo_connection=sumo_connection,
    )

    # Assert that expected warning was given
    e.add_files("tests/data/test_case_080/surface_error.bin")

    e.upload()
    time.sleep(4)

    # Assert children is on Sumo
    query = f"{e.fmu_case_uuid}"
    search_results = sumo_connection.api.get(
        "/search", query=query, size=100, **{"from": 0}
    )
    total = search_results.get("hits").get("total").get("value")
    assert total == 2


# TEMPORARILY REMOVE SEISMIC TEST PENDING SUMO SUPPORT

# def test_seismic_file(token):
#     """Upload one seimic file to Sumo. Assert that it is there."""
#     sumo_connection = uploader.SumoConnection(env=ENV, token=token)
#     e = uploader.CaseOnDisk(
#         case_metadata_path="tests/data/test_case_080/case.yml",
#         sumo_connection=sumo_connection,
#     )
#     e.register()
#     e.add_files("tests/data/test_case_080/seismic.segy")

#     # Assert children is on Sumo

#     e.upload()
#     time.sleep(4)
#     query = f"{e.fmu_case_uuid}"
#     search_results = sumo_connection.api.get(
#         "/search", query=query, size=100, **{"from": 0}
#     )
#     total = search_results.get("hits").get("total").get("value")
#     assert total == 3


def test_teardown(token):
    """Teardown all testdata"""
    sumo_connection = uploader.SumoConnection(env=ENV, token=token)

    _remove_cached_case_id()

    e = uploader.CaseOnDisk(
        case_metadata_path="tests/data/test_case_080/case.yml",
        sumo_connection=sumo_connection,
    )

    # This uploads case metadata to Sumo
    e.register()

    time.sleep(5)  # Sumo creates the container

    path = f"/objects('{e.sumo_parent_id}')"
    sumo_connection.api.delete(path=path)

    time.sleep(30)  # Sumo removes the container

    # Assert children is not on Sumo
    query = f"{e.fmu_case_uuid}"
    search_results = sumo_connection.api.get(
        "/searchroot", query=query, size=100, **{"from": 0}
    )
    total = search_results["hits"]["total"]["value"]
    assert total == 0

    _remove_cached_case_id()