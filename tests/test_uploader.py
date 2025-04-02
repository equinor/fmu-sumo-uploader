import contextlib
import json
import logging
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest
import xtgeo
import yaml
from sumo.wrapper import SumoClient

from fmu.dataio import CreateCaseMetadata, ExportData
from fmu.sumo import uploader

if not sys.platform.startswith("darwin") and sys.version_info < (3, 12):
    import openvds

# Run the tests from the root dir
TEST_DIR = Path(__file__).parent / "../"
os.chdir(TEST_DIR)

ENV = "dev"

logger = logging.getLogger(__name__)
logger.setLevel(level="DEBUG")


@pytest.fixture(name="case_metadata")
def fixture_case_metadata():
    """Read global variables and create case metadata"""

    global_variables_file = "tests/data/test_case_080/global_variables.yml"
    with open(global_variables_file) as f:
        global_vars = yaml.safe_load(f)
    case_metadata_file = CreateCaseMetadata(
        config=global_vars,
        rootfolder="tests/data/test_case_080/",
        casename="TestCase from fmu.sumo",
        caseuser="testuser",
    ).export()

    yield case_metadata_file

    with contextlib.suppress(FileNotFoundError):
        os.remove(case_metadata_file)


@pytest.fixture(name="surface_file")
def fixture_surface_file(monkeypatch):
    """Read global variables and create surface metadata"""

    monkeypatch.setenv("_ERT_REALIZATION_NUMBER", "0")
    monkeypatch.setenv("_ERT_ITERATION_NUMBER", "0")
    monkeypatch.setenv("_ERT_RUNPATH", "./")

    global_variables_file = "tests/data/test_case_080/global_variables.yml"
    with open(global_variables_file) as f:
        global_vars = yaml.safe_load(f)
    ed = ExportData(
        config=global_vars,
        name="VOLANTIS GP. Top",
        unit="m",
        content="depth",
        vertical_domain="depth",
        timedata=None,
        fmu_context="case",
        casepath="tests/data/test_case_080/",
    )

    surf = xtgeo.surface_from_file(
        "tests/data/test_case_080/topvolantis--ds_extract_geogrid.gri",
        fformat="irap_binary",
    )

    # Generate metadata for the surface
    file = ed.export(surf)

    # file = ed.generate_metadata(surf)
    # Store the metadata for uploader to find
    # with open("tests/data/test_case_080/.surface.bin.yml", "w") as f:
    # yaml.dump(file, f)

    yield file

    # with contextlib.suppress(FileNotFoundError):
    # os.remove("tests/data/test_case_080/.surface.bin.yml")
    # Delete the metadata when test is done
    with contextlib.suppress(FileNotFoundError):
        os.remove(file)


def _update_metadata_file_with_unique_uuid(metadata_file, unique_case_uuid):
    """Updates an existing sumo metadata file with unique case uuid.
    (To be able to run tests in parallell towards Sumo server,
    unique case uuids must be used.)
    """

    # Read the sumo metadata file given as input
    with open(metadata_file) as f:
        parsed_yaml = yaml.safe_load(f)

    # Update case uuid with the given unique uuid
    parsed_yaml["fmu"]["case"]["uuid"] = str(unique_case_uuid)

    # Update the metadata file using the unique uuid
    with open(metadata_file, "w") as f:
        yaml.dump(parsed_yaml, f)


def _update_metadata_file_absolute_path(metadata_file):
    """Updates an existing sumo metadata file with correct
    absolute_path.
    (SUMO_MODE=move depends on absolute_path for deleting files.)
    """

    # Read the sumo metadata file given as input
    with open(metadata_file) as f:
        parsed_yaml = yaml.safe_load(f)

    # Update absolute_path
    parsed_yaml["file"]["absolute_path"] = os.path.join(
        os.getcwd(), metadata_file
    )
    print(os.path.join(os.getcwd(), metadata_file))

    # Update the metadata file
    with open(metadata_file, "w") as f:
        yaml.dump(parsed_yaml, f)


def _hits_for_case(sumoclient, case_uuid):
    query = f"fmu.case.uuid:{case_uuid} AND NOT class:iteration AND NOT class:realization"
    search_results = sumoclient.get(
        "/search", {"$query": query, "$size": 0}
    ).json()
    return search_results.get("hits").get("total").get("value")


### TESTS ###


def test_initialization(token, case_metadata):
    """Assert that the CaseOnDisk object can be initialized"""
    sumoclient = SumoClient(env=ENV, token=token)

    uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        sumoclient=sumoclient,
    )


def test_pre_teardown():
    """Run teardown first to remove remnants from other test runs
    and prepare for running test suite again."""


def test_upload_without_registration(token, case_metadata):
    """Assert that attempting to upload to a non-existing/un-registered case gives warning."""
    sumoclient = SumoClient(env=ENV, token=token)

    case = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        sumoclient=sumoclient,
        verbosity="DEBUG",
    )

    # On purpose NOT calling case.register before adding file here
    child_binary_file = "tests/data/test_case_080/surface.bin"
    # child_metadata_file = "tests/data/test_case_080/.surface.bin.yml"
    # _update_metadata_file_with_unique_uuid(
    #     child_metadata_file, case.fmu_case_uuid
    # )
    case.add_files(child_binary_file)
    with pytest.warns(UserWarning, match="Case is not registered"):
        case.upload(threads=1)


def test_case(token, case_metadata):
    """Assert that after uploading case to Sumo, the case is there and is the only one."""
    sumoclient = SumoClient(env=ENV, token=token)

    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        sumoclient=sumoclient,
    )

    # Assert that this case is not there in the first place
    logger.debug("Asserting that the test case is not already there")
    query = f"class:case AND fmu.case.uuid:{e.fmu_case_uuid}"
    search_results = sumoclient.get(
        "/search", {"$query": query, "$size": 100}
    ).json()
    logger.debug("search results: %s", str(search_results))
    if not search_results:
        raise ValueError("No search results returned")
    hits = search_results.get("hits").get("hits")
    assert len(hits) == 0

    # Register the case
    e.register()
    time.sleep(1)

    # assert that the case is there now
    search_results = sumoclient.get(
        "/search", {"$query": query, "$size": 100}
    ).json()
    hits = search_results.get("hits").get("hits")
    logger.debug(search_results.get("hits"))
    assert len(hits) == 1

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumoclient.delete(path=path)


def test_case_with_restricted_child(token, case_metadata):
    """Assert that uploading a child with 'classification: restricted' works.
    Assumes that the identity running this test have enough rights for that."""
    sumoclient = SumoClient(env=ENV, token=token)

    logger.debug("initialize CaseOnDisk")

    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        sumoclient=sumoclient,
    )

    # Register the case
    e.register()
    time.sleep(1)

    child_binary_file = "tests/data/test_case_080/surface_restricted.bin"
    child_metadata_file = (
        "tests/data/test_case_080/.surface_restricted.bin.yml"
    )
    _update_metadata_file_with_unique_uuid(
        child_metadata_file, e.fmu_case_uuid
    )
    e.add_files(child_binary_file)
    e.upload()
    time.sleep(1)

    total = _hits_for_case(sumoclient, e.fmu_case_uuid)
    assert total == 2

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumoclient.delete(path=path)


def test_case_with_one_child(token, case_metadata, surface_file):
    """Upload one file to Sumo. Assert that it is there."""

    sumoclient = SumoClient(env=ENV, token=token)

    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        sumoclient=sumoclient,
        config_path="tests/data/test_case_080/global_variables.yml",
        parameters_path="tests/data/test_case_080/parameters.txt",
    )
    e.register()
    time.sleep(1)

    # child_binary_file = "tests/data/test_case_080/surface.bin"
    # child_metadata_file = "tests/data/test_case_080/.surface.bin.yml"
    # _update_metadata_file_with_unique_uuid(
    #     child_metadata_file, e.fmu_case_uuid
    # )
    e.add_files(surface_file)
    e.upload()
    time.sleep(1)

    total = _hits_for_case(sumoclient, e.fmu_case_uuid)
    assert total == 2

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumoclient.delete(path=path)


def test_case_with_one_child_and_parameters_txt(
    token, tmp_path, case_metadata, monkeypatch
):
    """Upload one file to Sumo. Assert that it is there."""

    sumoclient = SumoClient(env=ENV, token=token)

    # Create fmu like structure
    case_path = tmp_path / "gorgon"
    case_meta_folder = case_path / "share/metadata"
    case_meta_folder.mkdir(parents=True)
    case_meta_path = case_meta_folder / "fmu_case.yml"
    case_meta_path.write_text(Path(case_metadata).read_text(encoding="utf-8"))

    real_path = case_path / "realization-0/iter-0"
    share_path = real_path / "share/results/surface/"
    fmu_config_folder = real_path / "fmuconfig/output/"
    config_tmp_path = fmu_config_folder / "global_variables.yml"

    share_path.mkdir(parents=True)
    fmu_config_folder.mkdir(parents=True)
    child_binary_file = "tests/data/test_case_080/surface.bin"
    child_metadata_file = "tests/data/test_case_080/.surface.bin.yml"
    fmu_globals_config = "tests/data/test_case_080/global_variables.yml"
    tmp_binary_file_location = str(share_path / "surface.bin")
    shutil.copy(child_binary_file, tmp_binary_file_location)
    shutil.copy(fmu_globals_config, config_tmp_path)
    print(
        "Fmu config path: ",
        config_tmp_path,
        "and exists: ",
        config_tmp_path.exists(),
    )

    e = uploader.CaseOnDisk(
        case_metadata_path=case_meta_path,
        sumoclient=sumoclient,
    )

    _update_metadata_file_with_unique_uuid(
        child_metadata_file, e.fmu_case_uuid
    )
    shutil.copy(child_metadata_file, share_path / ".surface.bin.yml")

    param_file = real_path / "parameters.txt"
    param_file.write_text("TESTINGTESTING 1")

    monkeypatch.chdir(real_path)
    monkeypatch.setenv("_ERT_REALIZATION_NUMBER", "0")
    monkeypatch.setenv("_ERT_ITERATION_NUMBER", "0")
    monkeypatch.setenv("_ERT_RUNPATH", "./")

    e.register()
    time.sleep(1)

    e.add_files(tmp_binary_file_location)
    e.upload()
    # search_string = f"{str(share_path)}/*"
    # sumo_upload_main(case_path, search_string, ENV, search_string, 1)
    time.sleep(1)

    query = (
        f"{e.fmu_case_uuid} AND NOT class:iteration AND NOT class:realization"
    )
    search_results = sumoclient.get(
        "/search", {"$query": query, "$size": 100}
    ).json()
    hits = search_results["hits"]
    results = hits["hits"]
    expected_res = [
        "case",
        "dictionary",
        "surface",
    ]
    found_res = []
    for result in results:
        class_type = result["_source"]["class"]
        found_res.append(class_type)
        assert class_type in expected_res

    total = hits["total"]["value"]
    assert total == len(expected_res)

    # # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumoclient.delete(path=path)


def test_case_with_one_child_with_affiliate_access(token, case_metadata):
    """Upload one file to Sumo with affiliate access.
    Assert that it is there."""

    sumoclient = SumoClient(env=ENV, token=token)

    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        sumoclient=sumoclient,
    )
    e.register()
    time.sleep(1)

    child_binary_file = "tests/data/test_case_080/surface_affiliate.bin"
    child_metadata_file = "tests/data/test_case_080/.surface_affiliate.bin.yml"
    _update_metadata_file_with_unique_uuid(
        child_metadata_file, e.fmu_case_uuid
    )
    e.add_files(child_binary_file)
    e.upload()
    time.sleep(1)

    total = _hits_for_case(sumoclient, e.fmu_case_uuid)
    assert total == 2

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumoclient.delete(path=path)


def test_case_with_no_children(token, case_metadata):
    """Test failure handling when no files are found"""

    sumoclient = SumoClient(env=ENV, token=token)

    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        sumoclient=sumoclient,
    )
    e.register()
    time.sleep(1)

    with pytest.warns(UserWarning) as warnings_record:
        e.add_files("tests/data/test_case_080/NO_SUCH_FILES_EXIST.*")
        e.upload()
        time.sleep(1)
        for _ in warnings_record:
            assert len(warnings_record) == 2, warnings_record
            assert (
                warnings_record[0].message.args[0].startswith("No files found")
            )

    total = _hits_for_case(sumoclient, e.fmu_case_uuid)
    assert total == 1

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumoclient.delete(path=path)


def test_missing_child_metadata(token, case_metadata):
    """
    Try to upload files where one does not have metadata. Assert that warning is given
    and that upload commences with the other files. Check that the children are present.
    """
    sumoclient = SumoClient(env=ENV, token=token)

    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        sumoclient=sumoclient,
    )
    e.register()

    # Add a valid child
    child_binary_file = "tests/data/test_case_080/surface.bin"
    child_metadata_file = "tests/data/test_case_080/.surface.bin.yml"
    _update_metadata_file_with_unique_uuid(
        child_metadata_file, e.fmu_case_uuid
    )
    e.add_files(child_binary_file)

    # Assert that expected warning is given when the binary file
    # do not have a companion metadata file
    with pytest.warns(UserWarning) as warnings_record:
        e.add_files("tests/data/test_case_080/surface_no_metadata.bin")
        for _ in warnings_record:
            assert len(warnings_record) == 1, warnings_record
            assert warnings_record[0].message.args[0].startswith(
                "No metadata, skipping file"
            ) or warnings_record[0].message.args[0].startswith(
                "Invalid metadata"
            )

    e.upload()
    time.sleep(1)

    # Assert parent and valid child is on Sumo
    total = _hits_for_case(sumoclient, e.fmu_case_uuid)
    assert total == 2

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumoclient.delete(path=path)


def test_invalid_yml_in_case_metadata(token):
    """
    Try to upload case file where the metadata file is not valid yml.
    """
    sumoclient = SumoClient(env=ENV, token=token)

    case_file = "tests/data/test_case_080/case_invalid.yml"
    with pytest.warns(UserWarning) as warnings_record:
        uploader.CaseOnDisk(
            case_metadata_path=case_file,
            sumoclient=sumoclient,
        )
        for _ in warnings_record:
            assert len(warnings_record) >= 1, warnings_record
            assert warnings_record[0].message.args[0].startswith(
                "No metadata, skipping file"
            ) or warnings_record[0].message.args[0].startswith(
                "Invalid metadata"
            )


def test_invalid_yml_in_child_metadata(token, case_metadata, surface_file):
    """
    Try to upload child with invalid yml in its metadata file.
    """
    sumoclient = SumoClient(env=ENV, token=token)

    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        sumoclient=sumoclient,
    )
    e.register()

    # Add a valid child
    # child_binary_file = "tests/data/test_case_080/surface.bin"
    # child_metadata_file = "tests/data/test_case_080/.surface.bin.yml"
    # _update_metadata_file_with_unique_uuid(
    #     child_metadata_file, e.fmu_case_uuid
    # )
    e.add_files(surface_file)

    # Add a child with invalid yml in its metadata file
    problem_binary_file = "tests/data/test_case_080/surface_invalid.bin"
    # problem_metadata_file = "tests/data/test_case_080/.surface_invalid.bin.yml"
    with pytest.warns(UserWarning, match="No metadata*"):
        e.add_files(problem_binary_file)

    e.upload()
    time.sleep(1)

    # Assert parent and only 1 valid child are on Sumo
    total = _hits_for_case(sumoclient, e.fmu_case_uuid)
    assert total == 2

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumoclient.delete(path=path)


def test_schema_error_in_case(token, case_metadata):
    """
    Try to upload files where case have metadata with error.
    """
    sumoclient = SumoClient(env=ENV, token=token)

    # replace valid metdata key with an invalid one
    with open(case_metadata) as f:
        parsed_yaml = yaml.safe_load(f)
    parsed_yaml["masterdata_INVALID_SCHEMA"] = parsed_yaml["masterdata"]
    del parsed_yaml["masterdata"]
    with open(case_metadata, "w") as f:
        yaml.dump(parsed_yaml, f)

    with pytest.warns(UserWarning, match="Registering case on Sumo failed*"):
        e = uploader.CaseOnDisk(
            case_metadata_path=case_metadata,
            sumoclient=sumoclient,
        )
        e.register()


def test_schema_error_in_child(token, case_metadata):
    """
    Try to upload files where one does have metadata with error. Assert that warning is given
    and that upload commences with the other files. Check that the children are present.
    """
    sumoclient = SumoClient(env=ENV, token=token)

    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        sumoclient=sumoclient,
    )
    e.register()

    # Add a valid child
    child_binary_file = "tests/data/test_case_080/surface.bin"
    child_metadata_file = "tests/data/test_case_080/.surface.bin.yml"
    _update_metadata_file_with_unique_uuid(
        child_metadata_file, e.fmu_case_uuid
    )
    e.add_files(child_binary_file)

    # Add a child with problem in its metadata file
    problem_binary_file = "tests/data/test_case_080/surface_error.bin"
    problem_metadata_file = "tests/data/test_case_080/.surface_error.bin.yml"
    _update_metadata_file_with_unique_uuid(
        problem_metadata_file, e.fmu_case_uuid
    )
    e.add_files(problem_binary_file)

    e.upload()
    time.sleep(1)

    # Assert parent and valid child are on Sumo
    total = _hits_for_case(sumoclient, e.fmu_case_uuid)
    assert total == 2

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumoclient.delete(path=path)


def _get_segy_path(segy_command):
    """Find the path to the OpenVDS SEGYImport or SEGYExport executables.
    Supply either 'SEGYImport' or 'SEGYExport' as parameter"""
    if sys.platform.startswith("win"):
        segy_command = segy_command + ".exe"
    python_path = os.path.dirname(sys.executable)
    logger.info(python_path)
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
    return path_to_executable


@pytest.mark.skipif(
    sys.platform.startswith("darwin") or sys.version_info >= (3, 12),
    reason="do not run OpenVDS SEGYImport on mac os or python 3.12",
)
def test_openvds_available():
    """Test that OpenVDS is installed and can be successfully called"""
    path_to_segy_import = _get_segy_path("SEGYImport")
    check_segy_import_version = subprocess.run(
        [path_to_segy_import, "--version"], capture_output=True, text=True
    )
    assert check_segy_import_version.returncode == 0
    assert "SEGYImport" in check_segy_import_version.stdout


@pytest.mark.skipif(
    sys.platform.startswith("darwin") or sys.version_info >= (3, 12),
    reason="do not run OpenVDS SEGYImport on mac os or python 3.12",
)
def test_seismic_openvds_file(token, case_metadata):
    """Upload seimic in OpenVDS format to Sumo. Assert that it is there."""
    sumoclient = SumoClient(env=ENV, token=token)

    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        sumoclient=sumoclient,
    )
    e.register()
    time.sleep(1)

    child_binary_file = "tests/data/test_case_080/seismic.segy"
    child_metadata_file = "tests/data/test_case_080/.seismic.segy.yml"
    _update_metadata_file_with_unique_uuid(
        child_metadata_file, e.fmu_case_uuid
    )
    segy_filepath = child_binary_file
    e.add_files(segy_filepath)
    e.upload()
    time.sleep(1)

    # Read the parent object from Sumo
    query = f"_sumo.parent_object:{e.fmu_case_uuid} AND NOT class:iteration AND NOT class:realization"
    search_results = sumoclient.get(
        "/search", {"$query": query, "$size": 100}
    ).json()
    total = search_results.get("hits").get("total").get("value")
    assert total == 1

    # Verify some of the metadata values
    assert (
        search_results.get("hits")
        .get("hits")[0]
        .get("_source")
        .get("data")
        .get("format")
        == "openvds"
    )

    # Get SAS token to read seismic directly from az blob store
    child_id = search_results.get("hits").get("hits")[0].get("_id")
    method = f"/objects('{child_id}')/blob/authuri"
    token_results = sumoclient.get(method).content
    # Sumo server have had 2 different ways of returning the SAS token,
    # and this code should be able to work with both
    try:
        url = (
            "azureSAS:"
            + json.loads(token_results.decode("utf-8")).get("baseuri")[6:]
            + child_id
        )
        url_conn = "Suffix=?" + json.loads(token_results.decode("utf-8")).get(
            "auth"
        )
    except:  # noqa: E722
        token_results = token_results.decode("utf-8")
        url = "azureSAS" + token_results.split("?")[0][5:] + "/"
        url_conn = "Suffix=?" + token_results.split("?")[1]

    # Export from az blob store to a segy file on local disk
    # Openvds 3.4.0 workarounds:
    #     SEGYExport fails on 3 out of 4 attempts, hence retry loop
    #     SEGYExport does not work on ubuntu, hence the platform check
    export_succeeded = False
    export_retries = 0
    if not sys.platform.startswith("linux"):
        while not export_succeeded and export_retries < 40:
            print("SEGYExport retry", export_retries)
            exported_filepath = "exported.segy"
            if os.path.exists(exported_filepath):
                os.remove(exported_filepath)
            path_to_segy_export = _get_segy_path("SEGYExport")
            cmdstr = [
                path_to_segy_export,
                "--url",
                url,
                "--connection",
                url_conn,
                "exported.segy",
            ]
            cmd_result = subprocess.run(
                cmdstr, capture_output=True, text=True, shell=False
            )

            if cmd_result.returncode == 0:
                assert os.path.isfile(exported_filepath)
                assert (
                    os.stat(exported_filepath).st_size
                    == os.stat(segy_filepath).st_size
                )
                if os.path.exists(exported_filepath):
                    os.remove(exported_filepath)
                print("SEGYExport succeeded on retry", export_retries)
                export_succeeded = True
            else:
                time.sleep(16)

            export_retries += 1

        assert export_succeeded

    # Use OpenVDS Python API to read directly from az cloud storage
    handle = openvds.open(url, url_conn)
    layout = openvds.getLayout(handle)
    channel_count = layout.getChannelCount()
    assert channel_count == 3
    assert layout.getChannelName(0) == "Amplitude"

    # Delete this case
    path = f"/objects('{e.fmu_case_uuid}')"
    sumoclient.delete(path=path)
    # Sumo/Azure removes the container which takes some time
    time.sleep(30)

    # OpenVDS reads should fail after deletion
    with pytest.raises(RuntimeError, match="Error on downloading*"):
        handle = openvds.open(url, url_conn)


@pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="do not run on windows due to file-path differences",
)
def test_sumo_mode_default(token, case_metadata):
    """
    Test that SUMO_MODE defaults to copy, i.e. not deleting file after upload.
    """
    sumoclient = SumoClient(env=ENV, token=token)

    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        sumoclient=sumoclient,
    )
    e.register()

    # Add a valid child
    child_binary_file = "tests/data/test_case_080/surface.bin"
    child_metadata_file = "tests/data/test_case_080/.surface.bin.yml"
    _update_metadata_file_with_unique_uuid(
        child_metadata_file, e.fmu_case_uuid
    )
    e.add_files(child_binary_file)

    # Ensure that the absolute_path is correctly set in metadatafile
    # (The test files have dummy value for absolute_path)
    _update_metadata_file_absolute_path(child_metadata_file)

    e.upload()
    time.sleep(1)

    # Assert parent and valid child are on Sumo
    total = _hits_for_case(sumoclient, e.fmu_case_uuid)
    assert total == 2

    # Assert that child file and metadatafile are not deleted
    assert os.path.exists(child_binary_file)
    assert os.path.exists(child_metadata_file)

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumoclient.delete(path=path)


@pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="do not run on windows due to file-path differences",
)
def test_sumo_mode_copy(token, case_metadata):
    """
    Test SUMO_MODE=copy, i.e. not deleting file after upload.
    """
    sumoclient = SumoClient(env=ENV, token=token)

    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        sumoclient=sumoclient,
        sumo_mode="copy",
    )
    e.register()

    # Add a valid child
    child_binary_file = "tests/data/test_case_080/surface.bin"
    child_metadata_file = "tests/data/test_case_080/.surface.bin.yml"
    _update_metadata_file_with_unique_uuid(
        child_metadata_file, e.fmu_case_uuid
    )
    e.add_files(child_binary_file)

    # Ensure that the absolute_path is correctly set in metadatafile
    # (The test files have dummy value for absolute_path)
    _update_metadata_file_absolute_path(child_metadata_file)

    e.upload()
    time.sleep(1)

    # Assert parent and valid child are on Sumo
    total = _hits_for_case(sumoclient, e.fmu_case_uuid)
    assert total == 2

    # Assert that child file and metadatafile are not deleted
    assert os.path.exists(child_binary_file)
    assert os.path.exists(child_metadata_file)

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumoclient.delete(path=path)


@pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="do not run on windows due to file-path differences",
)
def test_sumo_mode_move(token, case_metadata):
    """
    Test SUMO_MODE=move, i.e. deleting file after upload.
    """
    sumoclient = SumoClient(env=ENV, token=token)

    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        sumoclient=sumoclient,
        sumo_mode="moVE",  # test case-insensitive
    )
    e.register()

    # Make copy of binary and metadatafile, so the delete
    # is not messing with git status
    shutil.copy2(
        "tests/data/test_case_080/surface.bin",
        "tests/data/test_case_080/surface.bin.copy",
    )
    shutil.copy2(
        "tests/data/test_case_080/.surface.bin.yml",
        "tests/data/test_case_080/.surface.bin.copy.yml",
    )

    # Add a valid child
    child_binary_file = "tests/data/test_case_080/surface.bin.copy"
    child_metadata_file = "tests/data/test_case_080/.surface.bin.copy.yml"
    _update_metadata_file_with_unique_uuid(
        child_metadata_file, e.fmu_case_uuid
    )
    e.add_files(child_binary_file)

    # Ensure that the absolute_path is correctly set in metadatafile
    # (The test files have dummy value for absolute_path)
    _update_metadata_file_absolute_path(child_metadata_file)

    e.upload()
    time.sleep(1)

    # Assert parent and valid child are on Sumo
    total = _hits_for_case(sumoclient, e.fmu_case_uuid)
    assert total == 2

    # Assert that child file and metadatafile are deleted
    assert not os.path.exists(child_metadata_file)
    assert not os.path.exists(child_binary_file)

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumoclient.delete(path=path)


def test_teardown():
    """Teardown all testdata between every test"""

    # Set all the metadata files back to same case uuid as before, to avoid
    # git reporting changes.
    test_dir = "tests/data/test_case_080/"
    files = os.listdir(test_dir)
    for f in files:
        if (
            f.endswith(".yml")
            and f.startswith(".")
            and not f.__contains__("invalid")
        ):
            dest_file = test_dir + os.path.sep + f
            _update_metadata_file_with_unique_uuid(
                dest_file, "11111111-1111-1111-1111-111111111111"
            )
