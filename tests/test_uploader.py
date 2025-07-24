import contextlib
import datetime
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
from fmu.dataio.manifest import get_manifest_path
from fmu.sumo import uploader

if not sys.platform.startswith("darwin") and sys.version_info < (3, 12):
    import openvds

ENV = "dev"
CASEPATH = Path.cwd() / "tests/data/"

logger = logging.getLogger(__name__)
logger.setLevel(level="DEBUG")


@pytest.fixture(name="case_metadata")
def fixture_case_metadata():
    """Read global variables and create case metadata"""

    global_variables_file = "tests/data/global_variables.yml"
    with open(global_variables_file) as f:
        global_vars = yaml.safe_load(f)
    case_metadata_file = CreateCaseMetadata(
        config=global_vars,
        rootfolder="tests/data/",
        casename="TestCase from fmu.sumo",
    ).export()

    yield case_metadata_file

    with contextlib.suppress(FileNotFoundError):
        os.remove(case_metadata_file)


@pytest.fixture(name="surface_file")
def fixture_surface_file(monkeypatch):
    """Read global variables and create surface metadata"""

    monkeypatch.setenv("_ERT_REALIZATION_NUMBER", "0")
    monkeypatch.setenv("_ERT_ITERATION_NUMBER", "0")
    monkeypatch.setenv("_ERT_RUNPATH", "./tests/data/")

    global_variables_file = "tests/data/global_variables.yml"
    with open(global_variables_file) as f:
        global_vars = yaml.safe_load(f)
    ed = ExportData(
        config=global_vars,
        name="VOLANTIS GP. Top",
        unit="m",
        content="depth",
        vertical_domain="depth",
        timedata=None,
        casepath=Path.cwd() / "tests/data/",
    )

    surf = xtgeo.surface_from_file(
        "tests/data/topvolantis--ds_extract_geogrid.gri",
        fformat="irap_binary",
    )

    # Export surface and generate metadata
    file = ed.export(surf)

    yield file

    # Delete grid file when test is done
    with contextlib.suppress(FileNotFoundError):
        os.remove(file)


@pytest.fixture(name="surface_metadata_file")
def fixture_surface_metadata_file(surface_file):
    """Get path to the metadata for surface_file"""

    dir_name = os.path.dirname(surface_file)
    basename = os.path.basename(surface_file)

    file = os.path.join(dir_name, f".{basename}.yml")

    yield file

    # Delete the metadata when test is done
    with contextlib.suppress(FileNotFoundError):
        os.remove(file)


@pytest.fixture(name="manifest_file")
def fixture_manifest_file():
    """Get path to the export manifest file"""

    file = get_manifest_path(CASEPATH)

    yield file

    # Delete the manifest when test is done
    with contextlib.suppress(FileNotFoundError):
        os.remove(file)


@pytest.fixture(name="sumo_uploads_file")
def fixture_sumo_uploads_file(manifest_file):
    """Get path to the sumo uploads file"""

    file = manifest_file.parent / ".sumo_uploads.json"

    yield file

    # Delete the sumo uploads log when test is done
    with contextlib.suppress(FileNotFoundError):
        os.remove(file)


@pytest.fixture(name="segy_file")
def fixture_segy_file(monkeypatch):
    """Create metadata for seismic.segy"""

    monkeypatch.setenv("_ERT_REALIZATION_NUMBER", "0")
    monkeypatch.setenv("_ERT_ITERATION_NUMBER", "0")
    monkeypatch.setenv("_ERT_RUNPATH", "./tests/data/")

    global_variables_file = "tests/data/global_variables.yml"
    with open(global_variables_file) as f:
        global_vars = yaml.safe_load(f)
    ed = ExportData(
        config=global_vars,
        name="seismic",
        content="seismic",
        content_metadata={"attribute": "owc", "is_discrete": False},
        casepath=Path.cwd() / "tests/data/",
    )

    segy_file = xtgeo.cube_from_file("tests/data/seismic.segy", fformat="segy")

    # Export and generate metadata
    file = ed.export(segy_file)

    yield file

    # Delete grid file when test is done
    with contextlib.suppress(FileNotFoundError):
        os.remove(file)


def _hits_for_case(sumoclient, case_uuid):
    query = f"fmu.case.uuid:{case_uuid} AND NOT class:ensemble AND NOT class:realization"
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
        casepath=CASEPATH,
        sumoclient=sumoclient,
    )


def test_manifest(token, case_metadata, surface_file, manifest_file):
    """Assert that manifest exists after exporting data"""
    sumoclient = SumoClient(env=ENV, token=token)

    uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        casepath=CASEPATH,
        sumoclient=sumoclient,
        verbosity="DEBUG",
    )

    # Assert that manifest is there.
    assert os.path.exists(manifest_file)

    with open(manifest_file, "r") as f:
        manifest = json.load(f)
    
    assert len(manifest) == 1


def test_sumo_uploads(token, case_metadata, surface_file, manifest_file, sumo_uploads_file):
    """Assert that sumo uploads log exists after exporting data"""
    sumoclient = SumoClient(env=ENV, token=token)

    case = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        casepath=CASEPATH,
        sumoclient=sumoclient,
        verbosity="DEBUG",
    )

    # Assert that manifest is there, and assert that sumo uploads log is not there. 
    assert os.path.exists(manifest_file)
    assert not os.path.exists(sumo_uploads_file)

    case.register()
    case.add_files()
    assert len(case.files) == 1
    
    case.upload()

    # Assert that sumo uploads log is there.
    assert os.path.exists(sumo_uploads_file)
    
    with open(manifest_file, "r") as f:
        manifest = json.load(f)
    
    with open(sumo_uploads_file, "r") as f:
        sumo_uploads = json.load(f)
    
    assert len(sumo_uploads) == 1
    assert sumo_uploads[-1]["last_index_manifest"] == len(manifest) - 1


def test_upload_without_registration(token, case_metadata, surface_file, manifest_file, sumo_uploads_file):
    """Assert that attempting to upload to a non-existing/un-registered case gives warning."""
    sumoclient = SumoClient(env=ENV, token=token)

    case = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        casepath=CASEPATH,
        sumoclient=sumoclient,
        verbosity="DEBUG",
    )

    assert os.path.exists(surface_file)
    assert os.path.exists(manifest_file)

    case.add_files()
    with pytest.warns(UserWarning, match="Case is not registered"):
        case.upload(threads=1)

    # Assert if sumo uploads log is not there.    
    assert not os.path.exists(sumo_uploads_file)


def test_validate_schema(token, case_metadata):
    """Assert when schema is not valid"""
    sumoclient = SumoClient(env=ENV, token=token)
    with open(case_metadata, "r") as f:
        parsed_yaml = yaml.safe_load(f)
    response = sumoclient.post(path="/json-validate", json=parsed_yaml).json()
    assert response.get("valid") is True


def test_case(token, case_metadata):
    """Assert that after uploading case to Sumo, the case is there and is the only one."""
    sumoclient = SumoClient(env=ENV, token=token)

    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        casepath=CASEPATH,
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


def test_case_with_restricted_child(
    token, case_metadata, surface_file, surface_metadata_file, manifest_file, sumo_uploads_file
):
    """Assert that uploading a child with 'classification: restricted' works.
    Assumes that the identity running this test have enough rights for that."""
    sumoclient = SumoClient(env=ENV, token=token)

    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        casepath=CASEPATH,
        sumoclient=sumoclient,
    )

    # Register the case
    e.register()
    time.sleep(1)

    # Create a metadata file with access.affiliate_roles set
    with open(surface_metadata_file) as f:
        parsed_yaml = yaml.safe_load(f)
    parsed_yaml["access"]["ssdl"]["access_level"] = "restricted"
    parsed_yaml["access"]["classification"] = "restricted"

    basename = os.path.dirname(surface_file)
    restricted_metadata_file = os.path.join(basename, ".surface_restricted.bin.yml")
    with open(restricted_metadata_file, "w") as f:
        yaml.dump(parsed_yaml, f)

    # Make copy of binary to match the modified metadata file
    surface_file_copy = os.path.join(basename, "surface_restricted.bin")
    shutil.copy(
        surface_file,
        surface_file_copy,
    )

    # Make new export manifest with entry for restricted file
    new_entry = {
        "absolute_path": surface_file_copy,
        "exported_at": datetime.datetime.now(datetime.UTC).isoformat().replace('+00:00', 'Z'),
        "exported_by": "TEST"
    }

    with open(manifest_file, "r") as f:
        manifest = json.load(f)
        manifest.append(new_entry)

    os.remove(manifest_file)
    with open(manifest_file, "w") as f:
        json.dump(manifest, f, indent=4)

    e.add_files()
    e.upload()
    time.sleep(1)

    total = _hits_for_case(sumoclient, e.fmu_case_uuid)
    assert total == 2

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumoclient.delete(path=path)

    os.remove(surface_file_copy)
    os.remove(restricted_metadata_file)


def test_case_with_one_child(token, case_metadata, surface_file, manifest_file, sumo_uploads_file):
    """Upload one file to Sumo. Assert that it is there."""

    sumoclient = SumoClient(env=ENV, token=token)

    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        casepath=CASEPATH,
        sumoclient=sumoclient,
        config_path="tests/data/global_variables.yml",
    )
    e.register()
    time.sleep(1)

    assert os.path.exists(surface_file)
    assert os.path.exists(manifest_file)
    e.add_files()
    e.upload()
    time.sleep(1)

    total = _hits_for_case(sumoclient, e.fmu_case_uuid)
    assert total == 2
    assert os.path.exists(sumo_uploads_file)

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumoclient.delete(path=path)


def test_case_with_one_child_and_parameters_txt(
    token,
    tmp_path,
    case_metadata,
    monkeypatch,
    surface_file,
    surface_metadata_file,
    manifest_file,
    sumo_uploads_file
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
    fmu_globals_config = "tests/data/global_variables.yml"
    tmp_binary_file_location = str(share_path / "surface.bin")
    shutil.copy(surface_file, tmp_binary_file_location)
    shutil.copy(fmu_globals_config, config_tmp_path)
    shutil.copy(surface_metadata_file, share_path / ".surface.bin.yml")
    shutil.copy("tests/data/parameters.txt", real_path / "parameters.txt")
    shutil.copy(
        manifest_file,
        real_path / ".dataio_export_manifest.json",
    )

    e = uploader.CaseOnDisk(
        case_metadata_path=case_meta_path,
        casepath=case_path,
        sumoclient=sumoclient,
    )

    monkeypatch.chdir(real_path)
    monkeypatch.setenv("_ERT_REALIZATION_NUMBER", "0")
    monkeypatch.setenv("_ERT_ITERATION_NUMBER", "0")
    monkeypatch.setenv("_ERT_RUNPATH", "./")

    e.register()
    time.sleep(1)

    e.add_files()
    e.upload()
    time.sleep(1)

    ert_run_sumo_uploads_file = real_path / ".sumo_uploads.json"
    assert not os.path.exists(sumo_uploads_file)
    assert os.path.exists(ert_run_sumo_uploads_file)

    query = (
        f"{e.fmu_case_uuid} AND NOT class:ensemble AND NOT class:realization"
    )
    search_results = sumoclient.get(
        "/search", {"$query": query, "$size": 100}
    ).json()
    hits = search_results["hits"]
    total = hits["total"]["value"]
    expected_res = [
        "case",
        "dictionary",
        "surface",
    ]
    assert total == len(expected_res)

    results = hits["hits"]
    for result in results:
        class_type = result["_source"]["class"]
        assert class_type in expected_res

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumoclient.delete(path=path)


def test_case_with_one_child_with_affiliate_access(
    token, case_metadata, surface_file, surface_metadata_file, manifest_file, sumo_uploads_file
):
    """Upload one file to Sumo with affiliate access.
    Assert that it is there."""

    sumoclient = SumoClient(env=ENV, token=token)

    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        casepath=CASEPATH,
        sumoclient=sumoclient,
    )
    e.register()
    time.sleep(1)

    # Create a metadata file with access.affiliate_roles set
    with open(surface_metadata_file) as f:
        parsed_yaml = yaml.safe_load(f)
    parsed_yaml["access"]["affiliate_roles"] = ["DROGON-AFFILIATE"]
    affiliate_access_metadata_file = os.path.join(os.path.dirname(surface_file), ".surface_affiliate.bin.yml")
    with open(affiliate_access_metadata_file, "w") as f:
        yaml.dump(parsed_yaml, f)

    # Make copy of binary to match the modified metadata file
    surface_file_copy = os.path.join(os.path.dirname(surface_file),"surface_affiliate.bin")
    shutil.copy(
        surface_file,
        surface_file_copy,
    )
    
    # Make new export manifest with entry for file with affilate access
    new_entry = {
        "absolute_path": surface_file_copy,
        "exported_at": datetime.datetime.now(datetime.UTC).isoformat().replace('+00:00', 'Z'),
        "exported_by": "TEST"
    }

    with open(manifest_file, "r") as f:
        manifest = json.load(f)
        manifest.append(new_entry)

    os.remove(manifest_file)
    with open(manifest_file, "w") as f:
        json.dump(manifest, f, indent=4)

    e.add_files()
    e.upload()
    time.sleep(1)

    total = _hits_for_case(sumoclient, e.fmu_case_uuid)
    assert total == 2

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumoclient.delete(path=path)

    os.remove(surface_file_copy)
    os.remove(affiliate_access_metadata_file)


def test_case_with_no_children(token, case_metadata):
    """Test failure handling when no files are found"""

    sumoclient = SumoClient(env=ENV, token=token)

    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        casepath=CASEPATH,
        sumoclient=sumoclient,
    )
    e.register()
    time.sleep(1)

    manifest_file = "tests/data/.dataio_export_manifest.json"
    manifest = [
        {
            "absolute_path": "path/to/NO_SUCH_FILES_EXIST.*",
            "exported_at": "2025-07-22T08:07:52.197429Z",
            "exported_by": "TEST",
        }
    ]
    with open(manifest_file, "w") as f:
        json.dump(manifest, f, indent=4)

    with pytest.warns(UserWarning) as warnings_record:
        e.add_files()
        e.upload()
        time.sleep(1)
        for _ in warnings_record:
            assert len(warnings_record) == 1, warnings_record
            assert (
                warnings_record[0]
                .message.args[0]
                .startswith("No files found")
            )

    total = _hits_for_case(sumoclient, e.fmu_case_uuid)
    assert total == 1

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumoclient.delete(path=path)

    # Delete manifest file
    os.remove(manifest_file)


def test_missing_child_metadata(token, case_metadata, surface_file, manifest_file, sumo_uploads_file):
    """
    Try to upload files where one does not have metadata. Assert that warning is given
    and that upload commences with the other files. Check that the children are present.
    """
    sumoclient = SumoClient(env=ENV, token=token)

    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        casepath=CASEPATH,
        sumoclient=sumoclient,
    )
    e.register()

    # Make a copy of the surface without copying companion metadata
    surface_file_copy= os.path.join(os.path.dirname(surface_file), "surface_no_metadata.bin")
    shutil.copy(
        surface_file,
        surface_file_copy
    )

    new_entry =  {
        "absolute_path": surface_file_copy,
        "exported_at": datetime.datetime.now(datetime.UTC).isoformat().replace('+00:00', 'Z'),
        "exported_by": "TEST",
    }
    
    # Append entry for file missing metadata in export manifest
    with open(manifest_file, "r") as f:
        manifest = json.load(f)
        manifest.append(new_entry)

    assert len(manifest) == 2
    os.remove(manifest_file)
    with open(manifest_file, "w") as f:
        json.dump(manifest, f, indent=4)

    # Assert that expected warning is given when the binary file
    # do not have a companion metadata file
    with pytest.warns(UserWarning) as warnings_record:
        e.add_files()
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

    assert os.path.exists(sumo_uploads_file)
    with open(sumo_uploads_file) as f:
        uploads = json.load(f)
    assert uploads[-1]["last_index_manifest"] == 1

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumoclient.delete(path=path)

    os.remove(surface_file_copy)


def test_invalid_yml_in_case_metadata(token):
    """
    Try to upload case file where the metadata file is not valid yml.
    """
    sumoclient = SumoClient(env=ENV, token=token)

    case_file = "tests/data/case_invalid.yml"
    with pytest.warns(UserWarning) as warnings_record:
        uploader.CaseOnDisk(
            case_metadata_path=case_file,
            casepath=CASEPATH,
            sumoclient=sumoclient,
        )
        for _ in warnings_record:
            assert len(warnings_record) >= 1, warnings_record
            assert warnings_record[0].message.args[0].startswith(
                "No metadata, skipping file"
            ) or warnings_record[0].message.args[0].startswith(
                "Invalid metadata"
            )


def test_invalid_yml_in_child_metadata(token, case_metadata, surface_file, manifest_file, sumo_uploads_file):
    """
    Try to upload child with invalid yml in its metadata file.
    """
    sumoclient = SumoClient(env=ENV, token=token)

    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        casepath=CASEPATH,
        sumoclient=sumoclient,
    )
    e.register()

    invalid_metadata_file = "tests/data/.surface_invalid.bin.yml"
    # Create a metadata file with invalid yml
    with open(invalid_metadata_file, "w") as f:
        yaml.dump("This is invalid yml", f)

    # Make copy of binary to match the modified metadata file
    surface_file_copy = "tests/data/surface_invalid.bin"
    shutil.copy(
        surface_file,
        surface_file_copy,
    )

    # Create new entry in export manifest
    new_entry =  {
            "absolute_path": str(Path.cwd() / "tests/data/surface_invalid.bin"),
            "exported_at": "2025-07-22T08:07:52.197429Z",
            "exported_by": "TEST",
        }

    with open(manifest_file, "r") as f:
        manifest = json.load(f)
        manifest.append(new_entry)
    
    os.remove(manifest_file)

    with open(manifest_file, "w") as f:
        json.dump(manifest, f, indent=4)

    with pytest.warns(UserWarning, match="No metadata*"):
        e.add_files()

    e.upload()
    time.sleep(1)

    # Assert parent and only 1 valid child are on Sumo
    total = _hits_for_case(sumoclient, e.fmu_case_uuid)
    assert total == 2

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumoclient.delete(path=path)

    os.remove(surface_file_copy)
    os.remove(invalid_metadata_file)


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
            casepath=CASEPATH,
            sumoclient=sumoclient,
        )
        e.register()


def test_schema_error_in_child(
    token, case_metadata, surface_file, surface_metadata_file, manifest_file, sumo_uploads_file
):
    """
    Try to upload files where one does have metadata with error. Assert that warning is given
    and that upload commences with the other files. Check that the children are present.
    """
    sumoclient = SumoClient(env=ENV, token=token)

    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        casepath=CASEPATH,
        sumoclient=sumoclient,
    )
    e.register()

    # Create a metadata file with an error
    with open(surface_metadata_file) as f:
        parsed_yaml = yaml.safe_load(f)
    parsed_yaml["fmu"]["realizationiswrong"] = parsed_yaml["fmu"][
        "realization"
    ]
    del parsed_yaml["fmu"]["realization"]
    parsed_yaml["masterdata_INVALID_SCHEMA"] = parsed_yaml["masterdata"]
    del parsed_yaml["masterdata"]
    error_metadata_file = "tests/data/.surface_error.bin.yml"
    with open(error_metadata_file, "w") as f:
        yaml.dump(parsed_yaml, f)

    # Make copy of binary to match the modified metadata file
    error_surface_file = "tests/data/surface_error.bin"
    shutil.copy(
        surface_file,
        error_surface_file,
    )

     # Create new entry in export manifest
    absolute_path = str(Path.cwd() / "tests/data/surface_error.bin")
    new_entry =  {
            "absolute_path": absolute_path,
            "exported_at": "2024-01-01T08:07:52.197429Z",
            "exported_by": "TEST",
        }

    with open(manifest_file, "r") as f:
        manifest = json.load(f)
        manifest.insert(0, new_entry)

    os.remove(manifest_file)

    with open(manifest_file, "w") as f:
        json.dump(manifest, f, indent=4)

    e.add_files()
    e.upload()
    time.sleep(1)

    # Assert parent and valid child are on Sumo
    total = _hits_for_case(sumoclient, e.fmu_case_uuid)
    assert total == 2
    assert os.path.exists(sumo_uploads_file)

    with open(sumo_uploads_file) as f:
        uploads = json.load(f)

    # Asset that there is one record in sumo uploads log, that the last_index_manifest is the length of manifest, which is 1 for this test.
    assert len(uploads) == 1
    assert uploads[-1]["last_index_manifest"] == 1

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumoclient.delete(path=path)

    os.remove(error_surface_file)
    os.remove(error_metadata_file)


def test_multiple_exports_to_manifest_append_to_sumo_uploads(token, case_metadata, surface_file, surface_metadata_file, manifest_file, sumo_uploads_file):
    """
    Upload new files added to the export manifest and new entry should be appended to the Sumo uploads log.
    """
    sumoclient = SumoClient(env=ENV, token=token)

    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        casepath=CASEPATH,
        sumoclient=sumoclient,
    )
    e.register()

    # Assert that there is 1 file added.
    e.add_files()
    assert len(e.files) == 1

    e.upload()

    # Assert that there is a sumo uploads log file and it has 1 entry.
    assert os.path.exists(sumo_uploads_file)
    with open(sumo_uploads_file, "r") as f:
        sumo_uploads = json.load(f)
    assert len(sumo_uploads) == 1

    # Create new entry in export manifest
    metadata_file_copy = os.path.join(os.path.dirname(surface_file), ".surface_copy.bin.yml")
    surface_file_copy = os.path.join(os.path.dirname(surface_file), "surface_copy.bin")
    shutil.copy(surface_metadata_file, metadata_file_copy)
    shutil.copy(surface_file, surface_file_copy)

    new_entry =  {
        "absolute_path": surface_file_copy,
        "exported_at": datetime.datetime.now(datetime.UTC).isoformat().replace('+00:00', 'Z'),
        "exported_by": "TEST",
    }

    with open(manifest_file, "r") as f:
        manifest = json.load(f)
        manifest.append(new_entry)

    os.remove(manifest_file)
    with open(manifest_file, "w") as f:
        json.dump(manifest, f, indent=4)


    # Assert that there is 1 new file added.
    e.add_files()
    assert len(e.files) == 2

    e.upload()
    time.sleep(1)

    with open(sumo_uploads_file, "r") as f:
        sumo_uploads = json.load(f)
    
    with open(manifest_file, "r") as f:
        manifest = json.load(f)
    
    assert len(sumo_uploads) == 2
    assert sumo_uploads[-1]["last_index_manifest"] == len(manifest) - 1

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumoclient.delete(path=path)

    os.remove(surface_file_copy)
    os.remove(metadata_file_copy)


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
def test_seismic_openvds_file(token, case_metadata, segy_file):
    """Upload seimic in OpenVDS format to Sumo. Assert that it is there."""
    sumoclient = SumoClient(env=ENV, token=token)

    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        casepath=CASEPATH,
        sumoclient=sumoclient,
    )
    e.register()
    time.sleep(1)

    e.add_files()
    e.upload()
    time.sleep(1)

    # Read the parent object from Sumo
    query = f"_sumo.parent_object:{e.fmu_case_uuid} AND NOT class:ensemble AND NOT class:realization"
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
                    == os.stat(segy_file).st_size
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
def test_sumo_mode_default(
    token, case_metadata, surface_file, surface_metadata_file, manifest_file, sumo_uploads_file
):
    """
    Test that SUMO_MODE defaults to copy, i.e. not deleting file after upload.
    """
    sumoclient = SumoClient(env=ENV, token=token)

    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        casepath=CASEPATH,
        sumoclient=sumoclient,
    )
    e.register()

    # Add a valid child
    e.add_files()

    e.upload()
    time.sleep(1)

    # Assert parent and valid child are on Sumo
    total = _hits_for_case(sumoclient, e.fmu_case_uuid)
    assert total == 2

    # Assert that child file and metadatafile are not deleted
    assert os.path.exists(surface_file)
    assert os.path.exists(surface_metadata_file)
    assert os.path.exists(manifest_file)
    assert os.path.exists(sumo_uploads_file)

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumoclient.delete(path=path)


@pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="do not run on windows due to file-path differences",
)
def test_sumo_mode_copy(
    token, case_metadata, surface_file, surface_metadata_file, manifest_file, sumo_uploads_file
):
    """
    Test SUMO_MODE=copy, i.e. not deleting file after upload.
    """
    sumoclient = SumoClient(env=ENV, token=token)

    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        casepath=CASEPATH,
        sumoclient=sumoclient,
        sumo_mode="copy",
    )
    e.register()

    # Add a valid child
    e.add_files()

    e.upload()
    time.sleep(1)

    # Assert parent and valid child are on Sumo
    total = _hits_for_case(sumoclient, e.fmu_case_uuid)
    assert total == 2

    # Assert that child file and metadatafile are not deleted
    assert os.path.exists(surface_file)
    assert os.path.exists(surface_metadata_file)
    assert os.path.exists(manifest_file)
    assert os.path.exists(sumo_uploads_file)

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumoclient.delete(path=path)


@pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="do not run on windows due to file-path differences",
)
def test_sumo_mode_move(
    token, case_metadata, surface_file, surface_metadata_file
):
    """
    Test SUMO_MODE=move, i.e. deleting file after upload.
    """
    sumoclient = SumoClient(env=ENV, token=token)

    e = uploader.CaseOnDisk(
        case_metadata_path=case_metadata,
        casepath=CASEPATH,
        sumoclient=sumoclient,
        sumo_mode="moVE",  # test case-insensitive
    )
    e.register()

    # Add a valid child
    e.add_files()

    e.upload()
    time.sleep(1)

    # Assert parent and valid child are on Sumo
    total = _hits_for_case(sumoclient, e.fmu_case_uuid)
    assert total == 2

    # Assert that the files on disk are deleted
    assert not os.path.exists(surface_file)
    assert not os.path.exists(surface_metadata_file)

    # Delete this case
    logger.debug("Cleanup after test: delete case")
    path = f"/objects('{e.sumo_parent_id}')"
    sumoclient.delete(path=path)
