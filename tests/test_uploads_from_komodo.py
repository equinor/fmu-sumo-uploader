"""Verifies SUMO uploads that has been run by Github Actions in the
komodo-releases repo. See
https://github.com/equinor/komodo-releases/blob/main/.github/workflows/run_drogon.yml

Verify that the expected number of objects of each expected type have been
uploaded. Note that komodo-releases repo usually runs multiple times every
day, and that many failed runs is to be expected, i.e. do not expect
every upload to be perfect.
Tests of aggregations are not in scope here, see the Sumo aggregation repo
for those tests.
"""

import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from random import randint, seed

import pytest
import xtgeo

from fmu.sumo.explorer import Explorer

if not sys.platform.startswith("darwin"):
    import openvds

# Run the tests from the root dir
TEST_DIR = Path(__file__).parent / "../"
os.chdir(TEST_DIR)


logger = logging.getLogger(__name__)
logger.setLevel(level="DEBUG")

if os.getenv("GITHUB_ACTIONS") == "true":
    RUNNING_OUTSIDE_GITHUB_ACTIONS = "False"
    print(
        "Found the GITHUB_ACTIONS env var, so I know I am running on Github now. Will run these tests."
    )
else:
    RUNNING_OUTSIDE_GITHUB_ACTIONS = "True"
    msg = "Skipping these tests since they make most sense to run on Github Actions only"
    print("\nNOT running on Github now.", msg)
    pytest.skip(msg, allow_module_level=True)


@pytest.fixture(name="explorer")
def fixture_explorer(token: str) -> Explorer:
    """Returns an explorer to every test which have 'explorer: Explorer' as argument"""
    return Explorer("dev", token=token)


def _get_creation_date(metadata):
    """Returns the date this case was created, based on the case metadata"""
    for item in metadata["tracklog"]:
        if item["event"] == "created":
            timestr = item["datetime"]
            if timestr.endswith("Z"):
                timestr = timestr.replace("Z", "+00:00")
            if "+" not in timestr:
                timestr = timestr + "+00:00"
            dt = datetime.strptime(timestr, "%Y-%m-%dT%H:%M:%S.%f%z")
            return dt


def _get_suitable_cases(explorer):
    """Returns list of suitable cases that was uploaded by f_scout_ci
    recently"""
    cases = explorer.cases
    cases = cases.filter(user="f_scout_ci")
    assert len(cases) > 0
    selected = []
    an_hour_ago = datetime.now(timezone.utc) + timedelta(hours=-1)
    a_day_ago = datetime.now(timezone.utc) + timedelta(hours=-24)
    for case in cases:
        case_created = _get_creation_date(case.metadata)
        if case_created > an_hour_ago:
            # Could still be uploading, skip
            print("Too new: ", case_created, case.uuid)
            continue
        if case_created < a_day_ago:
            # Too old for this nightly test
            print("Too old:", case_created, case.uuid)
            continue
        if len(case.realizations) < 1:
            # Skip cases wo realizations, typically these are
            # failed runs in komodo-releases / ert
            print("Too few realizations:", case_created, case.uuid)
            continue
        selected.append(case)

    print("Number of cases selected for further testing:", len(selected))
    for case in selected:
        print("   ", _get_creation_date(case.metadata), case.uuid)

    assert len(selected) > 0, "Could not find any suitable case for testing"
    return selected


def test_case_consistency(explorer: Explorer):
    """Test internal consistency on cases uploaded from komodo-releases"""
    cases = _get_suitable_cases(explorer)
    non_consistent_cases = 0
    for case in cases:
        mismatches_request = explorer._sumo.get(
            f"/admin/mismatches?case={case.uuid}"
        )
        mismatches_res_location = mismatches_request.headers.get("location")
        res_url = mismatches_res_location.removeprefix(explorer._sumo.base_url)
        time.sleep(3)  # Give time for result to be ready
        res = explorer._sumo.get(res_url)
        metadata_wo_blobs = len(res.json().get("metadata_without_blobs"))
        blobs_wo_metadata = len(res.json().get("blobs_without_metadata"))
        if metadata_wo_blobs > 0 or blobs_wo_metadata > 0:
            print("NOT consistent case:", case.uuid, res.json())
            non_consistent_cases += 1

    print(f"{non_consistent_cases} NON-consistent cases out of {len(cases)}")
    assert non_consistent_cases == 0, "One or more cases are NOT consistent"


def test_case_surfaces(explorer: Explorer):
    """Test surfaces from cases uploaded from komodo-releases"""
    cases = _get_suitable_cases(explorer)
    perfect_cases = 0
    for case in cases:
        realizations = len(case.realizations)
        iter_count = 0
        real_count = 0
        preproc_count = 0
        for surf in case.surfaces:
            assert surf.uuid
            assert surf.name
            assert surf.tagname
            if surf.metadata.get("fmu").get("aggregation") is not None:
                continue
            if surf.iteration is not None:
                iter_count += 1
            if surf.realization is not None:
                real_count += 1
            if surf.iteration is None and surf.realization is None:
                preproc_count += 1
        if (
            iter_count >= 54 * realizations
            and real_count >= 54 * realizations
            and preproc_count >= 33
        ):
            print(
                "'Perfect' surface case:",
                case.uuid,
                iter_count,
                real_count,
                preproc_count,
                realizations,
            )
            perfect_cases += 1
        else:
            print(
                "'NOT perfect' surface case:",
                case.uuid,
                iter_count,
                real_count,
                preproc_count,
                realizations,
            )

    # Will not test every blob element,
    # just test that a random blob can be read
    seed()
    random_index = randint(0, len(case.surfaces) - 1)
    reg = case.surfaces[random_index].to_regular_surface()
    assert type(reg) is xtgeo.RegularSurface, (
        "Failed to read content of a blob"
    )

    print(f"{perfect_cases} 'perfect' cases out of {len(cases)}")
    # There could be many failed runs from komodo-release repo,
    # so lets be happy if we find 1 or more 'perfect' cases.
    assert perfect_cases > 0, "None of the cases satisfy the surface test"


def test_case_tables(explorer: Explorer):
    """Test tables from cases uploaded from komodo-releases"""
    cases = _get_suitable_cases(explorer)
    perfect_cases = 0
    for case in cases:
        realizations = len(case.realizations)
        iter_count = 0
        real_count = 0
        tagname_count = 0
        # SIM2SUMO uploads:
        drogon_rft_count = 0
        drogon_satfunc_count = 0
        drogon_summary_count = 0

        for tbl in case.tables:
            assert tbl.uuid
            assert tbl.name
            if tbl.metadata.get("fmu").get("aggregation") is not None:
                continue
            if tbl.iteration is not None:
                iter_count += 1
            if tbl.realization is not None:
                real_count += 1
            if tbl.tagname is not None:
                tagname_count += 1
            if tbl.name == "DROGON" and tbl.tagname == "rft":
                drogon_rft_count += 1
            if tbl.name == "DROGON" and tbl.tagname == "satfunc":
                drogon_satfunc_count += 1
            if tbl.name == "DROGON" and tbl.tagname == "summary":
                drogon_summary_count += 1
        if (
            iter_count >= 7 * realizations
            and real_count >= 7 * realizations
            and tagname_count >= 7 * realizations
            and drogon_rft_count >= 1 * realizations
            and drogon_satfunc_count >= 1 * realizations
            and drogon_summary_count >= 1 * realizations
        ):
            print(
                "'Perfect' table case:",
                case.uuid,
                iter_count,
                real_count,
                tagname_count,
                realizations,
            )
            perfect_cases += 1
        else:
            print(
                "'NOT perfect' table case:",
                case.uuid,
                iter_count,
                real_count,
                tagname_count,
                realizations,
            )

    # Will not test every blob element,
    # just test that a random blob can be read
    seed()
    random_index = randint(0, len(case.tables) - 1)
    arrow = case.tables[random_index].to_arrow()
    arrow.validate()

    print(f"{perfect_cases} 'perfect' cases out of {len(cases)}")
    # There could be many failed runs from komodo-release repo,
    # so lets be happy if we find 1 or more 'perfect' cases.
    assert perfect_cases > 0, "None of the cases satisfy the table test"


def test_case_polygons(explorer: Explorer):
    """Test polygons from cases uploaded from komodo-releases"""
    cases = _get_suitable_cases(explorer)
    perfect_cases = 0
    for case in cases:
        realizations = len(case.realizations)
        iter_count = 0
        real_count = 0
        tagname_count = 0
        for poly in case.polygons:
            assert poly.uuid
            assert poly.name
            if poly.metadata.get("fmu").get("aggregation") is not None:
                continue
            if poly.iteration is not None:
                iter_count += 1
            if poly.realization is not None:
                real_count += 1
            if poly.tagname is not None:
                tagname_count += 1
        if (
            iter_count >= 6 * realizations
            and real_count >= 6 * realizations
            and tagname_count >= 6 * realizations
        ):
            print(
                "'Perfect' polygon case:",
                case.uuid,
                iter_count,
                real_count,
                tagname_count,
                realizations,
            )
            perfect_cases += 1
        else:
            print(
                "'NOT perfect' polygon case:",
                case.uuid,
                iter_count,
                real_count,
                tagname_count,
                realizations,
            )

    # Will not test every blob element,
    # just test that a random blob can be read
    seed()
    random_index = randint(0, len(case.polygons) - 1)
    case.polygons[random_index].to_pandas()

    print(f"{perfect_cases} 'perfect' cases out of {len(cases)}")
    # There could be many failed runs from komodo-release repo,
    # so lets be happy if we find 1 or more 'perfect' cases.
    assert perfect_cases > 0, "None of the cases satisfy the polygon test"


def test_case_dictionaries(explorer: Explorer):
    """Test dictionaries from cases uploaded from komodo-releases"""
    cases = _get_suitable_cases(explorer)
    perfect_cases = 0
    for case in cases:
        realizations = len(case.realizations)
        iter_count = 0
        real_count = 0
        tagname_count = 0
        name_parameter_found = False
        for dct in case.dictionaries:
            assert dct.uuid
            assert dct.name
            if dct.metadata.get("fmu").get("aggregation") is not None:
                continue
            if dct.iteration is not None:
                iter_count += 1
            if dct.realization is not None:
                real_count += 1
            if dct.tagname is not None:
                tagname_count += 1
            if dct.name == "parameters":
                name_parameter_found = True
        if (
            iter_count >= 1 * realizations
            and real_count >= 1 * realizations
            and tagname_count >= 1 * realizations
            and name_parameter_found
        ):
            print(
                "'Perfect' dictionary case:",
                case.uuid,
                iter_count,
                real_count,
                tagname_count,
                name_parameter_found,
                realizations,
            )
            perfect_cases += 1
        else:
            print(
                "'NOT perfect' dicationary case:",
                case.uuid,
                iter_count,
                real_count,
                tagname_count,
                name_parameter_found,
                realizations,
            )

    # Will not test every blob element,
    # just test that a random blob can be read
    seed()
    random_index = randint(0, len(case.dictionaries) - 1)
    obj = case.dictionaries[random_index]
    obj._blob

    print(f"{perfect_cases} 'perfect' cases out of {len(cases)}")
    # There could be many failed runs from komodo-release repo,
    # so lets be happy if we find 1 or more 'perfect' cases.
    assert perfect_cases > 0, "None of the cases satisfy the dictionary test"


@pytest.mark.skipif(
    sys.platform.startswith("darwin"),
    reason="do not run OpenVDS on mac os",
)
def test_case_seismic(explorer: Explorer):
    """Test seismic cubes in cases uploaded from komodo-releases"""
    cases = _get_suitable_cases(explorer)
    perfect_cases = 0
    for case in cases:
        realizations = len(case.realizations)
        iter_count = 0
        real_count = 0
        for cube in case.cubes:
            assert cube.uuid
            assert cube.name
            assert cube.tagname
            if cube.metadata.get("fmu").get("aggregation") is not None:
                continue
            if cube.iteration is not None:
                iter_count += 1
            if cube.realization is not None:
                real_count += 1
        if iter_count >= 10 * realizations and real_count >= 10 * realizations:
            print(
                "'Perfect' seismic case:",
                case.uuid,
                iter_count,
                real_count,
                realizations,
            )
            perfect_cases += 1
        else:
            print(
                "'NOT perfect' seismic case:",
                case.uuid,
                iter_count,
                real_count,
                realizations,
            )

    # Will not test every blob element,
    # just test that a random blob can be read
    seed()
    random_index = randint(0, len(case.cubes) - 1)
    cube = case.cubes[random_index]
    handle = cube.openvds_handle
    layout = openvds.getLayout(handle)
    channel_count = layout.getChannelCount()
    assert channel_count > 0

    print(f"{perfect_cases} 'perfect' cases out of {len(cases)}")
    # There could be many failed runs from komodo-release repo,
    # so lets be happy if we find 1 or more 'perfect' cases.
    assert perfect_cases > 0, "None of the cases satisfy the seismic test"
