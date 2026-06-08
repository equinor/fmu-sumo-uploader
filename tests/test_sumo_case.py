from pathlib import Path

import pytest

from fmu.sumo.uploader._sumocase import _get_field_from_metadata
from fmu.sumo.uploader.caseondisk import _load_case_metadata

CASEPATH = Path.cwd() / "tests/data/"
CASE_METADATA_PATH = CASEPATH / "dummy_metadata.json"


def test_load_case_metadata():
    case_metadata = _load_case_metadata(CASE_METADATA_PATH)
    case_uuid = _get_field_from_metadata(case_metadata, "fmu.case.uuid")
    assert case_uuid == "DUMMY_CASE_UUID"


def test_load_case_metadata_invalid_field():
    case_metadata = _load_case_metadata(CASE_METADATA_PATH)
    with pytest.warns(UserWarning, match="Invalid metadata"):
        non_existent_field = _get_field_from_metadata(
            case_metadata, "fmu.case.non_existent_field"
        )
    assert non_existent_field is None
