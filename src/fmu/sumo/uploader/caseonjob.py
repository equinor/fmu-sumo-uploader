"""Objectify an FMU case (results), for usage on Radix jobs."""

import logging
import warnings

from fmu.sumo.uploader._fileonjob import FileOnJob
from fmu.sumo.uploader._logger import get_uploader_logger
from fmu.sumo.uploader._sumocase import SumoCase

logger = get_uploader_logger()

# pylint: disable=C0103 # allow non-snake case variable names


class CaseOnJob(SumoCase):
    """Initialize the CaseOnJob object."""

    def __init__(
        self, case_metadata: dict, sumoclient, verbosity=logging.DEBUG
    ):
        super().__init__(case_metadata, sumoclient)
        logger.setLevel(level=verbosity)

        self.sumoclient = sumoclient

    @property
    def sumo_parent_id(self):
        return self._sumo_parent_id

    @property
    def fmu_case_uuid(self):
        return self._fmu_case_uuid

    @property
    def files(self):
        return self._files

    def add_files(self, byte_string, metadata):
        try:
            file = FileOnJob(byte_string=byte_string, metadata=metadata)
            self._files.append(file)
        except Exception as err:
            info = f"No metadata, skipping file: {err}"
            warnings.warn(info)

    def register(self):
        # Do nothing, as this is (presumably) called from an aggregation job, where the case object alread exists.
        return
