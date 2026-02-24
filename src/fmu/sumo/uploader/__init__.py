"""Top-level package for fmu.sumo.uploader"""

try:
    from ._version import version

    __version__ = version
except ImportError:
    __version__ = "0.0.0"

import sumo.wrapper

from fmu.sumo.uploader.caseondisk import CaseOnDisk
from fmu.sumo.uploader.caseonjob import CaseOnJob

SumoConnection = sumo.wrapper.SumoClient

# from fmu.sumo.uploader._fileondisk import FileOnDisk
# from fmu.sumo.uploader._fileonjob import FileOnJob
