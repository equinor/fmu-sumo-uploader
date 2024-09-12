#!/usr/bin/env python

"""Upload data to Sumo from FMU."""

import warnings
import os
import argparse
import logging
from pathlib import Path
from ert.shared.plugins.plugin_manager import hook_implementation  # type: ignore
from ert.shared.plugins.plugin_response import plugin_response  # type: ignore

try:
    from ert import ErtScript  # type: ignore
except ModuleNotFoundError:
    from res.job_queue import ErtScript  # type: ignore

from fmu.sumo import uploader
from fmu.sumo.uploader._logger import get_uploader_logger

logger = get_uploader_logger()


# This documentation is for sumo_uploader as an ERT workflow
DESCRIPTION = """SUMO_UPLOAD will upload files to Sumo. The typical use case
is as add-on to post-processing workflows which aggregate data across an
ensemble and stores the results outside the realization folders.

SUMO_UPLOAD depends on the current case being registered in Sumo (done through
the ``WF_CREATE_CASE_METADATA`` workflow job) and on data being exported
with ``fmu-dataio``.

``fmu-dataio`` must be used to produce metadata for each file
to be uploaded to Sumo.

The ``WF_CREATE_CASE_METADATA`` workflow job must run *before* all SUMO_UPLOAD
instances to ensure the case is registered in Sumo before data are uploaded.

SUMO_UPLOAD is implemented both as FORWARD_JOB and WORKFLOW_JOB and can be called from
both contexts when running ERT.

It is recommended to upload files immediately after they are produced, rather than
lumping all SUMO_UPLOADs at the end of the ERT config file.

"""

EXAMPLES = """``<SUMO_ENV>`` must be defined. It is typically defined in the ERT config,
and normally set to ``prod``.

``<SUMO_CASEPATH>`` must be defined. It is typically defined in the ERT config,
and normally set to ``<SCRATCH>/<USER>/<CASE_DIR>``
e.g. ``/scratch/myfield/myuser/mycase/``

Note! Filenames produced by FMU workflows use "--" as separator. Avoid this
string in searchpaths, as it will cause following text to be parsed as a comment.

FORWARD_MODEL example::

  FORWARD_MODEL XX -- Some other job that makes data
  FORWARD_MODEL SUMO_UPLOAD(<SEARCHPATH>="share/results/maps/*.gri")
  FORWARD_MODEL SUMO_UPLOAD(<SEARCHPATH>="share/results/polygons/*.csv")

WORKFLOW_JOB example::

  <MY_JOB> -- The workflow job that creates data
  SUMO_UPLOAD <SUMO_CASEPATH> "<SUMO_CASEPATH>/share/observations/maps/*.gri"  <SUMO_ENV>

"""


def main() -> None:
    """Entry point from command line (e.g. ERT FORWARD_JOB)."""

    parser = _get_parser()
    args = parser.parse_args()

    logger.setLevel(logging.INFO)

    if args.verbose:
        logger.setLevel(logging.INFO)
    if args.debug:
        logger.setLevel(logging.DEBUG)

    # Legacy? Still needed?
    args.casepath = os.path.expandvars(args.casepath)
    args.searchpath = os.path.expandvars(args.searchpath)

    _check_arguments(args)

    sumo_upload_main(
        casepath=args.casepath,
        searchpath=args.searchpath,
        env=args.env,
        metadata_path=args.metadata_path,
        threads=args.threads,
        config_path=args.config_path,
        parameters_path=args.parameters_path,
        sumo_mode=args.sumo_mode,
        verbosity=logging.INFO,
    )


def sumo_upload_main(
    casepath: str,
    searchpath: str,
    env: str,
    metadata_path: str,
    threads: int,
    config_path: str = "fmuconfig/output/global_variables.yml",
    parameters_path: str = "parameters.txt",
    sumo_mode: str = "copy",
    verbosity: int = logging.INFO,
) -> None:
    """A "main" function that can be used both from command line and from ERT workflow"""

    logger.setLevel(verbosity)
    logger.debug("Running fmu_uploader_main()")

    # Catch-all to ensure FMU workflow keeps running even if something happens.
    # This should be a temporary solution to be re-evaluated in the future.

    try:
        # establish the connection to Sumo
        sumo_connection = uploader.SumoConnection(env=env)
        logger.info("Connection to Sumo established, env=%s", env)

        # initiate the case on disk object
        logger.info("Case-relative metadata path is %s", metadata_path)
        case_metadata_path = Path(casepath) / Path(metadata_path)
        logger.info("case_metadata_path is %s", case_metadata_path)

        logger.info("Sumo mode: %s", sumo_mode)

        e = uploader.CaseOnDisk(
            case_metadata_path,
            sumo_connection,
            verbosity,
            sumo_mode,
            config_path,
            parameters_path,
        )
        # add files to the case on disk object
        logger.info("Adding files. Search path is %s", searchpath)
        e.add_files(searchpath)
        logger.info("%s files has been added", str(len(e.files)))

        if len(e.files) == 0:
            logger.debug("There are 0 (zero) files.")
            warnings.warn("No files found - aborting ")
            return

        # upload the indexed files
        logger.info("Starting upload")
        e.upload(threads=threads)
        logger.info("Upload done")
    except Exception as err:
        logger.warning(f"Problem related to Sumo upload: {err} {type(err)}")
        _sumo_logger = sumo_connection.api.getLogger("fmu-sumo-uploader")
        _sumo_logger.propagate = False
        _sumo_logger.warning(
            "Problem related to Sumo upload for case: %s; %s %s",
            case_metadata_path,
            err,
            type(err),
            extra={"objectUuid": e.fmu_case_uuid},
        )
        return


class SumoUpload(ErtScript):
    """A class with a run() function that can be registered as an ERT plugin.

    This is used for the ERT workflow context."""

    # pylint: disable=too-few-public-methods
    def run(self, *args):
        # pylint: disable=no-self-use
        """Parse with a simplified command line parser, for ERT only,
        call sumo_upload_main()"""

        logger.setLevel(logging.WARNING)

        logger.debug("Calling run() on SumoUpload")
        parser = _get_parser()
        args = parser.parse_args(args)
        _check_arguments(args)
        sumo_upload_main(
            casepath=args.casepath,
            searchpath=args.searchpath,
            env=args.env,
            metadata_path=args.metadata_path,
            threads=args.threads,
            config_path=args.config_path,
            parameters_path=args.parameters_path,
            sumo_mode=args.sumo_mode,
            verbosity=logging.WARNING,
        )


def _get_parser() -> argparse.ArgumentParser:
    """Construct parser object for sumo_upload."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "casepath", type=str, help="Absolute path to case root"
    )
    parser.add_argument(
        "searchpath",
        type=str,
        help="path relative to runpath for files to upload",
    )
    parser.add_argument("env", type=str, help="Sumo environment to use.")
    parser.add_argument(
        "--config_path",
        type=str,
        help="path to global variables relative to runpath",
        default="fmuconfig/output/global_variables.yml",
    )
    parser.add_argument(
        "--sumo_mode",
        type=str,
        help="copy or move files to cloud storage",
        default="copy",
    )

    parser.add_argument(
        "--threads", type=int, help="Set number of threads to use.", default=2
    )
    parser.add_argument(
        "--metadata_path",
        type=str,
        help="Case-relative path to case metadata",
        default="share/metadata/fmu_case.yml",
    )
    parser.add_argument(
        "--parameters_path",
        type=str,
        help="path to parameters.txt",
        default="parameters.txt",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Verbose output"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Debug output, more verbose than --verbose",
    )

    return parser


def _check_arguments(args) -> None:
    """Do sanity check of the input arguments."""

    logger.debug("Running check_arguments()")
    logger.debug("Arguments are: %s", str(vars(args)))

    if args.env not in ["preview", "dev", "test", "prod", "localhost"]:
        warnings.warn(f"Non-standard environment: {args.env}")

    if not Path(args.casepath).is_absolute():
        if args.casepath.startswith("<") and args.casepath.endswith(">"):
            ValueError("ERT variable is not defined: %s", args.casepath)
        raise ValueError(
            "Provided casepath must be an absolute path to the case root"
        )

    if not Path(args.casepath).exists():
        raise ValueError("Provided case path does not exist")

    logger.debug("check_arguments() has ended")


@hook_implementation
def legacy_ertscript_workflow(config):
    """Hook the SumoUpload class into ERT with the name SUMO_UPLOAD,
    and inject documentation"""
    workflow = config.add_workflow(SumoUpload, "SUMO_UPLOAD")
    workflow.parser = _get_parser
    workflow.description = DESCRIPTION
    workflow.examples = EXAMPLES
    workflow.category = "export"


@hook_implementation
@plugin_response(plugin_name="SUMO_UPLOAD")
def job_documentation(job_name):
    if job_name != "SUMO_UPLOAD":
        return None

    return {
        "description": DESCRIPTION,
        "examples": EXAMPLES,
        "category": "export",
    }
