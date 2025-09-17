import os
import subprocess

from ert import (  # type: ignore
    ForwardModelStepDocumentation,
    ForwardModelStepJSON,
    ForwardModelStepPlugin,
    ForwardModelStepValidationError,
    plugin,
)
from ert.plugins.plugin_manager import hook_implementation

from fmu.sumo.uploader.scripts.sumo_upload import DESCRIPTION, EXAMPLES


class SumoUpload(ForwardModelStepPlugin):
    def __init__(self):
        super().__init__(
            name="SUMO_UPLOAD",
            command=[
                "sumo_upload",
                "<SUMO_CASEPATH>",
                "--config_path",
                "<SUMO_CONFIG_PATH>",
                "--parameters_path",
                "<PARAMETERS_PATH>",
                "--sumo_mode",
                "<SUMO_MODE>",
            ],
            default_mapping={
                "<SUMO_CONFIG_PATH>": "fmuconfig/output/global_variables.yml",
                "<PARAMETERS_PATH>": "parameters.txt",
                "<SUMO_MODE>": "copy",
            },
            stderr_file="sumo_upload.stderr",
            stdout_file="sumo_upload.stdout",
        )

    def validate_pre_realization_run(
        self, fm_step_json: ForwardModelStepJSON
    ) -> ForwardModelStepJSON:
        return fm_step_json

    def validate_pre_experiment(
        self, fm_step_json: ForwardModelStepJSON
    ) -> None:
        env = fm_step_json["environment"].get("SUMO_ENV", "prod")
        command = f"sumo_login -e {env} -m silent"
        return_code = subprocess.call(command, shell=True)

        err_msg = (
            "Your config uses Sumo, please authenticate"
            " by running the following in your terminal:"
            f" sumo_login{f' -e {env}' if env != 'prod' else ''}"
        )

        if return_code != 0:
            raise ForwardModelStepValidationError(err_msg)

    @staticmethod
    def documentation() -> ForwardModelStepDocumentation | None:
        return ForwardModelStepDocumentation(
            description=DESCRIPTION,
            examples=EXAMPLES,
            category="export",
            source_package="fmu.sumo.uploader",
        )


@hook_implementation
@plugin(name="fmu_sumo_uploader")
def installable_forward_model_steps():
    return [SumoUpload]
