# fmu-sumo-uploader

[![Documentation Status](https://readthedocs.org/projects/fmu-sumo-uploader/badge/?version=latest)](https://fmu-sumo-uploader.readthedocs.io/en/latest/?badge=latest)
[![SCM Compliance](https://scm-compliance-api.radix.equinor.com/repos/equinor/fmu-sumo-uploader/badge)](https://scm-compliance-api.radix.equinor.com/repos/equinor/fmu-sumo-uploader/badge)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)


## Documentation and guidelines
[fmu-sumo-uploader documentation](https://fmu-sumo-uploader.readthedocs.io/en/latest/)

## Contribute
[Contribution guidelines](./CONTRIBUTING.md)

## Testing on top of Komodo
fmo-sumo-uploader and [sim2sumo](https://github.com/equinor/fmu-sumo-sim2sumo) are both installed under `fmu/sumo/`.
This means that sim2sumo must also be installed to test a new version of the uploader on top of Komodo.

Example: Installing the uploader from `mybranch` on top of Komodo bleeding
```
# Activate Komodo bleeding, csh
source /prog/res/komodo/bleeding/enable.csh
# or, activate Komodo bleeding, sh
source /prog/res/komodo/bleeding/enable

# Create a new komodo env from komodo bleeding
komodoenv ~/.komodoenv

# Activate the new env, csh
source ~/.komodoenv/enable.csh
# or, Activate the new env, sh
source ~/.komodoenv/enable

pip install git+https://github.com/equinor/fmu-sumo-uploader.git@mybranch
pip install git+https://github.com/equinor/fmu-sumo-sim2sumo.git --force-reinstall
```

The [Explorer](https://github.com/equinor/fmu-sumo) is also installed under `fmu/sumo`. Meaning that if the testing scenario includes the Explorer then it should also be installed on top of Komodo.
```
pip install git+https://github.com/equinor/fmu-sumo.git --force-reinstall
```

>[!NOTE]
> In order to upload to an environment other than `prod`, just set
> the environment variable `SUMO_ENV` accordingly: `setenv SUMO_ENV dev`
> (csh) or `export SUMO_ENV=dev`(sh).

> [!NOTE]
> In order to use the development version of the metadata schema,
> set the environment variable `DEV_SCHEMA`: `setenv DEV_SCHEMA 1` (csh)
> or `export DEV_SCHEMA=1`(sh).
