# fmu-sumo-uploader
A Python library for uploading from FMU to sumo

### Concepts
`SumoConnection`: The SumoConnection object represents the connection to Sumo, and will handle authentication etc when initiated. This object uses the Sumo python wrapper under the hood.

`CaseOnDisk`: The CaseOnDisk object represents an ensemble of reservoir model realisations. The object relates to the case metadata. Individual files belonging to the case are represented as FileOnDisk objects.

`FileOnDisk`: The FileOnDisk object represents a single file in an FMU case, stored on the local disk.

`CaseOnJob`: Similar to CaseOnDisk, but does not refer to files on disk. Instead uses in-memory structures.

`FileOnJob`: Similar to FileOnDisk, but uses in-memory structures.

### workflow for uploading during ERT runs

HOOK (presim) workflow registering the case:
```python
from fmu.sumo import uploader

# Establish connection to Sumo
connection = sumo.SumoConnection()

# Initiate the case object
case = sumo.CaseOnDisk(
    case_metadata_path="/path/to/case_metadata.yml",
    sumo_connection=sumo_connection
    )

# Register the case on Sumo
# This uploads case metadata to Sumo
case.register()
```

FORWARD_JOB uploading data (can be repeated multiple times during a workflow):
```python
from fmu.sumo import uploader

# Establish connection to Sumo
connection = sumo.SumoConnection()

# Initiate the case object
case = sumo.CaseOnDisk(
    case_metadata_path="/path/to/case_metadata",
    sumo_connection=sumo_connection
    )

# Add file-objects to the case
case.add_files("/globable/path/to/files/*.gri")

# Upload case data objects (files)
case.upload()

```

## Developer setup
Run: `pip install .[dev]` to also install development requirements.


## Testing on top of Komodo
The uploader and [sim2sumo](https://github.com/equinor/fmu-sumo-sim2sumo) are both installed under `fmu/sumo/`.
This means that sim2sumo must also be installed to test a new version of the uploader on top of Komodo.

Example: Installing uploader from `mybranch` on top of Komodo bleeding
```
< Create a new komodo env from komodo bleeding >
< Activate the new env >

pip install git+https://github.com/equinor/fmu-sumo-uploader.git@mybranch
pip install git+https://github.com/equinor/fmu-sumo-sim2sumo.git
```

The [Explorer](https://github.com/equinor/fmu-sumo) is also installed under `fmu/sumo`. Meaning that if the testing scenario includes the Explorer then it should also be installed on top of Komodo.
```
pip install git+https://github.com/equinor/fmu-sumo.git
```

# Contributing
Want to contribute? Read our [contributing](./CONTRIBUTING.md) guidelines