fmu-sumo-uploader
#################

Short introduction
*******************

A python class that can be used by Sumo client applications to 
upload to the Sumo core server. 

This is primarily intended for Sumo developers. 

The Sumo API is described at 
`https://api.sumo.equinor.com/swagger-ui/ <https://api.sumo.equinor.com/swagger-ui/>`_

The data model and schema and the component used to create metadata is described at 
`https://fmu-dataio.readthedocs.io/en/latest/datamodel.html <https://fmu-dataio.readthedocs.io/en/latest/datamodel.html>`_

Information on Sumo can be found `here <https://doc-sumo-doc-prod.radix.equinor.com/>`_

Preconditions
*************

Access
------

For internal Equinor users: Apply for access to Sumo in Equinor AccessIT, search for Sumo.

Install
*******

For internal Equinor users, this package is included in the Komodo distribution. 
For other use cases it can be pip installed:

.. code-block:: 

   pip install git+https://github.com/equinor/fmu-sumo-uploader.git@main#egg=fmu-sumo-uploader

Initialization
**************

.. code-block:: python

   from fmu.sumo import uploader
   sumo_connection = uploader.SumoConnection()


Dependencies
************

`sumo-wrapper-python <https://sumo-wrapper-python.readthedocs.io/en/latest/>`_ 
is used for the actual communication with the Sumo core
server and handles authentication and network retries. 

The `fmu-dataio <https://fmu-dataio.readthedocs.io/en/latest/>`_ 
must be used upfront to create the metadata files for each blob file to be uploaded. 

Concepts
********

`SumoConnection`: The SumoConnection object represents the connection to Sumo, 
and will handle authentication etc when initiated. 
This object uses the Sumo python wrapper under the hood.

`CaseOnDisk`: The CaseOnDisk object represents an ensemble of reservoir 
model realisations. The object relates to the case metadata. 
Individual files belonging to the case are represented as FileOnDisk objects.

`FileOnDisk`: The FileOnDisk object represents a single file in an FMU case, 
stored on the local disk.

`CaseOnJob`: Similar to CaseOnDisk, but does not refer to files on disk. Instead 
uses in-memory structures.

`FileOnJob`: Similar to FileOnDisk, but uses in-memory structures.

Usage and examples
******************

Workflow for uploading during ERT runs:

HOOK (presim) workflow registering the case:

.. code-block:: python

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

FORWARD_JOB uploading data (can be repeated multiple times during a workflow):

.. code-block:: python

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

Testing on top of Komodo
************************

The uploader and `sim2sumo <https://github.com/equinor/fmu-sumo-sim2sumo/>`_ are 
both installed under `fmu/sumo/`. This means that sim2sumo must also be 
installed to test a new version of the uploader on top of Komodo.

Example: Installing uploader from `mybranch` on top of Komodo bleeding

.. code-block:: 

   < Create a new komodo env from komodo bleeding >
   < Activate the new env >

   pip install git+https://github.com/equinor/fmu-sumo-uploader.git@mybranch
   pip install git+https://github.com/equinor/fmu-sumo-sim2sumo.git

The `Explorer <https://github.com/equinor/fmu-sumo/>`_ is also installed under `fmu/sumo`. 
Meaning that if the testing scenario includes the Explorer then it should 
also be installed on top of Komodo.

.. code-block:: 

   pip install git+https://github.com/equinor/fmu-sumo.git


.. toctree::
   :maxdepth: 2
   :caption: Contents:
