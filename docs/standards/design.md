# Pipeline Design Standards

## Documentation

All pipelines should contain a `README.md` in the root directory
that describes:

* what configuration needs to be done before the pipeline can be run
* what the main output wrapper Task is called (that produces the full
set of outputs)
* what the main individual outputs are
* what the main intermediate results are
* what the external inputs are

## Re-usability

In order to allow other analysts to reuse code, and to run pipelines,
any environment-specific variables, e.g. file paths, should be
specified in a configuration file and imported using a relative
import. That configuration file should be included in `.gitignore`
and the `README.md` should explain how to set up a configuration
file for a new analyst.

In general we prefer to use relative directories to hard-coded
paths in a configuration file. We assume that all pipelines run
from the project root directory.

Re-coding should always be done using dicts imported from a `mappings.py`
so that they only need to be maintained in a single, known location.

## Replicability

It is vital that we can replicate our analyses in other environments.
This allows us to prove our results to our clients, and to automate
production of regular outputs using pipelines running on servers.
The requirement for replicability places some constraints on how
pipelines are designed:

* All pipelines must start with data downloaded from a server, such as
KoboToolbox or CKAN. I.e. any `ExternalTask` must download data
from a remote location; an `ExternalTask` cannot output a `LocalTarget`.

## Collated Data

In general, we should produce a collated dataframe that contains
all relevant data, appropriately cleaned and coded, and then use
that dataframe as the basis for all relevant indicator calculations
and their outputs.

If we apply different data preparation steps to individual outputs we
risk producing inconsistent outputs.

Collated dataframes should:

* have meaningful variable names, preferably from the Question Library
* meaningful values - i.e. numeric codes in source data should be
replaced with meaningful codes
* be saved to both Microsoft Excel (or CSV) and a Pandas-friendly
format. We save to Excel to that data consumers who are not familiar
with Python can download the collated data and examine it
(provided they have appropriate permissions). We save to a Pandas-friendly
format (e.g. Feather, HDF5 or Parquet) to ensure that we can quickly and
easily import the data into a new Pipeline Task without worrying about
losing DataFrame specific information (dtypes, indexes, etc.).

## Outputs

* 