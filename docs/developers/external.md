# External Data

## Introduction

All Pipelines begin with data returned from an `ExternalTask` that
does no processing and just returns the existing external data.

This section describes common external data stores and recommends
techniques for accessing the data.

## External Data Stores

### KoboToolbox

KoboToolbox is a widely used ODK-compatible data collection tool.
Data collected using KoboToolbox is accessible via a REST API.
In order to access data you will need a user account on the
KoboToolbox server and an API Token. You can retrieve your own
API token by accessing `/kobocat/api/v1/user` on that server
from a web browser on your computer.

In order to download data from KoboToolbox the typical approach is
to use a `RESTExternalTask` to access KoboToolbox and then pass
that to a `ReadDataFrameTask`.

Most projects will only use a single KoboToolbox server, and so the
best way to configure credentials, etc. is to create a parent task.
For example, in `tasks.common.tasks` create:

```python
class KoboExternal(RESTExternalTask):
    """
    Fetch data from KoboToolbox

    Subclass RESTExternal to provide a convenient place to set
    parameters in luigi.cfg

    luigi.cfg should contain a section like:

    [KoboExternal]
    server=forms.domain.org
    auth=Token 12345567789abvdefghijklmnpsjjr
    path=kobocat/api/v1/extract

    The `server` should be set to the correct server name

    The `auth` should be set to your API token prefixed by 'Token '.
    You can retrieve your API Token by visiting the
    /kobocat/api/v1/user page on the server
    """
    pass
```

As described in the comments in the `Task` you can put default values
for your server, authentication token and the REST API end point into
`luigi.cfg` and then use them in all `Tasks` that inherit from
`KoboExternal`.

If you have a KoboToolbox server in your project and set up a `Task`
like `KoboExternal` then you should add a section to your `luigi.cfg.example` so
that it is clear to new users that they need to set those parameters.

A typical entry in `luigi.cfg` looks like:

```
[KoboExternal]
server=forms.domain.org
auth=Token 123456789abcdefghijklmnopsqrstuwxyz
path=kobocat/api/v1/extract
```

Note that the `auth` parameter must contain the word "Token" followed by a space and
then the value of the token.

Once the configuration file is setup, you can define `Tasks` to read the remote data:

```python
class KoboReadDataFrame(ReadDataFrameTask):

    read_method = 'read_csv'
    timeout = 60 * 60 * 24  # Set the timeout (in seconds) appropriately

    def requires(self):
        form = 40
        sections = 'Section_1'
        return KoboExternal(params={'form': form,
                                    'section': section,
                                    'format': 'csv'})
```

Note that you must explicitly specify the `read_method`.

You will also want to set the `timeout` appropriately to ensure that
you are not having to frequently download data from the KoboToolbox
server if it has not changed.

Typically, you should specify a `section` as well. If not, the output
will be a dataframe containing all the possible combinations of rows in repeat
sections for each form submission (i.e. the Cartesian product or Product Set).
It is very unlikely that this is the most useful output format.

If you provide a `section` you will receive a `DataFrame`
containing one row for each entry in that section or repeat, with columns for the
questions in that `section` *and* the questions for all sections at a higher
level that aren't a repeat themselves. I.e. the dataframe will contain the
equivalent of a SQL `LEFT JOIN` to all the parent sections. This also means
that if you specify the first section of a form, which is normally the
identifier section, the resulting `DataFrame` will actually contain all
top-level sections that are not a repeat.

If you will use multiple `sections` then it is best to create multiple
upstream dependencies in your `ReadDataFrame`:

```python
class KoboReadDataFrame(ReadDataFrameTask):

    read_method = 'read_csv'
    timeout = 60 * 60 * 24  # Set the timeout (in seconds) appropriately

    def requires(self):
        form = 40
        sections = [
            'Section_1',  # This will include all sections that are not repeats
            'Section_2/repeat_1',  # This will be a flat dataframe with one row per entry in the repeat
        ]
        # Return a dict of DataFrames with the section name as the key
        return {section: KoboExternal(params={'form': form, 'section': section, 'format': 'csv'}) 
                for section in sections}
```

You can then process the resulting `DataFrames` as you would if they had been read from disk. For example:

```python
class CheckKoboColumns(Task):

    def requires(self):
        return KoboReadDataFrame()

    def run(self):
        dfs = self.input().get()
        for section, df in dfs.items():
            logger.info(f'Section {section} has shape {df.shape} and columns:')
            logger.info(df.columns)
```