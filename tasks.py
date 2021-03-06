import logging
import os
from typing import BinaryIO, Dict, List, Union

import geopandas as gpd
import luigi
import pandas as pd
from pipelines.targets import ExpiringLocalTarget, ExpiringMemoryTarget, RESTTarget
from xlrd import open_workbook

logger = logging.getLogger('luigi-interface')


class Task(luigi.Task):
    """
    A pipeline task.
    Placeholder Kimetrica sub-class for additional functionality, eg, unit tests.
    """
    pass


class IndicatorTask(Task):
    """
    A pipeline task that processes an Indicator.
    Placeholder Kimetrica sub-class for additional functionality, eg, documentation tests.
    """
    pass


class LocalTarget(luigi.LocalTarget):
    """
    A file system Target.
    Placeholder Kimetrica sub-class for additional functionality, eg, config-based porting between AWS and local file
    systems.
    """
    pass


class RESTExternalTask(luigi.ExternalTask):
    """
    Fetch data from an external server using a REST API
    """
    server = luigi.Parameter()
    auth = luigi.Parameter(default='')
    path = luigi.Parameter()
    params = luigi.DictParameter(default={})

    def output(self):
        return RESTTarget(self.path, self.params, server=self.server, auth=self.auth)


class ReadDataFrameTask(Task):
    """
    A Pipeline Task that reads one or more Pandas DataFrames from file(s) and returns them as a MemoryTarget
    """
    extensions = {
        '.xls': 'read_excel',
        '.xlsx': 'read_excel',
        '.csv': 'read_csv',
        '.h5': 'read_hdf',
        '.json': 'read_json',
    }

    timeout = luigi.IntParameter(default=120)
    read_method = luigi.Parameter(default='')
    read_args = luigi.DictParameter(default={})

    def requires(self):
        return NotImplementedError('Concrete subclasses must specify one or more Targets '
                                   'returning files readable by Pandas')

    def output(self):
        return ExpiringMemoryTarget(str(self), timeout=self.timeout)

    def get_read_method(self, filename):
        if self.read_method:
            return self.read_method
        extension = os.path.splitext(filename)[1]
        if extension in self.extensions:
            return self.extensions[extension]
        raise ValueError('Cannot determine read method for file %s' % filename)

    def get_read_args(self):
        return self.read_args

    def read_dataframe(self, path_or_buffer, read_method):
        if read_method == 'read_excel':
            wb = open_workbook(path_or_buffer, logfile=self.logger)
            logger.debug('Workbook %s contains sheets %s' % (path_or_buffer, wb.sheet_names()))

        df = read_method(path_or_buffer, **self.get_read_args())
        logger.debug(f'Successfully read dataframe with shape {df.shape}')
        return df

    def read_target(self, target):
        try:
            with target.open('r') as f:
                filename = f.name
                read_method = getattr(pd, self.get_read_method(filename))
                return self.read_dataframe(filename, read_method)
        except AttributeError:
            # Target doesn't support open, so assume that it supports `get()`
            # and that `get()` returns a file-like object or a path
            # If we have a file-like object then the read_method must be specified explicitly
            assert self.read_method, 'ReadDataFrame requires an explicit read_method unless there is a local file'
            read_method = getattr(pd, self.read_method)
            path_or_buffer = target.get()
            return self.read_dataframe(path_or_buffer, read_method)

    def run(self):
        targets = self.input()

        if isinstance(targets, luigi.Target):
            self.output().put(self.read_target(targets))
        elif isinstance(targets, dict):
            output = {key: self.read_target(target) for key, target in targets.items()}
            self.output().put(output)
        elif isinstance(targets, (list, tuple)):
            output = {os.path.basename(target.path): self.read_target(target) for target in targets}
            self.output().put(output)


class ReadGeoDataFrameTask(ReadDataFrameTask):
    """
    A Pipeline Task that reads one or more GeoPandas GeoDataFrames from file(s) and returns them as a MemoryTarget
    """
    extensions = None

    def read_dataframe(self, file_obj):
        with file_obj.open('r') as f:
            filename = f.name
            gdf = gpd.read_file(filename, **self.get_read_args())
            return gdf

    def run(self):
        targets = self.input()

        if isinstance(targets, luigi.Target):
            self.output().put(self.read_dataframe(targets))
        elif isinstance(targets, dict):
            output = {key: self.read_dataframe(target) for key, target in targets.items()}
            self.output().put(output)
        elif isinstance(targets, (list, tuple)):
            output = {os.path.basename(target.path): self.read_dataframe(target) for target in targets}
            self.output().put(output)


class DataFrameOutputTask(Task):
    """
    Base class for all dataframe outputs.

    Sets up the source DataFrame and an appropriate output file.

    Concrete subclasses must implement a create_output() method.
    """

    filename: str

    timeout: luigi.IntParameter = luigi.IntParameter(default=3600)

    def requires(self) -> Union[luigi.Target, List[luigi.Target]]:
        """
        Return a single Target (or a list of Targets) where each Target
        contains either a single DataFrame or a list or dict of DataFrames
        """
        raise NotImplementedError

    def output(self) -> luigi.Target:
        # Note that format=luigi.format.Nop is required for binary files on Python3
        # see https://github.com/spotify/luigi/issues/1647
        return ExpiringLocalTarget(self.filename, format=luigi.format.Nop, timeout=self.timeout)

    def run(self):
        inputs = self.input()
        # Make sure we have a dict so we can iterate over items
        if isinstance(inputs, luigi.Target):
            inputs = {'Data': inputs}
        dfs = {}
        for key, target in inputs.items():
            contents = target.get()
            if isinstance(contents, pd.DataFrame):
                dfs[key] = contents
            elif isinstance(contents, dict):
                dfs.update(contents)
            elif isinstance(contents, list):
                for seq, df in enumerate(contents):
                    dfs['%s (%d)' % (key, seq)] = df

        assert all(isinstance(df, pd.DataFrame) for df in dfs.values())
        with self.output().open(mode='wb') as f:
            self.build_output(f, dfs)

    def build_output(self, f: BinaryIO, dfs: Dict[str, pd.DataFrame]):
        """
        Build the output from the provided dataframes and output it to the provided file
        """
        raise NotImplementedError


class DataFrameOutputToExcelTask(DataFrameOutputTask):

    def prepare_sheet(self, sheet_name, df):
        """
        Return a prepared DataFrame for saving to Excel, e.g. by applying styles
        """
        return df

    def build_output(self, f, dfs):
        # Use openpyxl so we can support DataFrame.style
        # see https://pandas.pydata.org/pandas-docs/stable/style.html#Export-to-Excel
        with pd.ExcelWriter(f, engine='openpyxl') as wb:
            for sheet_name, df in dfs.items():
                df = self.prepare_sheet(sheet_name, df)
                df.to_excel(wb, sheet_name=sheet_name)
