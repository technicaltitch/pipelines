import logging
import os
from typing import BinaryIO, Dict, List, Union

import geopandas as gpd
import luigi
import pandas as pd
from pipelines.targets import ExpiringLocalTarget, ExpiringMemoryTarget
from xlrd import open_workbook

logger = logging.getLogger('luigi-interface')


class Task(luigi.Task):
    """
    A Pipeline Task
    """


class ReadDataFrameTask(Task):
    """
    A Pipeline Task that reads one or more Pandas DataFrames from file(s) and returns them as a MemoryTarget
    """
    extensions = {
        '.xls': 'read_excel',
        '.xlsx': 'read_excel',
        '.csv': 'read_csv',
        '.h5': 'read_hdf'
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

    def read_dataframe(self, file_obj):
        with file_obj.open('r') as f:
            filename = f.name
            read_method = getattr(pd, self.get_read_method(filename))

            if read_method == 'read_excel':
                wb = open_workbook(filename, logfile=self.logger)
                logger.debug('Workbook %s contains sheets %s' % (filename, wb.sheet_names()))

            df = read_method(filename, **self.get_read_args())
            return df

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
