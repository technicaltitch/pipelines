import logging
import os
import string
import typing

import luigi
import pandas as pd
from pipelines.targets import ExpiringLocalTarget, ExpiringMemoryTarget
from xlrd import open_workbook
import xlsxwriter

logger = logging.getLogger('luigi-interface')


class Task(luigi.Task):
    """
    A Pipeline Task
    """


class ReadDataFrameTask(Task):
    """
    A Pipeline Task that reads a Pandas DataFrame from a file and returns it as a MemoryTarget
    """
    extensions = {
        '.xls': 'read_excel',
        '.xlsx': 'read_excel',
        '.csv': 'read_csv',
        '.h5': 'read_hdf'
    }

    timeout = luigi.IntParameter(default=120)
    read_method = luigi.Parameter(default='')
    read_args = luigi.DictParameter(default='{}')

    def requires(self):
        return NotImplementedError('Concrete subclasses must specify a Target returning a file readable by Pandas')

    def output(self):
        return ExpiringMemoryTarget(self.input().path, timeout=self.timeout)

    def get_read_method(self, filename):
        if self.read_method:
            return self.read_method
        extension = os.path.splitext(filename)[1]
        if extension in self.extensions:
            return self.extensions[extension]
        raise ValueError('Cannot determine read method for file %s' % filename)

    def get_read_args(self):
        return self.read_args

    def run(self):
        with self.input().open('r') as f:
            filename = f.name
            read_method = getattr(pd, self.get_read_method(filename))

            if read_method == 'read_excel':
                wb = open_workbook(filename, logfile=self.logger)
                logger.debug('Workbook %s contains sheets %s' % (filename, wb.sheet_names()))

            df = read_method(filename, **self.get_read_args())
            self.output().put(df)


class DataFrameOutputTask(Task):
    """
    Base class for all dataframe outputs.

    Sets up the source DataFrame and an appropriate output file.

    Concrete subclasses must implement a create_output() method.
    """

    filename = None

    timeout = luigi.IntParameter(default='3600')

    def requires(self):
        """
        DataFrame Outputs must require a ReadDataFrameTask
        """
        raise NotImplementedError

    def output(self):
        return ExpiringLocalTarget(self.filename, timeout=self.timeout)

    def run(self):
        df = self.input()
        assert isinstance(df, pd.DataFrame)
        with self.output().open(mode='w') as f:
            self.build_output(f, df)

    def build_output(self, f: typing.BinaryIO, df):
        """
        Build the output from the provided dataframe and output it to the provided filename
        """
        raise NotImplementedError


class DataFrameOutputToExcelTask(DataFrameOutputTask):

    first_column_label = luigi.BoolParameter(default='true')
    first_row_header = luigi.BoolParameter(default='true')
    format_numbers = luigi.BoolParameter(default='true')

    def add_formats(self, book: xlsxwriter.Workbook):
        """
        Replace the dict of formats with actual formats on the open Workbook
        """

        # Standard formats available to all workbooks
        formats = {
            'label': {'align': 'vcenter'},
            'percent': {'num_format': '0%'},
            'num': {'num_format': '0'},
            'header': {'align': 'vcenter', 'bold': True, 'font_color': 'white', 'bg_color': 'red'},
            'bold': {'bold': True},
        }

        # Add custom formats
        formats.update(self.formats)

        # Replace self.formats with usable formats on the open workbook
        for name, frmt in formats.items():
            self.formats[name] = book.add_format(frmt)

    def run(self):
        with self.output().open(mode='w') as f:
            # Convert a single DataFrame to an dict
            inputs = self.input()
            if isinstance(inputs, pd.DataFrame):
                inputs = {'Data': inputs}

            # Create the ExcelWriter and add the formats to it
            wb = pd.ExcelWriter(f, engine='xlsxwriter')
            self.add_formats(wb.book)

            # Build the workbook
            self.build_output(wb, inputs)

            # Save and quite
            wb.save()

    def build_output(self, wb, inputs):
        for sheet_name, df in inputs.items():
            df.to_excel(wb, sheet_name=sheet_name)
            worksheet = wb.sheets[sheet_name]
            self.format_worksheet(worksheet, df)

    def format_worksheet(self, worksheet, df):
        """Format worksheet cells"""
        formatted_ranges = self.formatted_ranges

        if self.first_column_label:
            formatted_ranges['A:A'] = self.formats['label']

        if self.first_row_header:
            formatted_ranges['1:1'] = self.formats['header']

        if self.format_numbers:
            end = string.ascii_uppercase[df.shape[1] + 2]
            worksheet.conditional_format('A1:' + str(end) + '1',
                                         {'type': 'cell',
                                          'criteria': '>',
                                          'value': 1,
                                          'format': self.formats['num']})

        # Set Columns Widths
        df_temp = df
        # Loop through all columns
        for idx, col in enumerate(df_temp):
            series = df_temp[col]
            # find length of largest item
            max_len = max((
                series.astype(str).map(len).max(),
                len(str(series.name))
            ))
            worksheet.set_column(idx, idx, max_len)
