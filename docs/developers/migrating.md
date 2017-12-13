# Migrating Existing Pipelines

Existing workflows can be migrated step by step without large initial changes. Consider the following existing workflow:

*Run the Remote Monitoring Monthly Analysis*

1. Sync the required raw data
2. Open a Python Shell (or Jupyter Notebook, etc.)
3. `python remote_monitoring/monthly/monthly_cleaning.py`

We can iteratively convert this pipeline to Luigi by following this blueprint.

**Check that the existing pipeline** runs correctly with the current code, in order to ensure that we don't waste effort trying to fix an error in Luigi that was actually already present.

**Convert to a minimal `Task`** by renaming the script to `tasks.py` (or for pipelines with many different script files by creating a file in a `tasks/` directory) and wrapping the existing code:

```python
import logging

from pipelines import Task

logger = logging.getLogger('luigi-interface')


class MainTask(Task):

    def run(self):
        # Existing code goes here
        # ...
```

**Test and debug the minimal `Task`** by running the `Task` in an interactive debugger. Put a breakpoint (`from IPython.core.debugger import set_trace; set_trace()`) in the file, and then run the `Task` in a Python shell:

```python
%load_ext autoreload
%autoreload 2
import luigi
from remote_monitoring.monthly.tasks import MainTask

luigi.build([MainTask()], local_scheduler=True)
```

As an alternative to using `Task.run()` from within a Python shell, you can also using the `luigi` command line program:

```
luigi --module remote_monitoring.monthly.tasks MainTask --local-scheduler
```

**Create a `requires()` method** and move any files or other input data that the Task requires into it:

```python
from luigi import ExternalTask, LocalTarget

class SurveyMonthlyHistoricalExternalTask(ExternalTask):
    """
    External Tasks document input data provided externally.
    """

    def output(self):
        """
        Outputs provide data to subsequent Tasks in the pipelinesthat can use them as inputs
        """
        return LocalTarget(
            os.path.join(raw_data_path, 'Monthly Historical Data.xlsx'))


class MainTask(Task):
    """
    A regular Task does work on inputs and produces outputs
    """

    def requires(self):
        """
        Requires specifies the upstream Tasks that this Task depends on
        """
        return {
            'xls_historical': SurveyMonthlyHistoricalExternalTask(),
        }

    def run(self):
        # Existing code goes here, self.input() accesses the outputs
        # from the required upstream Tasks
        # ...
        xls_historical = self.input()['xls_historical'].path
        df = pd.read_excel(xls_historical)
```

* **Create an `outputs()` method** that lists the files that the `Task` produces and convert existing output code to use `self.outputs()`:

```python
    def output(self):
        return ExpiringLocalTarget(
            os.path.join(output_path, 'Monthly Historical Data.h5'),
            # Pass a timeout (in secornds) to force data to recalculate automatically
            # when the timeout exires
            timeout=1800,
            # Pass the Task as well to recalculate if the Task source code
            # changes. This appends a hash to the end of the filename.
            task=self)

    def run(self):
        # Existing code goes here
        # ...
        # Use a context manager to ensure that the Target write is atomic
        with self.output().open(mode='w') as hdf:
            dfhist.to_hdf(hdf.name, 'df')
```

**Break the Task down into multiple `Tasks`** such that a separate `Task` is used to produce intermediate results that are used by multiple following output tasks. Ideally, a `Task` doing meaningful work should only produce one main output and you should use a wrapper task to collate all the individual outputs. For example, where there are multiple outputs produced from a single dataframe then the dataframe should be built by a separate `Task`, which is required by all the individual output `Tasks`. This approach makes it easier to add or change outputs later by splitting up the code into discrete chunks with a clear purpose:

```python
class SurveyMonthlyBuildDataFrameTask(Task):
    """
    Produce the main dataframe required for the output graphs
    """

    def requires(self):
        return SurveyMonthlyR7ExternalTask()

    def output(self):
        return ExpiringLocalTarget(
            os.path.join(output_path, 'Monthly Historical Data.h5'),
            timeout=3600,
            # Pass this Task into the Target so it can detect source code changes
            task=self))        

class FacilityDisruptionExcel(Task):
    """
    Save Facility Disruption table to Excel workbook
    """

    def requires(self):
        return SurveyMonthlyBuildDataFrameTask()

    def output(self):
        return ExpiringLocalTarget(
            os.path.join(output_path, 'Historical facility disruption table.xlsx'))

class FacilityDisruptionGraph(Task):
    """
    Build Facility Disruption graph and save to pdf
    """

    def requires(self):
        return SurveyMonthlyBuildDataFrameTask()

    def output(self):
        return ExpiringLocalTarget(
            os.path.join(output_path, 'Facility Disruption.pdf'))

class AllReports(WrapperTask):
    """
    Output all Monthly reports
    """

    def requires(self):
        yield FacilityDisruptionExcel()
        yield FacilityDisruptionGraph()

        yield IncidentsExcel()
        yield IncidentGraph(1)
        yield IncidentGraph(2)
        yield IncidentGraph(3)
        # etc.
```

Note that we can use decorators and subclasses to reduce the boilerplate code here, which is shown in the more verbose form in order to better explain the structure of the `Task` relationships.

**Make the code PEP8 compliant** in order to comply with the programming standards. For large files where coding standards have not been enforced until now this can be a time-consuming exercise and so it is worth trying `autopep8` (you will need to `pip install autopep8` as it is not in the requirements) as a starting point:

```
autopep8 --recursive --max-line-length=119 --in-place remote_monitoring/monthly/tasks/
```

Our preferred way to test compliance is with `flake8`:

```
flake8 remote_monitoring/monthly/tasks/
```
