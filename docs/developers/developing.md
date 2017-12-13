# Development Tips & Tricks

* If you are using the *Jupyter* shell, use autoreloading while developing code:

```
jupyter console
```

Then:

```
%load_ext autoreload
%autoreload 2
import datetime
import luigi

# Set configuration, including for parent tasks
config = luigi.configuration.get_config()
config.set('core', 'local_scheduler', 'True')
config.set('SurveyMonthlyR7Task', 'date', '2017-07-05')

# Import the Task
from  remote_monitoring.monthly.tasks.output import FacilityDisruptionGraph

# And run it
luigi.build([FacilityDisruptionGraph(area=4)])

```
