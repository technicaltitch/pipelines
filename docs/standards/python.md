# Python Programming Standards

## Dependencies

Any Operating System dependencies should be listed in comments at the start of the appropriate requirements file, e.g. `requirements/base.pip`.

Any Python libraries required must be listed in `requirements/base.pip` with a full version specification, e.g. `pandas==0.20.3`.

When a new dependency is identified it should be installed in a `virtualenv` working copy at the latest stable version using `pip`:

```
pip install arrow
```

Once the new dependency has been tested the specific version used should be added to the requirements, probably to either `requirements/base.pip`:

```
pip freeze | grep arrow
```

## Python Coding

* <https://docs.djangoproject.com/en/dev/internals/contributing/writing-code/coding-style/>
* <http://www.python.org/dev/peps/pep-0008/>
* <http://python.net/~goodger/projects/pycon/2007/idiomatic/handout.html>

### Linting ###

Kimetrica is using [flake8](http://flake8.pycqa.org/en/latest/) for static code analysis, which combines a number of [linting](https://en.wikipedia.org/wiki/Lint_(software)) tools to ensure style consistency. This improves readability but can also prevent certain errors. No code should be committed to Kimetrica git repositories that fails the *flake8* checks. To ensure this, add a `pre-commit` hook to git, by adding a file called `pre-commit` into the `.git/hooks` directory with the following content:

    #!/bin/sh
    flake8 .

This will prevent committing files that are not *flake8* compatible. This hook needs access to the *flake8* script, which should be installed from the `requirements/test.pip` file.

Flake8 checks are also enforced in the Gitlab repository server.

### Imports ###

* Never use `from x import *` because it makes it very difficult for new developers to understand where a function or class is imported from.
* Use relative imports (e.g. `from ..config.question_mapping import x`) to import functions or classes from the same package because this helps make the packages relocatable.
* As per PEP8, imports should be grouped in the following order, in alphabetical order within each section, with a blank line between sections:

* standard library imports (e.g. os, re, collections)
* related third party imports (e.g. Luigi, Pandas, Numpy, Matplotlib)
* local library specific imports (`from pipelines.targets import DataFrameTarget`)
* relative local package imports (`from ..config.question_mapping import x`)

### Line Length ###

The maximum line length is 119 characters, and the maximum length of a
a comment or docstring is 79 characters. Use the following approaches to manage long lines:

* For imports use parentheses as described in [PEP-328](https://www.python.org/dev/peps/pep-0328/)

    ```python
    from .models import (
        Market, MarketProduct, ExchangeRateValue, PriceDataSet, MarketPrice,
        MarketPriceFacts, PriceIndexValue)
    ````

* For chained Pandas methods we also use parentheses for implied line continuation - see [StackOverflow](http://stackoverflow.com/a/34335578/270310):

    ```python
    df = (df
          .drop(df[df[unique_id].isin(all_sec_drop_map[sec])].index)
          .reset_index())
    ```

### Single vs Double Quotes ###

PEP-8 does not specify a standard for when to use single or double quotes, although PEP-257 specifies triple double quotes for docstrings ("""). It is important that we have a standard internally, and so it is:

* Double quotes for text that contains substitution variables or will be printed on the screen
* Single quotes for anything that behaves like an identifier
* Double quoted raw string literals for regexps, e.g. `re.search(r"(?i)(info|warn|error)!", message)`
* Triple double quotes (""") for docstrings

### Date and Times ###

* To avoid confusion in code always use `import datetime` and then `datetime.date()`, `datetime.datetime()`, `datetime.time()` and `datetime.timedelta()` as required
* To calculate offsets from a date, use `dateutil.relativedelta`:

    ```python
from dateutil.relativedelta import relativedelta
last_day_of_month = some_date + relativedelta(day=31)
    ```

* To create a new date that is the last day of a month, you can use `calendar` instead:

    ```python
import calendar
import datetime
last_day_of_month = datetime.date(year, month, calendar.monthrange(year, month)[1])
    ```

* To convert `datetime.date` or other non-timestamps into aware timestamps, use [arrow](https://arrow.readthedocs.org/en/latest/):

    ```python
import datetime
import arrow
period_date = datetime.date(2014, 5, 31)
period_timestamp = arrow.get(period_date).datetime
    ```

* If you need to create an aware timestamp:

```python
from datetime import datetime

start = datetime.now(datetime.timezone.utc)
```
