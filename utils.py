"""
Various utlity functions to help with Data Pipeline development
"""
from contextlib import contextmanager
import datetime
from functools import wraps
import importlib
import inspect
import logging
import time

import luigi
from luigi.parameter import _no_value
from luigi.task_register import Register

logger = logging.getLogger('luigi-interface')


def timeit(method):

    @wraps(method)
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print('%r  %2.2f ms' % (method.__name__, (te - ts) * 1000))
        return result
    return timed


@contextmanager
def temporary_config():
    """
    Create a temporary Luigi Config
    """
    config = luigi.configuration.get_config()
    original_config = config.defaults()

    # Yield the completed config
    yield config

    # Remove any config we set up
    config.reload()
    for k, v in original_config.items():
        config[k] = v


@contextmanager
def complete_config():
    """
    Create a temporary Luigi Config that has defaults for all variables
    """
    def get_default(param_obj):
        if isinstance(param_obj, luigi.IntParameter):
            return '1'
        elif isinstance(param_obj, luigi.DateParameter):
            return datetime.date.today().isoformat()
        elif isinstance(param_obj, luigi.Parameter):
            return ''

    config = luigi.configuration.get_config()

    # Make sure every parameter has a default value
    sections_to_remove = []
    options_to_remove = []
    for task_name, is_without_section, param_name, param_obj in Register.get_all_params():
        if param_obj._default == _no_value:
            if is_without_section:
                sections_to_remove.append(task_name)
            options_to_remove.append((task_name, param_name))
            config.set(task_name, param_name, get_default(param_obj))

    # Yield the completed config
    yield

    # Remove any config we set up
    for section, option in options_to_remove:
        config.remove_option(section, option)
    for section in sections_to_remove:
        config.remove_section(section)


def print_dag(task, level=0):
    """
    Print the DAG for a Task
    """
    def print_task(task, level):
        padding = ' ' * 8 * level
        print(padding + str(task))
        params = task.get_params()
        values = task.get_param_values(params, [], {})
        if params:
            print(padding + '    Parameters:')

        for name, value in values:
            print(padding + '        ' + name + ': ' + (str(value) or "''"))

        deps = task.requires()
        if deps:
            print(padding + '    Requires:')
        if isinstance(deps, luigi.Task):
            print_dag(deps, level + 1)
        elif isinstance(deps, dict):
            for task in deps.values():
                print_dag(task, level + 1)
        elif isinstance(deps, (list, tuple)):
            for task in deps:
                print_dag(task, level + 1)
        else:
            # Remaining case: assume struct is iterable...
            try:
                for task in deps:
                    print_dag(task, level + 1)
            except TypeError:
                raise Exception('Cannot determine dependencies for %s' % str(deps))

    if level == 0:
        with complete_config():
            if inspect.isclass(task):
                task = task()
            print_task(task, level)
    else:
        print_task(task, level)


def remove_outputs(task, cascade=False):
    """Remove the outputs from a Task, and possibly its parents."""

    # Don't remove outputs from External Tasks
    if not isinstance(task, luigi.ExternalTask):

        output = task.output()
        if isinstance(output, luigi.Target):
            if output.exists():
                logger.info('Removing Target %s from Task %s' % (str(output), str(task)))
                output.remove()
        elif isinstance(output, dict):
            for target in output.values():
                if target.exists():
                    logger.info('Removing Target %s from Task %s' % (str(target), str(task)))
                    target.remove()
        elif isinstance(output, (list, tuple)):
            for target in output:
                if target.exists():
                    logger.info('Removing Target %s from Task %s' % (str(target), str(task)))
                    target.remove()
        else:
            # Remaining case: assume struct is iterable...
            try:
                for target in output:
                    if target.exists():
                        target.remove()
            except TypeError:
                raise Exception('Cannot determine target for %s' % str(output))

        # Remove any upstream dependencies too
        if cascade:
            deps = task.requires()
            if isinstance(deps, luigi.Task):
                remove_outputs(deps, cascade)
            elif isinstance(deps, dict):
                for task in deps.values():
                    remove_outputs(task, cascade)
            elif isinstance(deps, (list, tuple)):
                for task in deps:
                    remove_outputs(task, cascade)
            else:
                # Remaining case: assume struct is iterable...
                try:
                    for task in deps:
                        remove_outputs(task, cascade)
                except TypeError:
                    raise Exception('Cannot determine dependencies for %s' % str(deps))


def run(task, **kwargs):
    """Run a Task using a flexible interface"""
    with temporary_config() as config:
        config.update(kwargs)
        # If Task is a sting then replace it with the Class definition
        if isinstance(task, str):
            module, cls = task.rsplit('.', 1)
            task = getattr(importlib.import_module(module), cls)
        # If Task is a class then instantiate it
        if inspect.isclass(task):
            task = task()
        assert isinstance(task, luigi.Task)
        luigi.build([task])
        return task.output()
