from copy import deepcopy
from io import BytesIO
import glob
import hashlib
from inspect import getsource
import os
import logging
import random
import re
from threading import Condition
import time

import luigi
import requests

logger = logging.getLogger('luigi-interface')

config = luigi.configuration.get_config()


class ThreadSafeMemoryStore:
    _lock = Condition()
    _store = {}

    def __contains__(self, key):
        return key in self._store

    def __setitem__(self, key, value):
        with self._lock:
            self._store[key] = value

    def __getitem__(self, key):
        return deepcopy(self._store[key])

    def __delitem__(self, key):
        with self._lock:
            del self._store[key]


memory_cache = ThreadSafeMemoryStore()


# Patch atomic file so that the temporary file has the correct extension
# because DataFrame.to_excel requires it to infer the engine
def generate_tmp_path(self, path):
    basename, extension = os.path.splitext(path)
    return basename + '-luigi-tmp-%09d%s' % (random.randrange(0, 1e10), extension)


atomic_file = luigi.local_target.atomic_file
atomic_file.generate_tmp_path = generate_tmp_path


class ExpiringMemoryTarget(luigi.target.Target):
    """
    A Luigi Target that stores data in a thread-safe in-memory cache with per item expiry.

    This Target doesn't require objects to be serialized. Therefore it is a good choice
    for passing pure Python objects such as Pandas DataFrames between Tasks. It is the
    responsibility of the pipeline author to consider the memory implications of using
    the shared memory store.
    """

    def __init__(self, name, cache=None, timeout=None, task=None):
        self.name = name
        self.task = task
        self.timeout = timeout
        self.cache = cache or memory_cache

    @property
    def key(self):
        """
        Create a unique cache key for the Target.

        We include the Task source code so that code changes in the Task automatically
        cause a cache miss
        """
        key = self.name
        if self.task:
            key += getsource(self.task.__class__)
        return hashlib.md5(key.encode()).hexdigest()

    def get(self):
        return self.cache[self.key][1]

    def put(self, value):
        self.cache[self.key] = (time.time(), value)

    def remove(self):
        del self.cache[self.key]

    def exists(self):
        exists = self.key in self.cache
        if exists:
            modified_time = self.cache[self.key][0]
            if modified_time + self.timeout < time.time():
                logger.debug("MemoryTarget '%s' has expired" % self.key)
                exists = False
                self.remove()
        return exists


class ExpiringLocalTarget(luigi.local_target.LocalTarget):
    """
    A LocalTarget that automatically expires.

    A timeout in seconds can be provided by the calling Task. In addition, changes to the source code
    of the calling Task will force the Task to be run again and a new Target produced.
    """

    def __init__(self, path=None, format=None, is_tmp=False, timeout=None, task=None):  # NOQA
        self.timeout = (timeout or
                        config.get(task.get_task_family() if task else 'cache', 'default_timeout', 60 * 60 * 24))

        self.original_path = path
        self.task_hash = (hashlib.md5(getsource(task.__class__).encode()).hexdigest()
                          if task
                          else '')

        # Add a hash of the Task source to the path so that changes in the code force
        # the Task to run again, making sure we retain the correct extension
        if self.task_hash:
            basename, extension = os.path.splitext(path)
            path = ''.join((basename, '_', self.task_hash, extension))

        super().__init__(path, format, is_tmp)

    def open(self, mode='r'):
        rwmode = mode.replace('b', '').replace('t', '')
        if rwmode == 'w':
            # We are writing a new file, so remove files for this path but a different code version
            basename, extension = os.path.splitext(self.original_path)
            for f in glob.glob(basename + '_[0-9a-f]*' + extension):
                if re.match(basename + '_[a-f0-9]{32}' + extension, f) and f != self.path:
                    logger.debug("ExpiringLocalTarget '%s' is obsolete and has been removed" % f)
                    os.remove(f)
        return super().open(mode)

    def exists(self):
        exists = super().exists()
        if exists:
            mtime = os.path.getmtime(self.path)
            if mtime + self.timeout < time.time():
                logger.debug("ExpiringLocalTarget '%s' has expired" % self.path)
                exists = False
                self.remove()
        return exists


class CKANTarget(luigi.target.Target):
    """
    A Luigi Target that stores data in a CKAN Instance.
    """

    def __init__(self, resource=None, dataset=None, server=None, auth=None, cache=None, timeout=None, task=None):
        self.resource = resource
        self.dataset = dataset
        self.server = server or config['ckan.server']
        self.auth = auth or config['ckan.auth']
        self.task = task
        self.timeout = timeout or config['ckan.timeout'] or config['core.timeout']

    @property
    def url(self):
        pass
        # return resource_url

    @property
    def key(self):
        """
        Create a unique cache key for the Target.

        We include the Task source code so that code changes in the Task automatically
        cause a cache miss
        """
        key = self.resource
        if self.task:
            key += getsource(self.task.__class__)
        return hashlib.md5(key.encode()).hexdigest()

    def get(self):
        # Use requests to retrieve remote data so we can handle authentication
        auth = self.source_auth
        url = self.source_url
        if isinstance(auth, (tuple, list)):
            response = requests.get(url, params=self.source_parameters,
                                    auth=tuple(auth))
        elif auth:
            response = requests.get(url, params=self.source_parameters,
                                    headers={'Authorization': auth})
        else:
            response = requests.get(url, params=self.source_parameters)
        response.raise_for_status()
        return BytesIO(response.content)
        # return self.cache[self.key][1]

    def put(self, value):
        # if updating an existing resource, call:
        # http --json POST http://demo.ckan.org/api/3/action/resource_update id=<resource id> upload=@updated_file.csv Authorization:<api key>  # NOQA
        self.cache[self.key] = (time.time(), value)

    def remove(self):
        # delete a remote CKAN resource
        del self.cache[self.key]

    def exists(self):
        exists = self.key in self.cache
        if exists:
            modified_time = self.cache[self.key][0]
            if modified_time + self.timeout < time.time():
                logger.debug("MemoryTarget '%s' has expired" % self.key)
                exists = False
                self.remove()
        return exists
