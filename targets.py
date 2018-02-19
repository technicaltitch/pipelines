try:
    import cPickle as pickle
except ImportError:
    import pickle
import contextlib
import datetime
import glob
import hashlib
import logging
import os
import random
import re
import shutil
import time
from copy import deepcopy
from inspect import getsource
from io import BytesIO
from threading import Condition

import dateutil.parser
import luigi
import requests
import rfc6266_parser as rfc6266
from ckanapi import RemoteCKAN
from ckanapi.errors import ValidationError

logger = logging.getLogger('luigi-interface')

config = luigi.configuration.get_config()


class ThreadSafeMemoryStore:
    _lock = Condition()
    _store: dict = {}

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

    def items(self):
        return self._store.items()


memory_cache = ThreadSafeMemoryStore()


# Patch atomic file so that the temporary file has the correct extension
# because DataFrame.to_excel requires it to infer the engine
def generate_tmp_path(self, path):
    basename, extension = os.path.splitext(path)
    return basename + '-luigi-tmp-%09d%s' % (random.randrange(0, 1e10), extension)


atomic_file = luigi.local_target.atomic_file
atomic_file.generate_tmp_path = generate_tmp_path


class LocalTarget(luigi.local_target.LocalTarget):
    """
    A better Luigi LocalTarget that reports the full path.
    """

    def __init__(self, path=None, format=None, is_tmp=False):
        """Normalize path as part of initialization"""
        if path:
            path = os.path.normpath(os.path.expanduser(path))
        super().__init__(path, format, is_tmp)

    def exists(self):
        """
        Returns ``True`` if the path for this FileSystemTarget exists; ``False`` otherwise.

        This method is implemented by using :py:attr:`fs`.
        """
        result = super().exists()
        if result:
            logger.debug("Found local file or directory %s", self.path)
        else:
            logger.warning("Cannot find local file or directory %s", self.path)
        return result


class ExpiringMemoryTarget(luigi.target.Target):
    """
    A Luigi Target that stores data in a thread-safe in-memory cache with per item expiry.

    This Target doesn't require objects to be serialized. Therefore it is a good choice
    for passing pure Python objects such as Pandas DataFrames between Tasks. It is the
    responsibility of the pipeline author to consider the memory implications of using
    the shared memory store.
    """

    def __init__(self, name: str = None, timeout: int = None, cache=None, task: luigi.Task = None) -> None:
        assert isinstance(name, str), 'name is the unique identifier the Target, and must be a string, not %r' % name
        assert isinstance(timeout, int) and timeout > 0, ('timeout is the number of seconds the Target is valid for '
                                                          'before being recalculated, and must be a positive integer, '
                                                          'not %r' % timeout)
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

    def prune(self):
        for key, value in self.cache.items():
            modified_time = value[0]
            if modified_time + self.timeout < time.time():
                logger.debug("MemoryTarget '%s' has expired" % self.key)
                del self.cache[key]

    def put(self, value):
        self.prune()
        self.cache[self.key] = (time.time(), value)

    def remove(self):
        del self.cache[self.key]

    def exists(self):
        self.prune()
        return self.key in self.cache


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


class RESTTarget(luigi.target.Target):
    """
    A Luigi Target that retrieves data from a REST API.
    """

    def __init__(self, path=None, params={}, server=None, auth=None):
        self.path = path
        self.params = params
        self.server = server
        self.auth = auth

    def exists(self):
        """
        Assume that data exists
        """
        return True

    def get(self):
        # Use requests to retrieve remote data so we can handle authentication
        auth = self.auth
        url = f'{self.server}/{self.path}'
        if url[:4] != 'http':
            # Default to https if the scheme wasn't specified in the server
            url = 'https://' + url
        logger.debug("Downloading remote data from  %s" % url)
        if isinstance(auth, (tuple, list)):
            response = requests.get(url, params=self.params,
                                    auth=tuple(auth))
        elif auth:
            response = requests.get(url, params=self.params,
                                    headers={'Authorization': auth})
        else:
            response = requests.get(url, params=self.params)
        logger.info(f'{response.request.url} {response.status_code} {response.reason}')
        response.raise_for_status()
        return BytesIO(response.content)


# TODO:
#   Upload:
#    * create resource, optionally also creating dataset/package
#    * update resource metadata and/or file - metadata patch or update
#    * delete resource? dataset/package?
#    * integrate with luigi Target by receiving / returning file path and metadata


class CachedCKAN:

    RESOURCE_STATUS_FILE = 'local_resource_status.pickle'
    METADATA_FILE = 'metadata_%s.pickle'  # % username

    def __init__(self, address, username, password, ckan_cache_dir, apikey=None, user_agent=None,
                 get_only=False, check_for_updates_every=None, cache_resources_for=None):
        # Can't use CAKN API to download resources - must use web UI, which doesn't authenticate with token
        self.username = username
        self.password = password
        self.check_for_updates_every = check_for_updates_every or 60 * 60  # seconds between updating resource metadata
        self.cache_resources_for = cache_resources_for or 60 * 60  # seconds to keep cached resources after last access
        self.ckan_cache_dir = os.path.abspath(ckan_cache_dir)
        self.api = RemoteCKAN(address, apikey=apikey, user_agent=user_agent, get_only=get_only, session=None)
        self.__metadata = None
        self.__local_resource_status = None
        self.__session = None
        # if user does not use CachedCKAN in a `with` context, we need to materialize metadata caches to disk on every update
        self.__in_context_block = False

    @classmethod
    def _pickle_to_file(cls, path, data):
        normpath = os.path.normpath(path)
        folder = os.path.dirname(normpath)
        with contextlib.suppress(OSError):  # TOCTTOU
            os.makedirs(folder)

        try:
            with contextlib.suppress(FileNotFoundError):
                os.remove(path)
            with open(path, 'wb') as file_ref:
                pickle.dump(data, file_ref)
        except IOError:
            logger.warning('Save failed: %s to %s', str(data), path, exc_info=1)
        else:
            logger.info('Saved state %s in %s', str(data), path)

    @classmethod
    def _unpickle_from_file(cls, path):
        if os.path.exists(path):
            logger.info('Attempting to load from %s', path)
            try:
                with open(path, 'rb') as file_ref:
                    return pickle.load(file_ref)
            except BaseException:
                logger.exception('Error when loading data from %s', path)
                return
        else:
            logger.info('No prior data file exists at %s', path)

    def get_ckan_metadata(self, force_download=False):
        """
        Downloads and caches the metadata of all resources the user has access to. Refreshes after self.timeout seconds.
        """
        # Simplify the metadata structure to insulate from CKAN API changes?
        # No - more explicit if done in accessor methods instead, e.g. `self.get_resource_metadata`
        if not self.__metadata and force_download is False:
            self.__metadata = self._unpickle_from_file(self.metadata_cache_filename)
        if not self.__metadata or \
                force_download or \
                (self.__metadata['metadata_last_updated'] + datetime.timedelta(seconds=self.check_for_updates_every) <
                 datetime.datetime.utcnow()):
            try:
                metadata = self.api.action.current_package_list_with_resources()
            except requests.exceptions.ConnectionError as e:
                error = \
                    'Unable to reach CKAN and no local copy of CKAN metadata found at %s' % self.metadata_cache_filename
                logging.exception(error)
                raise Exception('%s\n%s' % (error, str(e)))

            self.__metadata = {'metadata_last_updated': datetime.datetime.utcnow()}
            # more efficient to pickle.dump to file twice, to avoid unpickling expired data, but unnecessary here
            for dataset in metadata:
                for resource in dataset['resources']:
                    # Internal references are maintained, so pickle does not unpickle the nested dataset dicts multiple
                    # times, ie, after unpickling, `(meta['resource_a']['dataset'] is meta['resource_b']['dataset'])`
                    resource['dataset'] = dataset
                    self.__metadata[resource['id']] = resource
            if not self.__in_context_block:
                self.save_user_metadata()
        return self.__metadata

    @property
    def local_resource_status(self):
        if not self.__local_resource_status:
            self.__local_resource_status = self._unpickle_from_file(self.resource_status_cache_filename)
            if not self.__local_resource_status:
                self.__local_resource_status = dict()
        return self.__local_resource_status

    @property
    def server_domain(self):
        """
        Returns the domain of the server URL for use in filename cache keys. (`urlparse` behaves inconsistently)
        :return: If the server URL is http://data.example.com:8080/datasets?foo=bar this returns data_example_com
        """
        url = self.api.address
        domain_start = url.find('://') + 3 if url.find('://') >= 0 else 0
        domain_end = url.find(':', domain_start) if url.find(':', domain_start) >= 0 else \
            url.find('/', domain_start) if url.find('/', domain_start) >= 0 else \
            url.find('?', domain_start) if url.find('?', domain_start) >= 0 else \
            len(url)
        regex = re.compile('[^a-zA-Z0-9\.]')  # being cautious as changing this later will invalidate everyone's cache
        return regex.sub('_', url[domain_start:domain_end]).lower()

    def __enter__(self):
        """
        Context manager to cache session, CKAN metadata and resource status
        """
        self.__in_context_block = True
        # TODO: create local backup of file in case we can't upload and have to roll back
        return self

    def save_resource_statuses(self):
        if self.__local_resource_status:
            self._pickle_to_file(self.resource_status_cache_filename, self.__local_resource_status)

    def save_user_metadata(self):
        if self.__metadata:
            self._pickle_to_file(self.metadata_cache_filename, self.__metadata)

    def __exit__(self, type, value, traceback):
        self.save_resource_statuses()
        self.save_user_metadata()
        if self.__session:
            self.__session.close()
        self.api.close()
        # TODO: restore from local backup of file in case we can't upload and have to roll back

    @property
    def resource_status_cache_filename(self):
        return os.path.join(self.ckan_cache_dir, self.server_domain, self.RESOURCE_STATUS_FILE)

    @property
    def metadata_cache_filename(self):
        return os.path.join(self.ckan_cache_dir, self.server_domain, self.METADATA_FILE % self.username)

    @property
    def iso_utc_datetime(self):
        return datetime.datetime.utcnow().replace(microsecond=0).isoformat()  # .replace(':', '.')

    def get_resource_metadata(self, resource_id):
        return self.get_ckan_metadata()[resource_id]

    def get_resource_last_updated(self, resource_id):
        # CKAN doesn't correctly report resource last_modified: https://github.com/ckan/ckan/issues/3907
        metadata = self.get_resource_metadata(resource_id)
        date_candidates = [metadata['created'],
                           metadata['dataset']['metadata_created'],
                           metadata['dataset']['metadata_modified'],
                           metadata['last_modified']]
        date_candidates = filter(None.__ne__, date_candidates)
        return max(map(dateutil.parser.parse, date_candidates))

    def login(self):
        """
        Login to CKAN web UI.

        Returns a ``requests.Session`` instance with the CKAN
        session cookie.
        """
        self.__session = requests.Session()
        data = {'login': self.username, 'password': self.password}
        url = self.api.address + '/login_generic'
        r = self.__session.post(url, data=data)
        if 'field-login' in r.text:
            # Response still contains login form
            raise RuntimeError('Login failed.')

    def get_resource_cache_path(self, resource_id, create=False):
        path = os.path.abspath(os.path.join(self.ckan_cache_dir, self.server_domain, resource_id))
        if create:
            with contextlib.suppress(OSError):
                os.makedirs(path)
        return path

    def _update_resource_status(self, resource_id, last_accessed=None, cache_for=None):
        if resource_id not in self.local_resource_status:
            self.local_resource_status[resource_id] = dict()

        self.local_resource_status[resource_id]['last_accessed'] = last_accessed if last_accessed else \
            datetime.datetime.utcnow()

        if cache_for:
            cache_until = datetime.datetime.utcnow() + datetime.timedelta(minutes=cache_for)
            self.local_resource_status[resource_id]['cache_until'] = cache_until

        if not self.__in_context_block:
            self.save_resource_statuses()

    def _delete_cache(self, resource_id=None):
        if resource_id is None:
            logging.warning('CachedCKAN._delete_cache called with no resource_id - deleting WHOLE cache.')
        cache_path = self.get_resource_cache_path(resource_id) if resource_id else self.ckan_cache_dir
        with contextlib.suppress(FileNotFoundError):
            for cache_file in os.listdir(cache_path):
                if os.path.isdir(cache_file):
                    shutil.rmtree(cache_file)
                    logging.info('Deleting directory from cache %s', cache_file)
                else:
                    os.remove(cache_file)
                    logging.info('Deleting file from cache %s', cache_file)

    def _prune_cache(self):
        """
        Delete any cached resources that haven't been accessed for `self.cache_resources_for minutes`, or their custom
        expiry deadline. This is run each time a resource is downloaded.

        Nb. Cache pruning is done according to the *current* `self.cache_resources_for minutes` setting, rather than the
        setting when the resource was previously cached or accessed. Conversely, the custom expiry is saved, and so
        persists between CachedCKAN instantiations and calls until updated. It can be updated by passing
        `cache_for` (in minutes) whenever the resource is accessed.
        """
        default_expiry = datetime.datetime.utcnow() + datetime.timedelta(minutes=self.cache_resources_for)
        for resource_id, resource in self.local_resource_status.items():
            if 'cache_until' in resource:
                if datetime.datetime.utcnow() > resource['cache_until']:
                    self._delete_cache(resource_id)
            elif resource['last_accessed'] < default_expiry:
                self._delete_cache(resource_id)

    def get_resource(self, resource_id, cache_for=None):
        """
        Returns the path of a local copy of a resource, downloading into the cache if necessary.

        You can override the default cache duration, eg, you want a default of a year but you want a multi-GB file
        removed earlier.
        """
        metadata = self.get_resource_metadata(resource_id)
        last_updated = self.get_resource_last_updated(resource_id)

        if metadata and last_updated > self.local_resource_status.get(resource_id, {})\
                                                                 .get('last_downloaded', datetime.datetime(1900, 1, 1)):
            self._update_resource_status(resource_id, cache_for=cache_for)

            with contextlib.suppress(ConnectionError):
                path = self._download_resource(resource_id)

            if path:
                return path

        candidate_files = os.listdir(self.get_resource_cache_path(resource_id))

        if len(candidate_files) > 1:
            logging.exception('More than one file found for resource %s.', resource_id)
        elif len(candidate_files) == 1:
            self._update_resource_status(resource_id, cache_for=cache_for)
            return os.path.join(self.get_resource_cache_path(resource_id), candidate_files[0])
        else:
            logging.exception('No connection or local copy found of resource %s.', resource_id)

    def _download_resource(self, resource_id):
        """
        Download resource from web UI.

        Returns a local filename.
        """
        package_id = self.get_resource_metadata(resource_id)['package_id']

        url = '{ckan}/dataset/{pkg}/resource/{res}/download/'.format(
                ckan=self.api.address, pkg=package_id, res=resource_id)

        self.login()
        response = self.__session.get(url)

        cache_path = self.get_resource_cache_path(resource_id, True)

        if response.status_code == 200:
            self._delete_cache(resource_id)
            self._prune_cache()

            # `cgi.parse_header` doesn't handle non-ASCII filenames
            file_path = os.path.join(cache_path, rfc6266.parse_requests_response(response).filename_unsafe)

            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(512):  # chunk size in bytes
                    f.write(chunk)

            if resource_id not in self.local_resource_status:
                self.local_resource_status[resource_id] = dict()
            self.local_resource_status[resource_id]['last_downloaded'] = datetime.datetime.utcnow()

            if not self.__in_context_block:
                self.save_resource_statuses()
                self.__session.close()

            return file_path
        else:
            raise ConnectionError('Error %s occurred when downloading resource %s.', resource_id, response.status_code)

    def create_resource(self, **kwargs):
        """
        Example usage:
            with CachedCKAN('http://myckan.example.com', apikey='real-key', user_agent=ua, username='joe', password='pwd') as ckan:
                ckan.create_resource(
                    package_id='my-dataset-with-files',
                    upload=open('/path/to/file/to/upload.csv', 'rb')
                )

        See: http://docs.ckan.org/en/latest/api/#ckan.logic.action.create.resource_create
        """
        results = self.api.action.resource_create(**kwargs)
        # TODO: use `results` rather than re-download, using an isolation layer to standardize the re-structure
        self.get_ckan_metadata(True)
        if 'id' in results:
            self._import_resource_to_cache(kwargs['upload'], results['id'])
        return results

    def _import_resource_to_cache(self, upload, resource_id):
        self._delete_cache(resource_id)
        cache_dir = self.get_resource_cache_path(resource_id, True)
        if os.path.abspath(os.path.dirname(upload.name)) != cache_dir:
            shutil.copy2(upload.name, cache_dir)

    def create_package(self, **kwargs):
        """
        Example usage:
            with CachedCKAN('http://myckan.example.com', apikey='real-key', user_agent=ua, username='joe', password='pwd') as ckan:
                ckan.create_package(
                    name='joe_data',
                    title='Joe\'s data',
                    author='joe',
                    version='1.2a',
                    format='XLSX'
                )

        I don't think there's a way to upload resources as part of the same API call as creating the package through ckanapi.
        https://stackoverflow.com/questions/48054042/creating-a-ckan-package-dataset-with-resources-using-ckanapi-and-python
        Unless it proves to be a common requirement I think we defer - they may address this in the `ckanapi` package.

        See: http://docs.ckan.org/en/latest/api/#ckan.logic.action.create.package_create
        """
        results = self.api.action.package_create(**kwargs)
        self.get_ckan_metadata(True)
        # Should we find out how to upload resources as part of this call we'll need to update the local file cache when uploads done.
        return results

    def patch_package(self, **kwargs):
        """
        Example usage:
            with CachedCKAN('https://data.kimetrica.com', 'user', 'pwd', 'cache', apikey='123-abc') as ckan:
                ckan.patch_package(name='chris_test',
                                    title='Chris test data',
                                    author='arthur',
                                    version='1.3c',
                                    id='c86186f4-8368-4ecc-a907-08ca67d0e7ab'

        See: http://docs.ckan.org/en/latest/api/#ckan.logic.action.update.package_patch
        """
        results = self.api.action.package_patch(**kwargs)
        self.get_ckan_metadata(True)
        return results

    def patch_resource(self, **kwargs):
        """
        Example usage:
            with CachedCKAN('http://myckan.example.com', apikey='real-key', user_agent=ua, username='joe', password='pwd') as ckan:
                ckan.patch_resource(
                    name='an updated test resource upload',
                    id='e2cbc4be-2bb3-47c4-a64a-b61f82a8a0e2',
                    upload=open('/Users/technicaltitch/Downloads/country_cpis.csv', 'rb')
                )

        See: http://docs.ckan.org/en/latest/api/#ckan.logic.action.update.resource_patch
        """
        results = self.api.action.resource_patch(**kwargs)
        self.get_ckan_metadata(True)
        if 'upload' in kwargs:
            resource_id = results['id'] if 'id' in results else kwargs['id']
            self._import_resource_to_cache(kwargs['upload'], resource_id)
        return results

    def update_package(self, **kwargs):
        """
        WARNING: Using this method removes all existing data from the package. If you want to keep existing data, use
        `CachedCKAN.patch_package`.

        Example usage:
            with CachedCKAN('https://data.kimetrica.com', 'user', 'pwd', 'cache', apikey='123-abc') as ckan:
                ckan.update_package(name='chris_test',
                                    title='Chris test data',
                                    author='arthur',
                                    version='1.3c',
                                    id='c86186f4-8368-4ecc-a907-08ca67d0e7ab'

        See: http://docs.ckan.org/en/latest/api/#ckan.logic.action.update.package_update
        """
        logging.warning('Updating a package removes all data. If you wish to keep the existing data, use `CachedCKAN.patch_package`.')
        results = self.api.action.package_update(**kwargs)
        self.get_ckan_metadata(True)
        return results

    def update_resource(self, **kwargs):
        """
        Example usage:
            with CachedCKAN('https://data.kimetrica.com', 'user', 'pwd', 'cache', apikey='01234abc') as ckan:
                ckan.update_resource(
                    name='another updated resource',
                    id='fd01f45a-b2d4-4897-848c-2b6c16343a49',
                    upload=open('/Users/technicaltitch/Downloads/country_cpis.csv', 'rb')
                )

        See: http://docs.ckan.org/en/latest/api/#ckan.logic.action.update.resource_update
        """
        results = self.api.action.resource_update(**kwargs)
        self.get_ckan_metadata(True)
        if 'upload' in kwargs:
            resource_id = results['id'] if 'id' in results else kwargs['id']
            self._import_resource_to_cache(kwargs['upload'], resource_id)
        return results

    def delete_resource(self, resource_id):
        """
        Example usage:
            with CachedCKAN('https://data.kimetrica.com', 'user', 'pwd', 'cache', apikey='01234abc') as ckan:
                ckan.delete_resource(resource_id='fd01f45a-b2d4-4897-848c-2b6c16343a49')

        See: http://docs.ckan.org/en/latest/api/#ckan.logic.action.update.resource_delete
        """
        results = self.api.action.resource_delete(id=resource_id)
        self.get_ckan_metadata(True)
        self._delete_cache(resource_id=resource_id)
        return results

"""
CachedCKAN works - test code is commented below and will be removed.

I still need to:

 * Finish the file-like (`open('r/w')`) API to CKANTarget
 * Extend CachedCKAN to retrieve resources by package/dataset and resource names or titles - using local metadata if necessary
 * I suspect there are offline-related exceptions that need ignoring
 * Unfortunately the API calls datasets packages - I don't really want to get involved in mapping Target parameters to CKAN API parameters, so while analysts will see 'dataset' in the UI they will need to use 'package' in Target parameters.
 * Implement pseudocode in CKANTarget outlined below
 * On creating a resource, store the new resource_id in the Target class so it is picked up from cache by the next task
 * On entering CachedCKAN, grab a copy of the resource file. On exiting the CachedCKAN context manager, if the file has been deleted, attempt to delete online. If not, ensure that any updates to the file have been uploaded. If they haven't, overwrite with the prior version, possibly also copy the modified version somewhere, and raise an exception.
 * Validation of the CKANTarget.__init__ parameters
 * Unit tests

I plan to do the file-like API and store new resource_id in Target for the next task, (and the pseudocode below), so it is ready for use, submit that for your review, then look at what the MVP is from the above - which might mean scrapping the cache for now and requiring analysts to store locally - although I think we'll want the cache at some point and it works nicely.

#### Overview of `CachedCKAN`

The cache timeout is the frequency with which we should update the CKAN metadata. The metadata tells us whether a particular resource is out of date or not. Resources are updated on demand, if there is a connection and if the latest version of the metadata indicates our copy is out of date. Local copies of CKAN metadata and resource last downloaded times are kept pickled in the cache directory.

#### Pseudocode of `CKANTarget`

All but the name/title search is implemented and working in `CachedCKAN`.

```
if mode='w':
    if resource_id:
        call resource_patch
        call package_patch
    elif package_id:
        call package_patch
        if resource_file:
            call resource_create passing package_id
    elif resource_file or resource_metadata:
        call package_create
        call resource_create passing new package_id, and file if present
    elif package_metadata:
        call package_create
elif mode='r':
    if resource_id:
        call resource_get (retrieves metadata and downloads file if necessary otherwise opens local cached version)
    else:
        call CachedCKAN.search(package_name='', resource_name='', package_title='', resource_title='') - tries online API else searches local cache, caches and returns resource file
```
"""


class CKANTarget(luigi.Target):
    """
    A Luigi Target that stores and retrieves data from a (cached) CKAN Instance.
    """

    def __init__(self, address, auth=None, username=None, password=None, resource=None, package=None, timeout=None,
                 cache_dir=None, overwrite=False, **kwargs):
        """
        :param address: the URL for the CKAN server
        :param auth:
        :param username:
        :param password:
        :param resource:
            Example `resource` dict:
            {
                id='c86186f4-8368-4ecc-a907-08ca67d0e7ab',
                name='a_test_resource_upload',
                title='a test resource upload',
                package_id='c86186f4-8368-4ecc-a907-08ca67d0e7ab',
                upload=open('/Users/technicaltitch/Documents/Kimetrica/rm/rmluigi/rm/pipelines/cache/data.kimetrica.com/e2cbc4be-2bb3-47c4-a64a-b61f82a8a0e2/test-data.xlsx', 'rb')
            }
        :param package:
            Example `package` dict (a 'dataset' is called a 'package' when using the CKAN API):
            {
                name='chris_test',
                title='Chris test data',
                author='arthur',
                version='1.3c',
                id='c86186f4-8368-4ecc-a907-08ca67d0e7ab'
            }
        :param timeout: of CKAN metadata in seconds
        :param cache_dir: local directory in which CKAN cache is stored - should be shared by all users
        :param overwrite: has to be set to True to permit overwriting an existing resource
        :param kwargs: extra parameters for initializing the CKAN API
        """

        self.resource = resource or {}
        self.package = package or {'id': config['ckan.default_package_id']}
        self.ckan_kwargs = kwargs or {}
        self.overwrite = overwrite
        self.ckan_kwargs.update({'address': address or config['ckan.address'],
                                 'username': username or config['ckan.username'],
                                 'password': password or config['ckan.password'],
                                 'auth': auth or config['ckan.auth'],
                                 'cache_dir': cache_dir or config['ckan.cache_dir'],
                                 'timeout': timeout or config['ckan.timeout'] or config['core.timeout'], })

    @property
    def resource_id(self):
        # TODO: if 'id' not in self.resource or not self.resource['id']: self.resource['id'] = ckan.find_resource(self.resource)['id']
        return self.resource['id']

    @property
    def package_id(self):
        # TODO: if 'package_id' not in self.resource and 'id' not in self.package or (not self.resource['package_id'] and not self.package['id']:
        # self.package['id'] = ckan.find_package(self.package)['id'] or ckan.find_resource(self.resource)['package_id']
        return self.resource['package_id'] or self.package['id']

    def exists(self):
        # The purpose of this method is to inform the scheduler whether it needs to run the prior task to generate this
        # resource.
        # TODO: Offline mode should be able to run offline (so pass files from task to task), but not upload and get resource id, and expire files after a few seconds
        return bool(self.get())

    def remove(self):
        # delete a remote CKAN resource
        # Call CKAN  to delete original resource
        # http://docs.ckan.org/en/latest/api/#ckan.logic.action.delete.resource_delete
        with CachedCKAN(**self.ckan_kwargs) as ckan:
            status = ckan.delete_resource(resource_id=self.resource_id)

    def get(self):
        if self.resource_id:
            with CachedCKAN(**self.ckan_kwargs) as ckan:
                return ckan.get_resource(resource_id=self.resource_id)
        return None

    def put(self, file_path):
        resource_kwargs = self.resource
        if file_path:
            resource_kwargs['upload'] = open(file_path, 'rb')
        if self.package_id:
            resource_kwargs['package_id'] = self.package_id
        if self.resource_id:
            if self.overwrite:
                with CachedCKAN(**self.ckan_kwargs) as ckan:
                    status = ckan.patch_resource(**resource_kwargs)
            else:
                msg = ('Existing CKAN resource %s specified, but `overwrite=False`, so `CKANTarget.put` failed.'
                       'Change the resource `name`, or set `overwrite=True` to fix.' % self.resource.get('name', self.resource_id))
                logging.error(msg)
                raise Exception(msg)
        else:
            with CachedCKAN(**self.ckan_kwargs) as ckan:
                status = ckan.create_resource(**resource_kwargs)
        self.resource['id'] = status['id']
        self.package['package_id'] = status['package_id']
        # TODO: Create/patch package too? How indicate in CKANTarget API?


import pprint

ckan_target = dict()

import luigi
class CKANTarget(luigi.Target):
    def __init__(self, name, cache=None):
        print("Init name %s" % name)
        self.name = name
        self.cache = cache or ckan_target
    def exists(self):
        print("Checking CKANTarget %s .exists(), returning %s" % (self.name, self.name in self.cache))
        return self.name in self.cache
    def put(self, value):
        print("CKANTarget.putting value %s" % value)
        self.cache[self.name] = value
    def get(self):
        print("CKANTarget %s .getting value %s" % (self.name, self.cache[self.name]))
        return self.cache[self.name]

class One(luigi.Task):
    def output(self):
        print("Creating Target in One.output()")
        return CKANTarget(name='x')
    def run(self):
        print("Running One.run() - outputting 'y'")
        self.output().put('y')

class Two(luigi.Task):
    def requires(self):
        print("Two.requiring One()")
        return One()
    def run(self):
        print('Task Two getting result: %s' % self.input().get())
    def output(self):
        return None

luigi.build([Two()], workers=1, local_scheduler=True)


# with CachedCKAN('https://data.kimetrica.com', 'chrisp', 'wR$6*jf!', 'cache', apikey='fc660a1a-cf95-4057-ba0e-43bae1aa5eac') as ckan:
#     pprint.pprint(
#         ckan.patch_package(name='chris_test',
#                             title='Chris test data',
#                             author='arthur',
#                             version='1.3c',
#                             id='c86186f4-8368-4ecc-a907-08ca67d0e7ab'
#         )
#     )

# with CachedCKAN('https://data.kimetrica.com', 'chrisp', 'wR$6*jf!', 'cache', apikey='fc660a1a-cf95-4057-ba0e-43bae1aa5eac') as ckan:
#     pprint.pprint(
#         ckan.update_resource(
#             name='another updated resource',
#             id='fd01f45a-b2d4-4897-848c-2b6c16343a49',
#             upload=open('/Users/technicaltitch/Downloads/country_cpis.csv', 'rb')
#         )
#     )

# with CachedCKAN('https://data.kimetrica.com', 'chrisp', 'wR$6*jf!', 'cache', apikey='fc660a1a-cf95-4057-ba0e-43bae1aa5eac') as ckan:
#     ckan.create_package(name='joe_data',
#                         title='Joe data',
#                         author='joe',
#                         version='1.2a',
#                         format='XLSX',
#                         resources=('report.xls',
#                                    open('/Users/technicaltitch/Documents/Kimetrica/rm/rmluigi/rm/pipelines/cache/data.kimetrica.com/e2cbc4be-2bb3-47c4-a64a-b61f82a8a0e2/test-data.xlsx', 'rb'),
#                                    'application/vnd.ms-excel',
#                                    {'Expires': '0'}))

# with CachedCKAN('https://data.kimetrica.com', 'chrisp', 'wR$6*jf!', 'cache', apikey='fc660a1a-cf95-4057-ba0e-43bae1aa5eac') as ckan:
#     ckan.create_package(name='joe_data',
#                         title='Joe data',
#                         author='joe',
#                         version='1.2a',
#                         format='XLSX',
#                         other_test_data=open('/Users/technicaltitch/Documents/Kimetrica/rm/rmluigi/rm/pipelines/cache/data.kimetrica.com/e2cbc4be-2bb3-47c4-a64a-b61f82a8a0e2/test-data.xlsx', 'rb'))

# with CachedCKAN('https://data.kimetrica.com', 'chrisp', 'wR$6*jf!', 'cache', apikey='fc660a1a-cf95-4057-ba0e-43bae1aa5eac') as ckan:
#     print(ckan.create_resource(
#             name='a test resource upload',
#             package_id='c86186f4-8368-4ecc-a907-08ca67d0e7ab',
#             upload=open('/Users/technicaltitch/Documents/Kimetrica/rm/rmluigi/rm/pipelines/cache/data.kimetrica.com/e2cbc4be-2bb3-47c4-a64a-b61f82a8a0e2/test-data.xlsx', 'rb')))

# with CachedCKAN('https://data.kimetrica.com', 'chrisp', 'wR$6*jf!', 'cache', apikey='fc660a1a-cf95-4057-ba0e-43bae1aa5eac') as ckan:
#     print(ckan.get_resource('e2cbc4be-2bb3-47c4-a64a-b61f82a8a0e2'))
#     print(ckan.get_resource_metadata('e2cbc4be-2bb3-47c4-a64a-b61f82a8a0e2'))


