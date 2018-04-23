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
        to_remove = []
        for key, value in self.cache.items():
            modified_time = value[0]
            if modified_time + self.timeout < time.time():
                logger.debug("MemoryTarget '%s' has expired" % self.key)
                to_remove.append(key)
        for key in to_remove:
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


class CachedCKAN:
    """
    Provides access to the CKAN API, caching metadata and accessed resources locally to speed up local
    operations, particularly for stateless consumers such as Luigi tasks.

    The cache timeout is the frequency with which we should update the CKAN metadata. The metadata tells us
    whether a particular resource is out of date or not. Resources are updated on demand, if there is a
    connection and if the latest version of the metadata indicates our copy is out of date. Local copies
    of CKAN metadata and resource last downloaded times are kept pickled in the cache directory.
    """

    RESOURCE_STATUS_FILE = 'cached_ckan_local_resource_status.pickle'
    METADATA_FILE = 'cached_ckan_metadata_%s.pickle'  # % username

    def __init__(self, address, username, password, cache_dir, user_agent=None,
                 get_only=False, check_for_updates_every=None, cache_resources_for=None):
        """
        :param address: URL of CKAN instance including protocol and not including trailing /
        :param username: Username for CKAN authentication, required as the API does not permit
        downloading of resources.
        :param password: Password for above username for CKAN instance.
        :param cache_dir: Absolute or relative directory within which cached copies of resources
        are stored. Can be used for more than one user and CKAN instance.
        :param user_agent: Optional override for user agent sent to API.
        :param get_only: Optionally only make GET requests. Limits functionality.
        :param check_for_updates_every: Number of seconds after which the local copy of the CKAN
        resource metadata should be re-downloaded. Last updated data in the metadata determines
        whether resources are refreshed.
        :param cache_resources_for: Number of seconds to keep locally cached resources after last
        access.
        """
        try:
            r = requests.head(address, timeout=30)
            r.raise_for_status()
        except requests.exceptions.RequestException as e:
            error = 'Unable to reach CKAN server. CkanTarget requires a working connection to the server.'
            logging.error(error)
            raise RuntimeError('%s\n%s' % (error, str(e)))

        # Can't use CKAN API to download resources - must use web UI, which doesn't authenticate with token.
        # API key then downloaded using a session authenticated with username and password.
        self.username = username
        self.password = password
        self.check_for_updates_every = check_for_updates_every or 60 * 60  # seconds
        self.cache_resources_for = cache_resources_for or 60 * 60  # seconds
        self.cache_dir = os.path.abspath(cache_dir)
        self.address = address
        self.login()
        self.api = RemoteCKAN(address, user_agent=user_agent, get_only=get_only, session=self._session)
        self._get_api_key()
        self._metadata = None
        self._metadata_last_updated = None
        self._local_resource_status = None
        self._session = None
        # If CachedCKAN not used as a `with`-scoped context manager, we need to materialize state,
        # eg, metadata cache, to disk on every update.
        self._in_context_block = False

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
        # If you need to see the metadata, add: else: logger.info('Saved state %s in %s', str(data), path)

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

    def _get_api_key(self):
        """
        Retrieves the user's API key from the user record and stores in the API instance. This is done using a
        session authenticated with username and password, to avoid the user having to specify all three.
        :return: None
        """
        self.api.apikey = self.api.action.user_show(id=self.username)['apikey']

    def get_ckan_metadata(self, force_download=False):
        """
        Downloads and caches the metadata of all resources the user has access to.
        Refreshes from server every `self.check_for_updates_every` seconds.

        :param force_download: Forces a download even if `self.check_for_updates_every` seconds have not passed. Used
        if we know a metadata change has taken place, eg, after we've created, updated or deleted a Resource. At some
        point we may make these changes locally rather than download so CachedCKAN can operate offline.
        :return: a dict of each Resource's metadata, including their Datasets, keyed on resource_id
        """
        # Simplify the metadata structure to insulate from CKAN API changes? Only need resource name or dataset title?
        # No - more explicit if done in accessor methods instead, e.g. `self.get_resource_metadata`
        if not self._metadata and force_download is False:
            self.load_user_metadata()

        if not self._metadata or \
                force_download or \
                (self._metadata_last_updated + datetime.timedelta(seconds=self.check_for_updates_every) <
                 datetime.datetime.utcnow()):
            try:
                # This returns a list of datasets, and within each there is a 'resources' key with a list of resources.
                metadata = self.api.action.package_search(include_private=True)['results']
                # `api.current_package_list_with_resources` gets public resources only, not private ones.
            except requests.exceptions.ConnectionError as e:
                error = \
                    'Unable to reach CKAN and no local copy of CKAN metadata found at %s' % self.metadata_cache_filename
                logging.error(error)
                raise RuntimeError('%s\n%s' % (error, str(e)))

            self._metadata_last_updated = datetime.datetime.utcnow()

            self._metadata = dict()
            for dataset in metadata:
                for resource in dataset['resources']:
                    # After unpickling, `(meta['resource_a']['dataset'] is meta['resource_b']['dataset'])`
                    resource['dataset'] = dataset
                    self._metadata[resource['id']] = resource

            # self._metadata = {resource_id: {resource}} where resource['dataset'] = {dataset} for all CKAN resources

            if not self._in_context_block:
                self.save_user_metadata()
        return self._metadata

    @property
    def local_resource_status(self):
        """
        Returns a dict that stores when each Resource file was last downloaded from the server. By comparing this
        to the server last_updated date we can determine whether our copy is still up to date, or a new copy needs
        downloading.

        :return: A dict containing the last download date keyed on resource_id
        """
        if not self._local_resource_status:
            self._local_resource_status = self._unpickle_from_file(self.resource_status_cache_filename)
            if not self._local_resource_status:
                self._local_resource_status = dict()
        return self._local_resource_status

    @property
    def server_domain(self):
        """
        Returns the domain of the server URL for use in filename cache keys. This is used so that a single cache
        directory can be shared by multiple cached CKAN instances. (`urlparse` behaves inconsistently.)

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
        self._in_context_block = True
        # TODO: create local backup of file in case we can't upload and have to roll back
        return self

    def save_resource_statuses(self):
        if self._local_resource_status:
            self._pickle_to_file(self.resource_status_cache_filename, self._local_resource_status)

    def save_user_metadata(self):
        if self._metadata:
            self._pickle_to_file(self.metadata_cache_filename, (self._metadata_last_updated, self._metadata))

    def load_user_metadata(self):
        try:
            self._metadata_last_updated, self._metadata = self._unpickle_from_file(self.metadata_cache_filename)
        except TypeError:  # file not found / pickle error
            with contextlib.suppress(FileNotFoundError):
                os.remove(self.metadata_cache_filename)  # remove for re-download

    def __exit__(self, type, value, traceback):
        """
        Materialize all state stored in transient storage and release session resources.
        """
        self.save_resource_statuses()
        self.save_user_metadata()
        if self._session:
            self._session.close()
        self.api.close()
        # TODO: restore from local backup of file in case we can't upload and have to roll back

    @property
    def resource_status_cache_filename(self):
        """
        :return: The absolute path of the pickle file that stores when each local resource was last downloaded.
        """
        return os.path.join(self.cache_dir, self.server_domain, self.RESOURCE_STATUS_FILE)

    @property
    def metadata_cache_filename(self):
        """
        :return: The absolute path of the pickle file that stores a local copy of the metadata for all resources a
        user has access to.
        """
        return os.path.join(self.cache_dir, self.server_domain, self.METADATA_FILE % self.username)

    @property
    def iso_utc_datetime(self):
        """
        :return: Returns the current UTC time as an ISO formatted string.
        """
        return datetime.datetime.utcnow().replace(microsecond=0).isoformat()

    def get_resource_metadata(self, resource_id):
        """
        Returns the metadata for a resource, downloading and caching locally as necessary.

        :param resource_id: The resource to look up.
        :return: A dict containing the metadata for the resource.
        """
        try:
            return self.get_ckan_metadata()[resource_id]
        except KeyError:
            raise RuntimeError('Resource ID %s metadata not found.' % resource_id)

    def find_package(self, package_title):
        """
        Searches for a Dataset by title. Used if Dataset title but not ID specified.
        :param package_title: The title to look for.
        :return: An empty list, a single dict of Dataset metadata, or a list of Dataset metadata dicts.
        """
        metadata = self.get_ckan_metadata()
        results = []
        for id, resource in metadata.items():
            if resource['dataset']['title'] == package_title:
                results.append(resource['dataset'])
        return results[0] if len(results) == 1 else results

    def find_resource(self, resource_name, package_title=None):
        """
        Searches for a Resource by Resource name and optionally also containing Dataset title. Used if resource_id
        not specified.
        :param resource_name: The Resource name to look for.
        :param package_title: The Dataset title to look for.
        :return: An empty list, a single dict of Resource metadata, or a list of Resource metadata dicts.
        """
        metadata = self.get_ckan_metadata()
        results = []
        for id, resource in metadata.items():
            if resource['name'] == resource_name:
                if package_title is None or resource['dataset']['title'] == package_title:
                    results.append(resource)
        return results[0] if len(results) == 1 else results

    def get_resource_last_updated(self, resource_id):
        """
        Returns when a resource was last updated on the server. Used to determine whether our local cached copy is up
        to date or a new version needs downloading.

        :param resource_id: The resource_id to examine
        :return: The date and time that the resource was last updated on the server (according to the current cache
        of resource metadata).
        """
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
        Log in to CKAN web UI and store authenticated session.
        """
        self._session = requests.Session()
        data = {'login': self.username, 'password': self.password}
        url = self.address + '/login_generic'
        r = self._session.post(url, data=data)
        if 'field-login' in r.text:
            # Response still contains login form
            raise RuntimeError('Login failed.')

    def get_resource_cache_path(self, resource_id, create=False):
        """
        Returns where a local cached copy of a resource should be stored.

        :param resource_id: The resource_id being stored or sought
        :param create: Whether to create the directory if it doesn't already exist, default False.
        :return: The absolute path in the cache for the resource file.
        """
        path = os.path.abspath(os.path.join(self.cache_dir, self.server_domain, resource_id))
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

        if not self._in_context_block:
            self.save_resource_statuses()

    def _delete_cache(self, resource_id=None):
        if resource_id is None:
            logging.warning('CachedCKAN._delete_cache called with no resource_id - deleting WHOLE cache.')
        cache_path = self.get_resource_cache_path(resource_id) if resource_id else self.cache_dir
        with contextlib.suppress(FileNotFoundError):
            shutil.rmtree(cache_path)
            logging.info('Deleting directory from cache %s', cache_path)

    def _prune_cache(self):
        """
        Delete any cached resources that haven't been accessed for `self.cache_resources_for minutes`, or their custom
        expiry deadline. This is run each time a resource is downloaded.

        Nb. Cache pruning is done according to the *current* `self.cache_resources_for minutes` setting, rather than the
        setting when the resource was previously cached or accessed, unless an override custom expiry was set for a
        specific Resource. These persist between CachedCKAN instantiations and calls until updated. It can be
        updated by passing `cache_for` (in minutes) whenever the resource is accessed.
        """
        default_expiry = datetime.datetime.utcnow() - datetime.timedelta(minutes=self.cache_resources_for)
        for resource_id, resource in self.local_resource_status.items():
            if 'cache_until' in resource:
                if datetime.datetime.utcnow() > resource['cache_until']:
                    self._delete_cache(resource_id)
            elif resource['last_accessed'] < default_expiry:
                self._delete_cache(resource_id)

    def get_resource(self, resource_id, cache_for=None):
        """
        Returns the path of a local copy of a resource, downloading into the cache if necessary.

        :param resource_id: The resource required
        :param cache_for: You can override the default cache duration for an individual resource,
        eg, you want a default of a year but you want a multi-GB file removed (pruned) from the
        cache earlier.
        :return: The absolute path to a local copy of the resource file
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
        Download resource from web UI into the local cache file system.

        :param resource_id: The resource to download
        :return: Absolute path to the local cached copy of the resource file
        """
        package_id = self.get_resource_metadata(resource_id)['package_id']

        url = '{ckan}/dataset/{pkg}/resource/{res}/download/'.format(
              ckan=self.address, pkg=package_id, res=resource_id)

        self.login()
        response = self._session.get(url)

        response.raise_for_status()

        self._delete_cache(resource_id)
        self._prune_cache()

        cache_path = self.get_resource_cache_path(resource_id, True)

        # `cgi.parse_header` doesn't handle non-ASCII filenames
        file_path = os.path.join(cache_path, rfc6266.parse_requests_response(response).filename_unsafe)

        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(512):  # chunk size in bytes
                f.write(chunk)

        if resource_id not in self.local_resource_status:
            self.local_resource_status[resource_id] = dict()
        self.local_resource_status[resource_id]['last_downloaded'] = datetime.datetime.utcnow()

        if not self._in_context_block:
            self.save_resource_statuses()
            self._session.close()

        return file_path

    def create_resource(self, **kwargs):
        """
        Creates a new CKAN Resource with the metadata specified, maintaining a local cache.

        Example usage:
            with CachedCKAN(address='https://data.kimetrica.com', username='user', password='pwd',
                            cache_dir='/ckan_cache') as ckan:

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
        Creates a new CKAN Dataset with the details specified.

        Example usage:
            with CachedCKAN(address='https://data.kimetrica.com', username='user', password='pwd',
                            cache_dir='/ckan_cache') as ckan:

                ckan.create_package(
                    name='joe_data',
                    title='Joe\'s data',
                    author='joe',
                    version='1.2a',
                    format='XLSX'
                )

        I don't think there's a way to upload resources as part of the same API call as creating the package through ckanapi.  # NOQA
        https://stackoverflow.com/questions/48054042/creating-a-ckan-package-dataset-with-resources-using-ckanapi-and-python  # NOQA
        Unless it proves to be a common requirement I think we defer - they may address this in the `ckanapi` package.

        See: http://docs.ckan.org/en/latest/api/#ckan.logic.action.create.package_create
        """
        results = self.api.action.package_create(**kwargs)
        self.get_ckan_metadata(True)
        return results

    def patch_package(self, **kwargs):
        """
        Updates an exsting CKAN Dataset. Any metadata not included in the parameters is unaltered.

        Example usage:
            with CachedCKAN(address='https://data.kimetrica.com', username='user', password='pwd',
                            cache_dir='/ckan_cache') as ckan:

                ckan.patch_package(title='Chris test data',
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
        Updates an exsting CKAN Resource. The file or any metadata not included in the parameters is unaltered.

        Example usage:
            with CachedCKAN(address='https://data.kimetrica.com', username='user', password='pwd',
                            cache_dir='/ckan_cache') as ckan:

                ckan.patch_resource(
                    name='an updated test resource upload',
                    id='e2cbc4be-2bb3-47c4-a64a-b61f82a8a0e2',
                    upload=open('/path/to/file.csv', 'rb')
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
        Updates an existing CKAN Dataset.

        WARNING: Using this method removes all existing data from the package. If you want to keep existing data, use
        `CachedCKAN.patch_package`.

        Example usage:
            with CachedCKAN(address='https://data.kimetrica.com', username='user', password='pwd',
                            cache_dir='/ckan_cache') as ckan:

                ckan.update_package(title='Chris test data',
                                    author='arthur',
                                    version='1.3c',
                                    id='c86186f4-8368-4ecc-a907-08ca67d0e7ab')

        See: http://docs.ckan.org/en/latest/api/#ckan.logic.action.update.package_update
        """
        logging.warning('Updating a package removes all existing data. '
                        'If you wish to keep the existing data, use `CachedCKAN.patch_package`.')
        results = self.api.action.package_update(**kwargs)
        self.get_ckan_metadata(True)
        return results

    def update_resource(self, **kwargs):
        """
        Updates an existing CKAN Resource.

        WARNING: Using this method removes and replaces all existing data from the resource, including the file.
        If you want to keep existing data and only update, use the `CachedCKAN.patch_resource()` method instead.

        Example usage:
            with CachedCKAN(address='https://data.kimetrica.com', username='user', password='pwd',
                            cache_dir='/ckan_cache') as ckan:

                ckan.update_resource(
                    name='another updated resource',
                    id='fd01f45a-b2d4-4897-848c-2b6c16343a49',
                    upload=open('/path/to/file.csv', 'rb')
                )

        See: http://docs.ckan.org/en/latest/api/#ckan.logic.action.update.resource_update
        """
        logging.warning('Updating a resource removes all existing data. '
                        'If you wish to keep the existing data, use `CachedCKAN.patch_resource`.')
        results = self.api.action.resource_update(**kwargs)
        self.get_ckan_metadata(True)
        if 'upload' in kwargs:
            resource_id = results['id'] if 'id' in results else kwargs['id']
            self._import_resource_to_cache(kwargs['upload'], resource_id)
        return results

    def delete_resource(self, resource_id):
        """
        Irreversibly deletes a CKAN Resource.

        Example usage:
            with CachedCKAN(address='https://data.kimetrica.com', username='user', password='pwd',
                            cache_dir='/ckan_cache') as ckan:

                ckan.delete_resource(resource_id='fd01f45a-b2d4-4897-848c-2b6c16343a49')

        See: http://docs.ckan.org/en/latest/api/#ckan.logic.action.update.resource_delete
        """
        results = self.api.action.resource_delete(id=resource_id)
        self.get_ckan_metadata(True)
        self._delete_cache(resource_id=resource_id)
        return results


class CkanTarget(luigi.Target):
    """
    A Luigi Target that stores and retrieves data from a (locally cached) CKAN Instance.

    Usage:

        In the `output` method:

            return CkanTarget(address='https://data.kimetrica.com', username='user', password='pwd',
                              cache_dir='/path/to/cache', resource={'name': 'My CKAN resource'})

        address, user, password and cache directory can alternatively be specified in your `luigi.cfg`

        In the `run` method:

            * `CkanTarget.put(path)` uploads a resource to CKAN.
            * `CkanTarget.get()` returns the path of a downloaded copy of a CKAN resource.
            * `CkanTarget.remove()` deletes the CKAN resource from CKAN.

    A local cache of the resources and their status and metadata is automatically maintained.

    The put/get/remove API was chosen as a step towards a consistent API for all Target types, so
    that Tasks can be defined more independently of Target type. All metadata is set at
    CkanTarget instantiation, and only the path of the file passed at runtime.

    Limitations:

        This version of `CkanTarget` does **not**:

            * Patch, update or create datasets
            * Store the `resource id` of created resources - this would require persistence, which
            would limit `CkanTarget` to single worker installs or require persistence to disk
            independently per pipeline instance in a thread-safe manner.
            * The dataset title / resource name combination must be unique if you do not specify
            resource id. (The API also supports dataset name and resource title, but these are not
            exposed in the web GUI, so are ignored (passed to CKAN untouched) by `CkanTarget`.)
            * Patch or update existing resources. Luigi tests for the presence of a Target, and if
            found does not re-run the task that created it. Therefore there isn't a 'standard Luigi'
            approach to updating existing resources. For CkanTarget you simply delete then recreate,
            suggesting the standard practice of using naming (dataset and resource) rather than
            resource IDs to identify resources. We could revisit this with a timeout or overwrite
            flag. And/or we might want to add the ability to create/read/update/delete by organization
            too, which will probably by used to categorize by something other than organization in our
            context.
            * Work offline. CkanTarget does cache files locally - this is important as Luigi
            instantiates Targets several times per use. But working offline is harder because when
            offline, any updates invalidate the local cache, we must update the local copy of the
            resource metadata in the same way the server would, and unit tests. Not too hard to fix but
            not important enough now.
            * `CkanTarget` is not thread-safe - if a second Python shell attempts to access a resource
            while a first shell is already downloading it, I believe the second shell will start a
            second download rather than wait. (There probably needs to be semaphore functionality in
            local_resource_status.pickle to tell subsequent downloaders to wait.)
            * Unit tests

    Running the code

        There are fully worked and working examples of each use of `CkanTarget` in CkanTarget's
        docstrings. (Some require datasets and resources to already exist on CKAN.)

        CKAN section of luigi.cfg is now (this is also in `luigi.cfg.example` in the repo):

        ```
        [core]
        cache_dir = /cache  # CkanTarget will create a subdirectory in here if only this is
        specified, so that this cache directory can be universal to all Targets.

        [ckan]
        username=username
        password=pwd
        address=https://data.kimetrica.com
        cache_dir=/cache  # optional, overrides [core] cache_dir
        check_for_updates_every=300  # update CKAN metadata every x seconds
        default_dataset_id=c86186f4-8368-4ecc-a907-08ca67d0e7ab  # if none specified
        ```
    """
    def __init__(self, address=None, username=None, password=None, resource=None, dataset=None,
                 check_for_updates_every=None, cache_dir=None, **kwargs):
        """
        :param address: the URL for the CKAN server. You  can specify a default CKAN address,
        username and password in your luigi.cfg file, in which case these three are optional.
        :param username: CKAN username
        :param password: CKAN password
        :param resource:
            Example `resource` dict:
            {
                id='c86186f4-8368-4ecc-a907-08ca67d0e7ab',
                name='a test resource upload',
                package_id='e76186f4-8368-4ecc-a907-08ca67d0e7ab',
                upload=open('/path/to/file.xls', 'rb')
            }
        :param dataset:
            Example `dataset` dict:
            {
                title='Chris test data',
                author='arthur',
                version='1.3c',
                id='e76186f4-8368-4ecc-a907-08ca67d0e7ab'
            }
        :param check_for_updates_every: Frequency CKAN metadata updated in seconds
        :param cache_dir: local directory in which CKAN cache is stored - should be shared by all users. If you omit
        this, CkanTarget looks in luigi.cfg for the `cache_dir` setting in the `[ckan]` section. If that is omitted too,
        CkanTarget looks for the `cache_dir` setting in the `[core]` section, then creates a sub-directory within this
        called `CkanTarget`. The latter configuration sharing a cache directory between all Targets is probably the
        simplest.
        :param kwargs: extra parameters may be passed through to the CKAN API initialization

        Nb. If you wish to specify the dataset inside the resource dict, it is
        `resource['package_id']`. This is synonymous with `dataset['id']`. The latter
        is recommended. The former is because the API follows an old CKAN naming convention.
        """
        # TODO: Prohibit use of resource_id, as Targets do not patch in place? Or add `overwrite` flag or timeout value?
        self.resource = resource or {}
        self.dataset = dataset or {'id': config['ckan']['default_dataset_id']}

        if cache_dir is None:
            cache_dir = config.get('ckan', 'cache_dir', None)
        if cache_dir is None:
            cache_dir = os.path.join(config.get('core', 'cache_dir'), type(self).__name__)
        cache_dir = os.path.abspath(cache_dir)

        self.ckan_kwargs = kwargs or {}
        self.ckan_kwargs.update({'address': address or config.get('ckan', 'address'),
                                 'username': username or config.get('ckan', 'username'),
                                 'password': password or config.get('ckan', 'password'),
                                 'cache_dir': cache_dir,
                                 'check_for_updates_every':
                                     check_for_updates_every or
                                     int(config.get('ckan', 'check_for_updates_every')), })

    @property
    def resource_id(self):
        if 'id' not in self.resource or not self.resource['id']:
            resource_name = self.resource['name']
            dataset_title = self.dataset_title
            # TODO: Cache instantiated CkanTarget in memory when 1 worker, as Luigi instantiates Targets 4 times per use
            with CachedCKAN(**self.ckan_kwargs) as ckan:
                match = ckan.find_resource(resource_name, dataset_title)
                if isinstance(match, dict):
                    self.resource['id'] = match['id']
                else:
                    logging.info('%s matching resources found.', len(match))
        return self.resource.get('id', None)

    @property
    def dataset_title(self):
        return self.resource.get('dataset', {}).get('title', None) or self.dataset.get('title', None)

    @property
    def dataset_id(self):
        dataset_id = self.dataset.get('id', None) or \
                     self.resource.get('package_id', None) or \
                     self.resource.get('dataset', {}).get('id', None)  # NOQA

        if not dataset_id:
            with CachedCKAN(**self.ckan_kwargs) as ckan:
                match = ckan.find_package(self.dataset_title)
                if isinstance(match, dict):
                    dataset_id = match['id']
                else:
                    logging.info('%s matching datasets found.', len(match))
                if not dataset_id:
                    resource_name = self.resource['name']
                    dataset_title = self.dataset_title
                    match = ckan.find_resource(resource_name, dataset_title)
                    if isinstance(match, dict):
                        dataset_id = match['package_id']
                    else:
                        logging.info('%s matching resources found.', len(match))

        if dataset_id:
            self.dataset['id'] = dataset_id
        return dataset_id

    def exists(self):
        """
        The purpose of this method is to inform the scheduler whether it needs to
        run the prior task to generate this resource. This will download and cache
        the resource if not already local or local copy is out of date, or return
        False if the resource isn't found.

        :return: True if the resource is found locally or on CKAN, else False.
        """
        return bool(self.get())

    @property
    def path(self):
        return self.get()

    def get(self):
        """
        Returns the path to a local copy of the resource file, downloading/updating if necessary.

        Example usage:

            class GetFromCkan(luigi.ExternalTask):
                def output(self):
                    return CkanTarget(address='https://data.kimetrica.com', username='user', password='pwd',
                                      cache_dir='/path/to/cache', resource={'name': 'My CKAN resource'})

            class CopyToLocalTarget(luigi.Task):
                def requires(self):
                    return GetFromCkan()

                def output(self):
                    filename = 'TstCknDld%s' % \
                               datetime.datetime.utcnow().replace(microsecond=0).isoformat().replace(':', '.')
                    # Nop: https://github.com/spotify/luigi/issues/1647
                    return luigi.LocalTarget(filename, format=luigi.format.Nop)

                def run(self):
                    with open(self.input().get(), 'rb') as in_file, self.output().open('wb') as out_file:
                        for l in in_file:
                            out_file.write(l)

        :return: `str` The absolute file path to the locally cached copy of the resource.
        """
        if self.resource_id:
            with CachedCKAN(**self.ckan_kwargs) as ckan:
                return ckan.get_resource(resource_id=self.resource_id)
        return None

    def put(self, file_path):
        """
        Uploads a new version of a resource file and metadata to the specified dataset and resource,
        maintaining a local cached copy. To update an existing resource, call `self.input().remove()`
        before uploading a new version. (Otherwise Luigi detects the resource file's presence and
        assumes the upstream task doesn't need re-running to recreate.)

        Example usage:

            class GetFromCkan(luigi.ExternalTask):
                def output(self):
                    return CkanTarget(address='https://data.kimetrica.com', username='user', password='pwd',
                                      cache_dir='/path/to/cache', resource={'name': 'My CKAN resource'})

            class UploadToCkan(luigi.Task):
                def requires(self):
                    return GetFromCkan()

                def output(self):
                    name = 'Test upload at %s' % \
                           datetime.datetime.utcnow().replace(microsecond=0).isoformat().replace(':', '.')
                    return CkanTarget(address='https://data.kimetrica.com', username='user', password='pwd',
                                      cache_dir='/path/to/cache', dataset={'id': 'c86186f4-8368-4ecc-a907-08ca67d0e7ab'},  # NOQA
                                      resource={'name': name})

                def run(self):
                    self.output().put(self.input().get())

        :param file_path: `str` Absolute path to file to be uploaded
        :return: None

        Note: As our CKAN instance scales, these references have useful performance suggestions for large CKAN instances:  # NOQA
            * https://stackoverflow.com/a/33265658/1308967
            * https://github.com/ckan/ckan/issues/681
            * https://github.com/ckan/ckan/wiki/Performance-tips-for-large-imports (Summary of previous bullet)
        """
        resource_kwargs = self.resource
        if file_path:
            resource_kwargs['upload'] = open(file_path, 'rb')
        if self.dataset_id:
            resource_kwargs['package_id'] = self.dataset_id
        # TODO: Offline mode should be able to run offline (so pass files from task to task),
        # without uploading and getting a resource id, and expiring files after a few seconds
        with CachedCKAN(**self.ckan_kwargs) as ckan:
            status = ckan.create_resource(**resource_kwargs)
        self.resource['id'] = status['id']
        self.resource['package_id'] = self.dataset['id'] = status['package_id']
        # TODO: Create dataset too? How indicate in CkanTarget API?
        # TODO: Patch (update) existing dataset and/or resource? Issue is if a Target exists, Luigi
        # considers running precedent tasks unnecessary, so currently you must delete and re-create.
        # One solution would be some sort of timeout that automatically deletes a resource if someone
        # tries to create it after x seconds has elapsed.

    def remove(self):
        """
        Deletes a resource permanently from CKAN and the local cache.

        Example usage:

            class GetFromCkan(luigi.ExternalTask):
                def output(self):
                    return CkanTarget(address='https://data.kimetrica.com', username='user', password='pwd',
                                      cache_dir='/path/to/cache', resource={'name': 'My CKAN resource'})

            class DeleteFromCkan(luigi.Task):
                def requires(self):
                    return GetFromCkan()

                def run(self):
                    self.input().remove()

        :return: None
        """
        with CachedCKAN(**self.ckan_kwargs) as ckan:
            ckan.delete_resource(resource_id=self.resource_id)
