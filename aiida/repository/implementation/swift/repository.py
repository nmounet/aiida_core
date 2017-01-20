# -*- coding: utf-8 -*-

from abc import ABCMeta, abstractmethod
from aiida.repository.repository import Repository

class RepositorySwift(Repository):
    """
    """
    __metaclass__ = ABCMeta

    def __init__(self, repo_config):
        """
        Requires the following parameters to properly configure the Repository

         * uuid_key: Key under which the file containing the UUID is stored
         * auth_url: Fully qualified URI to authentication server
         * auth_version: Version number of authentication scheme
         * auth_user: Username to authenticate
         * auth_key: Key to be used for authentication
         * auth_cacert: Absolute path to certificate for authentication
         * container: Container name in which objects should be stored

        :param repo_config: dictionary with configuration details for repository
        """
        self.name         = repo_config['repo_name']
        self.uuid_key     = repo_config['uuid_key']
        self.auth_url     = repo_config['auth_url']
        self.auth_version = repo_config['auth_version']
        self.auth_user    = repo_config['auth_user']
        self.auth_key     = repo_config['auth_key']
        self.auth_cacert  = repo_config['auth_cacert']
        self.container    = repo_config['container']
        self.os_options   = {
                                'user_domain_name': 'Default',
                                'project_name': 'aiida_project',
                                'project_domain_name': 'Default'
                            }

        self.connection   = Connection(
                                authurl=self.auth_url,
                                user=self.auth_user,
                                key=self.auth_key,
                                os_options=self.os_options,
                                auth_version=self.auth_version,
                                cacert=self.auth_cacert,
                                insecure=True,
                            )


    def _uniquify(self, key):
        """
        Given a key for an object, make sure it is unique, by adding a suffix
        of the format '(%d)' to the original key, such that the resulting new key
        corresponds to a key that does not yet exist.

        :param key: string with object key
        :return: string with unique object key
        """
        count = 1
        unique_key = '%s(%d)' % (key, count)
        while self.exists(unique_key):
            count += 1
            unique_key = '%s(%d)' % (key, count)

        return unique_key


    def _validate_key(self, key):
        """
        Validate a key
        TODO: implement checks for key validity
        """
        pass


    def get_name(self):
        """
        Return the name of the repository which is a human-readable label

        :return name: the human readable label associated with this repository
        """
        return self.name


    def get_uuid(self):
        """
        Return the UUID identifying the repository

        :return uuid: the uuid associated with this repository
        :raise ValueError: raises exception if the file that should contain the repo uuid cannot be read
        """
        try:
            _, uuid = self.connection.get_object(self.container, self.uuid_key)
        except IOError as error:
            raise ValueError("Could not retrieve '{}:{}' and therefore cannot retrieve the UUID associated with this repository".format(self.container, self.uuid_key))

        return uuid.strip()


    def exists(self, key):
        """
        Determine whether the object identified by key exists and is readable

        :return boolean: returns True if the object exists and is readable, False otherwise
        """
        try:
            response = self.connection.head_object(self.container, key)
            is_readable = True
        except ClientException as exception:
            is_readable = False

        return is_readable


    def put_object(self, key, source, stop_if_exists=False):
        """
        Store a new object under 'key' with contents 'source' if it does not yet exist.
        Overwrite an existing object if stop_if_exists is set to False.
        Raise an exception if stop_if_exists is True and the object already exists.

        :param key: fully qualified identifier for the object within the repository
        :param source: filelike object with the content to be stored
        :param stop_if_exists:
        """
        self._validate_key(key)

        if stop_if_exists and self.exists(key):
            raise ValueError("Cannot write to '{}' because the object already exists".format(key))

        try:
            etag = self.connection.put_object(self.container, key, source.getvalue())
        except ClientException as exception:
            raise ValueError("Writing object with key '{}' to '{}' failed".format(key, os.path.join(self.base_path, key)))

        return key


    def put_new_object(self, key, source):
        """
        Store a new object under 'key' with contents 'source'
        If the provided key already exists, it will be adapted to
        ensure that it is unique. The eventual key under which the
        newly created object is stored is returned upon success

        :param key: fully qualified identifier for the object within the repository
        :param source: filelike object with the content to be stored
        :return: the key of the newly generated object
        """
        if self.exists(key):
            key = self._uniquify(key)

        try:
            key = self.put_object(key, source, True)
        except ValueError as error:
            raise

        return key


    def get_object(self, key):
        """
        Return the content of a object identified by key

        :param key: fully qualified identifier for the object within the repository
        :raise ValueError: raises exception if given key can not be resolved to readable object
        """
        try:
            _, content = self.connection.get_object(self.container, key)
        except ClientException as exception:
            raise ValueError("Provided key can not be mapped to an existing object")

        return content


    def del_object(self, key):
        """
        Delete the object from the repository

        :param key: fully qualified identifier for the object within the repository
        :raises: ValueError if object identified by key could not be deleted
        """
        try:
            self.connection.get_object(self.container, key)
        except ClientException as exception:
            raise ValueError("Provided key can not be mapped to an existing object")

        return