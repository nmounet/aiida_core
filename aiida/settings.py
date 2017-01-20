# -*- coding: utf-8 -*-

import os

from aiida.backends import settings

from aiida.common.exceptions import ConfigurationError
from aiida.common.setup import (get_config, get_secret_key, get_property,
                                get_profile_config, get_default_profile,
                                parse_repository_uri)

__copyright__ = u"Copyright (c), This file is part of the AiiDA platform. For further information please visit http://www.aiida.net/. All rights reserved."
__license__ = "MIT license, see LICENSE.txt file."
__authors__ = "The AiiDA team."
__version__ = "0.7.1"


TIME_ZONE = "Europe/Paris"
USE_TZ = True

try:
    confs = get_config()
except ConfigurationError:
    raise ConfigurationError("Please run the AiiDA Installation, no config found")

if settings.AIIDADB_PROFILE is None:
    raise ConfigurationError("AIIDADB_PROFILE not defined, did you load django "
                             "through the AiiDA load_dbenv()?")

profile_conf = get_profile_config(settings.AIIDADB_PROFILE, conf_dict=confs)

# put all database specific portions of settings here
BACKEND = profile_conf.get('AIIDADB_BACKEND', 'django')
DBENGINE = profile_conf.get('AIIDADB_ENGINE', '')
DBNAME = profile_conf.get('AIIDADB_NAME', '')
DBUSER = profile_conf.get('AIIDADB_USER', '')
DBPASS = profile_conf.get('AIIDADB_PASS', '')
DBHOST = profile_conf.get('AIIDADB_HOST', '')
DBPORT = profile_conf.get('AIIDADB_PORT', '')
REPOSITORY_NAME = profile_conf.get('REPOSITORY_NAME', '')


import errno
import urlparse
import tempfile

try:
    repo_conf = confs['repositories'][REPOSITORY_NAME]
except Exception as exception:
    raise ConfigurationError("The chosen repository '{}' is not defined in the configuration".format(REPOSITORY_NAME))

try:
    repo_type = repo_conf['TYPE'];
except Exception as exception:
    raise ConfigurationError("The chosen repository '{}' does not specify a type in 'TYPE'".format(REPOSITORY_NAME))

if repo_type == 'filesystem':
    base_url  = repo_conf.get('BASE_URL', '')
    uuid_file = repo_conf.get('UUID_FILE', '')

    parsed_base_url   = urlparse.urlparse(base_url)
    repository_scheme = parsed_base_url.scheme
    repository_path   = os.path.normpath(parsed_base_url.path)

    # At this point configured repositories should have been created and initialized
    # so we verify that the prerequisite resources exist and are accessible
    if repository_scheme != 'file':
        raise ConfigurationError("The protocol for a filesystem repository should be 'file://'")

    # Check if the repository directory is writable
    try:
        tempfile = tempfile.TemporaryFile(dir=repository_path)
        tempfile.close()
    except OSError as e:
        raise ConfigurationError("The configured base path '{}' is not writable".format(repository_path))

    # Check if the UUID file is readable
    try:
        uuid_path = os.path.join(repository_path, uuid_file)
        with open(uuid_path, 'rb') as fp:
            fp.read()
    except IOError as e:
        raise ConfigurationError("The configured uuid file '{}' is not readable".format(uuid_path))

    # Set global variables
    REPOSITORY_BASE_PATH = repository_path
    REPOSITORY_UUID_PATH = uuid_path

elif repo_type == 'swift':
    raise ConfigurationError("The configured repository type '{}' is not yet implemented".format(repo_type))
else:
    raise ConfigurationError("Unknown repository type '{}'".format(repo_type))