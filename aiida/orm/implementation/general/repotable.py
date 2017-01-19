# -*- coding: utf-8 -*-

from abc import ABCMeta, abstractmethod, abstractproperty

__copyright__ = u"Copyright (c), This file is part of the AiiDA platform. For further information please visit http://www.aiida.net/. All rights reserved."
__license__ = "MIT license, see LICENSE.txt file."
__version__ = "0.7.1"
__authors__ = "The AiiDA team."


class AbstractRepotable(object):
    """
    This is the AiiDA ORM class to access information about the files stored in the repo.
    Will have both Django and SqlAlchemy implementations.
    """
    __metaclass__ = ABCMeta


    @abstractmethod
    def get_file(self, node, path):
        """
        """
        pass


    @abstractmethod
    def register_directory(self, node, path, recursive=False, stop_if_exists=True):
        """
        """
        pass


    @abstractmethod
    def register_file(self, node, path, repo, key):
        """
        """
        pass