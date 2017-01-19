# -*- coding: utf-8 -*-
__copyright__ = u"Copyright (c), This file is part of the AiiDA platform. For further information please visit http://www.aiida.net/. All rights reserved."
__license__ = "MIT license, see LICENSE.txt file."
__version__ = "0.7.1"
__authors__ = "The AiiDA team."

import uuid as UUID

from aiida import settings
from aiida.backends.testbase import AiidaTestCase
from aiida.repository.implementation.filesystem.repository import RepositoryFileSystem
from aiida.orm.repotable import Repotable
from aiida.orm.node import Node

class TestRepotable(AiidaTestCase):
    """
    """

    @classmethod
    def setUpClass(cls):
        """
        Executed once before any of the test methods are executed
        """
        super(TestRepotable, cls).setUpClass()
        cls.config = {
            'base_path' : settings.REPOSITORY_PATH,
            'uuid_file' : settings.REPOSITORY_UUID_PATH,
            'repo_name' : settings.REPOSITORY_NAME,
        }
        cls.repository = RepositoryFileSystem(cls.config)
        cls.repotable  = Repotable()

    @classmethod
    def tearDownClass(cls):
        """
        Executed once after all of the test methods have been executed
        """
        super(TestRepotable, cls).tearDownClass()

    def setUp(self):
        """
        Executed before each test method
        """

    def tearDown(self):
        """
        Executed after each test method
        """

    def test_00(self):
        """
        register_file should only be called from within a node's store() method
        """
        node = Node()
        with self.assertRaises(ValueError):
            self.repotable.register_file(node, self.repository, UUID.uuid4(), 'path')