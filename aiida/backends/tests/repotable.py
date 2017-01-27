# -*- coding: utf-8 -*-
__copyright__ = u"Copyright (c), This file is part of the AiiDA platform. For further information please visit http://www.aiida.net/. All rights reserved."
__license__ = "MIT license, see LICENSE.txt file."
__version__ = "0.7.1"
__authors__ = "The AiiDA team."

import uuid as UUID

from aiida.settings import get_repository
from aiida.backends.testbase import AiidaTestCase
from aiida.orm.node import Node
from aiida.orm.repotable import Repotable

class TestRepotable(AiidaTestCase):
    """
    """

    @classmethod
    def setUpClass(cls):
        """
        Executed once before any of the test methods are executed
        """
        super(TestRepotable, cls).setUpClass()
        cls.repository = get_repository()
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
        super(TestRepotable, self).setUp()

    def tearDown(self):
        """
        Executed after each test method
        """
        super(TestRepotable, self).tearDown()


    def test_00(self):
        """
        register_file should only be called from within a node's store() method
        """
        node = Node()
        with self.assertRaises(ValueError):
            self.repotable.register_file(node, self.repository, UUID.uuid4(), 'path')