# -*- coding: utf-8 -*-
__copyright__ = u"Copyright (c), This file is part of the AiiDA platform. For further information please visit http://www.aiida.net/. All rights reserved."
__license__ = "MIT license, see LICENSE.txt file."
__version__ = "0.7.1"
__authors__ = "The AiiDA team."

import StringIO

from aiida import settings
from aiida.backends.testbase import AiidaTestCase
from aiida.repository.implementation.filesystem.repository import RepositoryFileSystem

class TestRepository(AiidaTestCase):
    """
    """

    @classmethod
    def setUpClass(cls):
        """
        Executed once before any of the test methods are executed
        """
        super(TestRepository, cls).setUpClass()
        cls.config = {
            'base_path' : settings.REPOSITORY_BASE_PATH,
            'uuid_path' : settings.REPOSITORY_UUID_PATH,
            'repo_name' : settings.REPOSITORY_NAME,
        }
        cls.repository = RepositoryFileSystem(cls.config)

    @classmethod
    def tearDownClass(cls):
        """
        Executed once after all of the test methods have been executed
        """
        super(TestRepository, cls).tearDownClass()

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
        put_new_object should only accept normalized paths
        """
        stream = StringIO.StringIO('content')
        with self.assertRaises(ValueError):
            self.repository.put_new_object('a/../b.dat', stream)


    def test_01(self):
        """
        put_new_object should accept paths with multiple nested non-existing directories
        """
        stream = StringIO.StringIO('content')
        self.repository.put_new_object('a/b/c/d.dat', stream)


    def test_02(self):
        """
        put_new_object should guarantee a new unique path if desired path already exists
        """
        stream = StringIO.StringIO('content')
        key_00 = self.repository.put_new_object('a/b.dat', stream)
        key_01 = self.repository.put_new_object('a/b.dat', stream)
        key_02 = self.repository.put_new_object('a/b.dat', stream)

        self.assertEqual(key_01, '%s(%d)' % (key_00, 1))
        self.assertEqual(key_02, '%s(%d)' % (key_00, 2))


    def test_03(self):
        """
        get_object should return a ValueError if reading the object fails 
        """
        with self.assertRaises(ValueError):
            self.repository.get_object('a/non_existing.dat')


    def test_04(self):
        """
        get_object should return the content of the object given a valid key
        """
        stream = StringIO.StringIO('content')
        key = self.repository.put_new_object('a/non_existing.dat', stream)
        content = self.repository.get_object(key)

        self.assertEqual(content, stream.getvalue())


    def test_05(self):
        """
        del_object should delete an object given a valid key
        """
        key = 'a/non_existing.dat'
        self.repository.del_object(key)
        with self.assertRaises(ValueError):
            self.repository.get_object(key)


    def test_06(self):
        """
        del_object should raise no exception when deleting a non-existing object
        """
        key = 'a/non_existing.dat'
        self.repository.del_object(key)


    def test_07(self):
        """
        The exists() method should return False for a non-existing key
        """
        key = 'a/non_existing.dat'
        self.assertEqual(self.repository.exists(key), False)


    def test_08(self):
        """
        The exists() method should return True for an existing key
        """
        key    = 'a/non_existing.dat'
        stream = StringIO.StringIO('content')

        self.repository.put_new_object(key, stream)
        self.assertEqual(self.repository.exists(key), True)