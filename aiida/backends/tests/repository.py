# -*- coding: utf-8 -*-
__copyright__ = u"Copyright (c), This file is part of the AiiDA platform. For further information please visit http://www.aiida.net/. All rights reserved."
__license__ = "MIT license, see LICENSE.txt file."
__version__ = "0.7.1"
__authors__ = "The AiiDA team."

import os
import errno
import shutil
import uuid as UUID

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
        cls.config = {
            'base_path' : settings.REPOSITORY_PATH,
            'uuid_file' : os.path.join(settings.REPOSITORY_PATH, 'uuid.dat'),
        }
        cls.repo = RepositoryFileSystem(cls.config)

        try:
            os.makedirs(cls.config['base_path'])
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(cls.config['base_path']):
                pass
            else:
                raise

        with open(cls.config['uuid_file'], 'w') as f:
            f.write(unicode(UUID.uuid4()))

    @classmethod
    def tearDownClass(cls):
        """
        Executed once after all of the test methods have been executed
        """
        shutil.rmtree(cls.config['base_path'])

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
        with self.assertRaises(ValueError):
            self.repo.put_new_object('content', 'uuid', 'a/../b.dat')

    def test_01(self):
        """
        put_new_object should accept paths with multiple nested non-existing directories
        """
        self.repo.put_new_object('content', 'uuid', 'a/b/c/d.dat')

    def test_02(self):
        """
        put_new_object should guarantee a new unique path if desired path already exists
        """
        key_00 = self.repo.put_new_object('content', 'uuid', 'a/b.dat')
        key_01 = self.repo.put_new_object('content', 'uuid', 'a/b.dat')
        key_02 = self.repo.put_new_object('content', 'uuid', 'a/b.dat')

        self.assertEqual(key_01, '%s(%d)' % (key_00, 1))
        self.assertEqual(key_02, '%s(%d)' % (key_00, 2))

    def test_03(self):
        """
        get_object should return a ValueError if reading the object fails 
        """
        with self.assertRaises(ValueError):
            self.repo.get_object('a/non_existing.dat')

    def test_04(self):
        """
        get_object should return the content of the object given a valid key
        """
        source = 'content'
        key = self.repo.put_new_object(source, 'uuid', 'a/non_existing.dat')
        content = self.repo.get_object(key)

        self.assertEqual(content, source)