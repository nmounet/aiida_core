# -*- coding: utf-8 -*-
__copyright__ = u"Copyright (c), This file is part of the AiiDA platform. For further information please visit http://www.aiida.net/. All rights reserved."
__license__ = "MIT license, see LICENSE.txt file."
__version__ = "0.7.1"
__authors__ = "The AiiDA team."

import uuid as UUID
from StringIO import StringIO

from aiida.settings import get_repository
from aiida.backends.testbase import AiidaTestCase

class TestRepository(AiidaTestCase):
    """
    """

    @classmethod
    def setUpClass(cls):
        """
        Executed once before any of the test methods are executed
        """
        super(TestRepository, cls).setUpClass()
        cls.repository = get_repository()

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
        super(TestRepository, self).setUp()

    def tearDown(self):
        """
        Executed after each test method
        """
        super(TestRepository, self).tearDown()


    def test_get_uuid(self):
        """
        get_uuid should return the uuid entered in the constructor or through set_uuid
        """
        uuid = unicode(UUID.uuid4())
        self.repository.set_uuid(uuid)
        self.assertEqual(self.repository.get_uuid(), uuid)


    def test_exists_false(self):
        """
        The exists() method should return False for a non-existing key
        """
        key = 'exists/non_existing.dat'
        self.assertEqual(self.repository.exists(key), False)


    def test_exists_true(self):
        """
        The exists() method should return True for an existing key
        """
        key    = 'exists/existing.dat'
        stream = StringIO('content')

        self.repository.put_new_object(key, stream)
        self.assertEqual(self.repository.exists(key), True)


    def test_put_object_overwrite(self):
        """
        put_object should overwrite a pre-existing object by default
        without complaints
        """
        key = 'put/object_overwrite.dat'
        stream = StringIO('content')
        
        self.repository.put_object(key, stream)
        self.repository.put_object(key, stream)
        content = self.repository.get_object(key)

        self.assertEqual(content, stream.getvalue())


    def test_put_object_no_overwrite(self):
        """
        put_object should raise an exception when stop_if_exists is set to True
        and the object already exists
        """
        key = 'put/object_non_overwrite.dat'
        stream = StringIO('content')
        
        self.repository.put_object(key, stream)

        with self.assertRaises(ValueError):
            self.repository.put_object(key, stream, stop_if_exists=True)


    def test_put_object_non_normalized_path(self):
        """
        put_object should only accept normalized paths
        """
        stream = StringIO('content')
        with self.assertRaises(ValueError):
            self.repository.put_object('a/../b.dat', stream)


    def test_put_object_nested_object(self):
        """
        put_object should accept paths with multiple nested non-existing directories
        """
        stream = StringIO('content')
        self.repository.put_object('a/b/c/d.dat', stream)


    def test_put_new_object_should_guarantee_unique_key(self):
        """
        put_new_object should guarantee a new unique path if desired path already exists
        """
        stream = StringIO('content')
        key_00 = self.repository.put_new_object('a/b.dat', stream)
        key_01 = self.repository.put_new_object('a/b.dat', stream)
        key_02 = self.repository.put_new_object('a/b.dat', stream)

        self.assertEqual(key_01, '%s(%d)' % (key_00, 1))
        self.assertEqual(key_02, '%s(%d)' % (key_00, 2))


    def test_get_object_non_existing(self):
        """
        get_object should return a ValueError if reading the object fails 
        """
        with self.assertRaises(ValueError):
            self.repository.get_object('get/non_existing.dat')


    def test_get_object(self):
        """
        get_object should return the content of the object given a valid key
        """
        stream = StringIO('content')
        key = self.repository.put_new_object('get/existing.dat', stream)
        content = self.repository.get_object(key)

        self.assertEqual(content, stream.getvalue())


    def test_del_object(self):
        """
        del_object should delete an object given a valid key
        """
        key = 'delete/existing.dat'
        self.repository.put_object(key, StringIO())
        self.repository.del_object(key)
        with self.assertRaises(ValueError):
            self.repository.get_object(key)


    def test_del_object_non_existing(self):
        """
        del_object should raise no exception when deleting a non-existing object
        """
        key = 'delete/non_existing.dat'
        self.repository.del_object(key)