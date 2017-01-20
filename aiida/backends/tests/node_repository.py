# -*- coding: utf-8 -*-
__copyright__ = u"Copyright (c), This file is part of the AiiDA platform. For further information please visit http://www.aiida.net/. All rights reserved."
__license__ = "MIT license, see LICENSE.txt file."
__version__ = "0.7.1"
__authors__ = "The AiiDA team."

import os
import StringIO

from aiida import settings
from aiida.backends.testbase import AiidaTestCase
from aiida.repository.implementation.filesystem.repository import RepositoryFileSystem
from aiida.repository.node_repository import NodeRepository
from aiida.orm.node import Node
from aiida.backends.djsite.db.models import DbFile, DbRepository, DbNode, DbNodeFile

class TestNodeRepository(AiidaTestCase):
    """
    """

    @classmethod
    def setUpClass(cls):
        """
        Executed once before any of the test methods are executed
        """
        super(TestNodeRepository, cls).setUpClass()
        cls.config = {
            'base_path' : settings.REPOSITORY_PATH,
            'uuid_file' : settings.REPOSITORY_UUID_PATH,
            'repo_name' : settings.REPOSITORY_NAME,
        }
        cls.repository = RepositoryFileSystem(cls.config)

    @classmethod
    def tearDownClass(cls):
        """
        Executed once after all of the test methods have been executed
        """
        super(TestNodeRepository, cls).tearDownClass()

    def setUp(self):
        """
        Executed before each test method
        """

    def tearDown(self):
        """
        Executed after each test method
        """

    def _mock_directory_tree(self, node, tree):
        """
        Takes a dictionary that represents a file hierarchy and transfers
        it to the node by creating the directories and files

        The tree variable is a dictionary that represents the hierarchy
        where a directory is represented by another dictionary and a file
        is a filelike object
        """
        for path, content in self._tree_leaves(tree):
            if isinstance(content, dict):
                folder = node._get_folder_pathsubfolder.get_subfolder(path)
                folder.create()
            else:
                dirname  = os.path.dirname(path)
                basename = os.path.basename(path)

                folder = node._get_folder_pathsubfolder.get_subfolder(dirname)
                folder.create()
                folder.create_file_from_filelike(content, basename)


    def _tree_leaves(self, tree):
        """
        Recursively unwind the tree object to return the leaves
        be it empty dictionaries or filelike objects
        """
        import collections
        for key, value in tree.iteritems():
            if isinstance(value, collections.Mapping) and not value:
                yield key, value
            elif isinstance(value, collections.Mapping):
                for inner_key, inner_value in self._tree_leaves(value):
                    yield os.path.join(key, inner_key), inner_value
            else:
                yield key, value


    def test_non_stored_node(self):
        """
        Operating on a non-stored node should raise a ValueError
        """
        node = Node()
        repo = NodeRepository(node, self.repository)

        with self.assertRaises(ValueError):
            repo.print_tree()


    def test_non_existing_directory(self):
        """
        Retrieving a non-existing directory should raise a ValueError
        """
        node = Node()
        repo = NodeRepository(node, self.repository)

        with self.assertRaises(ValueError):
            repo.get_directory('path_that_does_not_exist')


    def test_non_existing_file(self):
        """
        Retrieving a non-existing file should raise a ValueError
        """
        node = Node()
        repo = NodeRepository(node, self.repository)

        with self.assertRaises(ValueError):
            repo.get_file_content('file_that_does_not_exist.dat')


    def test_store_and_retrieve_single_file(self):
        """
        Store a single file in the top directory and retrieve it
        through the NodeRepository
        """
        node = Node()
        repo = NodeRepository(node, self.repository)
        data = StringIO.StringIO('Temporary content')
        tree = {
            'file_a.dat' : data
        }

        self._mock_directory_tree(node, tree)
        node.store()

        source = tree['file_a.dat'].getvalue()
        stored = repo.get_file_content('file_a.dat')

        self.assertEqual(source, stored)


    def test_store_and_retrieve_single_directory(self):
        """
        Store a single empty directory in the top directory and retrieve it
        through the NodeRepository
        """
        node = Node()
        repo = NodeRepository(node, self.repository)
        data = StringIO.StringIO('Temporary content')
        tree = {
            'dir_a' : {}
        }

        self._mock_directory_tree(node, tree)
        node.store()

        source = 'dir_a/'
        stored = repo.get_directory('dir_a')

        self.assertEqual(source, stored.path)


    def test_store_and_retrieve_tree(self):
        """
        Store a nested directory structure and retrieve both empty
        leaf directories and leaf files through the NodeRepository
        """
        node = Node()
        repo = NodeRepository(node, self.repository)
        data = StringIO.StringIO('Temporary content')
        tree = {
            'dir_a' : {
                'dir_c' : {
                    'file_a.dat' : data
                },
                'dir_d' : {}
            },
            'dir_b' : {
                'file_b.dat' : data
            }
        }

        self._mock_directory_tree(node, tree)
        node.store()

        source = tree['dir_a']['dir_c']['file_a.dat'].getvalue()
        stored = repo.get_file_content('dir_a/dir_c/file_a.dat')

        self.assertEqual(source, stored)

        source = 'dir_a/dir_d/'
        stored = repo.get_directory(source)

        self.assertEqual(source, stored.path)


    def test_ls(self):
        """
        Store a nested directory structure and test the ls method
        """
        node = Node()
        repo = NodeRepository(node, self.repository)
        data = StringIO.StringIO('Temporary content')
        tree = {
            'dir_a' : {
                'dir_c' : {
                    'file_a.dat' : data
                },
                'dir_d' : {}
            },
            'dir_b' : {
                'file_b.dat' : data
            }
        }

        self._mock_directory_tree(node, tree)
        node.store()

        result = [x.path for x in repo.ls()]
        self.assertEqual(result, [u'dir_a/', u'dir_b/'])

        result = [x.path for x in repo.ls('dir_a/')]
        self.assertEqual(result, [u'dir_a/dir_c/', u'dir_a/dir_d/'])


    def test_tree(self):
        """
        Try to print the entire tree of the node's virtual hierarchy
        """
        node = Node()
        repo = NodeRepository(node, self.repository)
        data = StringIO.StringIO('Temporary content')
        tree = {
            'dir_a' : {
                'dir_c' : {
                    'file_a.dat' : data
                },
                'dir_d' : {}
            },
            'dir_z' : {
                'file_t.dat' : data,
                'file_x.dat' : data,
                'file_a.dat' : data,
                'file_f.dat' : data,
                'dir_x' : {
                    'dir_idgaf' : {},
                    'file_a.dat' : data
                },
                'file_k.dat' : data,
            },
            'dir_b' : {
                'file_b.dat' : data
            }
        }

        self._mock_directory_tree(node, tree)
        node.store()

        repo.print_tree()


    def test_store_test(self):
        """
        Store a nested directory structure and retrieve both empty
        leaf directories and leaf files through the NodeRepository
        """
        node = Node()
        repo = NodeRepository(node, self.repository)
        data = StringIO.StringIO('Temporary content')
        tree = {
            'dir_a' : {
                'dir_b' : {
                    'dir_c' : {
                        'dir_d' : {}
                    },
                },
            },
        }

        self._mock_directory_tree(node, tree)
        node.store()

        source = 'dir_a/dir_b/'
        stored = repo.get_directory(source)

        self.assertEqual(source, stored.path)