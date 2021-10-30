import os
from unittest import TestCase

from tests.helpers import DATA_DIR, make_data_dir, delete_data_dir
from shttpfs3.common import cpjoin, file_put_contents
from shttpfs3.storage.versioned_storage import versioned_storage
from shttpfs3.storage.server_db import get_server_db_instance_for_thread

CONF_DIR   = 'shttpfs'
BACKUP_DIR = 'back'

MANIFEST_FILE      = 'manifest_xzf.json'

class TestVersionedStorage(TestCase):
############################################################################################
    def setUp(self):
        delete_data_dir() # Ensure clean start
        make_data_dir()

        sdb = get_server_db_instance_for_thread(DATA_DIR, True)
        sdb.db_init()

############################################################################################
    def tearDown(self):
        delete_data_dir()

############################################################################################
    def test_get_changes_since(self):
        file_put_contents(cpjoin(DATA_DIR, 'test 1'), b'test')
        file_put_contents(cpjoin(DATA_DIR, 'test 2'), b'test 1')
        file_put_contents(cpjoin(DATA_DIR, 'test 3'), b'test 2')

        #==================
        data_store = versioned_storage(DATA_DIR)
        data_store.begin()
        data_store.fs_put_from_file(cpjoin(DATA_DIR, 'test 1'), {'path' : '/test/path'})
        id1 = data_store.commit('test msg', 'test user')

        changes = data_store.get_changes_since('root', data_store.get_head())

        self.assertEqual(changes, {'/test/path': {'hash': '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08',
                                                   'path': '/test/path',
                                                   'status': 'new'}})

        #==================
        data_store.begin()
        data_store.fs_put_from_file(cpjoin(DATA_DIR, 'test 2'), {'path' : '/another/path'})
        data_store.fs_put_from_file(cpjoin(DATA_DIR, 'test 3'), {'path' : '/yet/another/path'})
        data_store.commit('test msg', 'test user')

        changes = data_store.get_changes_since(id1, data_store.get_head())

        self.assertEqual(changes, {'/another/path':     {'hash': 'f67213b122a5d442d2b93bda8cc45c564a70ec5d2a4e0e95bb585cf199869c98',
                                                          'path': '/another/path',
                                                          'status': 'new'},
                                   '/yet/another/path': {'hash': 'dec2e4bc4992314a9c9a51bbd859e1b081b74178818c53c19d18d6f761f5d804',
                                                          'path': '/yet/another/path',
                                                          'status': 'new'}})

############################################################################################
    def test_rollback(self):
        file_put_contents(cpjoin(DATA_DIR, 'test 1'), b'test')
        file_put_contents(cpjoin(DATA_DIR, 'test 2'), b'test')
        file_put_contents(cpjoin(DATA_DIR, 'test 3'), b'test 2')

        #==================
        data_store = versioned_storage(DATA_DIR)

        data_store.begin()
        data_store.fs_put_from_file(cpjoin(DATA_DIR, 'test 1'), {'path' : '/test/path'})
        data_store.commit('test msg', 'test user')

        data_store.begin()
        data_store.fs_put_from_file(cpjoin(DATA_DIR, 'test 2'), {'path' : '/another/path'})
        data_store.fs_put_from_file(cpjoin(DATA_DIR, 'test 3'), {'path' : '/yet/another/path'})
        data_store.rollback()

        self.assertEqual(os.listdir(cpjoin(DATA_DIR, 'files')), ['9f'])
        self.assertEqual(os.listdir(cpjoin(DATA_DIR, 'files', '9f')), ['86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08'])
