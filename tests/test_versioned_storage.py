from helpers import *
from unittest import TestCase
from shttpfs.versioned_storage import *
from shttpfs.common import *
from pprint import pprint

CONF_DIR   = 'shttpfs'
BACKUP_DIR = 'back'

MANIFEST_FILE      = 'manifest_xzf.json'

class TestVersionedStorage(TestCase):
############################################################################################
    def test_get_changes_since(self):
        make_data_dir()

        file_put_contents(cpjoin(DATA_DIR, 'test 1'), 'test')
        file_put_contents(cpjoin(DATA_DIR, 'test 2'), 'test 1')
        file_put_contents(cpjoin(DATA_DIR, 'test 3'), 'test 2')

        #==================
        data_store = versioned_storage(DATA_DIR)
        data_store.begin()
        data_store.fs_put_from_file(cpjoin(DATA_DIR, 'test 1'), {'path' : '/test/path'})
        id1 = data_store.commit('test msg', 'test user')

        changes = data_store.get_changes_since('root', data_store.get_head())

        self.assertEqual(changes, {u'/test/path': {u'hash': u'9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08',
                                                   u'path': u'/test/path',
                                                   u'status': u'new'}})


        #==================
        data_store.begin()
        data_store.fs_put_from_file(cpjoin(DATA_DIR, 'test 2'), {'path' : '/another/path'})
        data_store.fs_put_from_file(cpjoin(DATA_DIR, 'test 3'), {'path' : '/yet/another/path'})
        id2 = data_store.commit('test msg', 'test user')

        changes = data_store.get_changes_since(id1, data_store.get_head())

        self.assertEqual(changes, {u'/another/path':     {u'hash': u'f67213b122a5d442d2b93bda8cc45c564a70ec5d2a4e0e95bb585cf199869c98',
                                                          u'path': u'/another/path',
                                                          u'status': u'new'},
                                   u'/yet/another/path': {u'hash': u'dec2e4bc4992314a9c9a51bbd859e1b081b74178818c53c19d18d6f761f5d804',
                                                          u'path': u'/yet/another/path',
                                                          u'status': u'new'}})
        delete_data_dir()


############################################################################################
    def test_rollback(self):
        make_data_dir()

        file_put_contents(cpjoin(DATA_DIR, 'test 1'), 'test')
        file_put_contents(cpjoin(DATA_DIR, 'test 2'), 'test')
        file_put_contents(cpjoin(DATA_DIR, 'test 3'), 'test 2')

        #==================
        data_store = versioned_storage(DATA_DIR)
        data_store.begin()
        data_store.fs_put_from_file(cpjoin(DATA_DIR, 'test 1'), {'path' : '/test/path'})
        id1 = data_store.commit('test msg', 'test user')

        data_store.begin()
        data_store.fs_put_from_file(cpjoin(DATA_DIR, 'test 2'), {'path' : '/another/path'})
        data_store.fs_put_from_file(cpjoin(DATA_DIR, 'test 3'), {'path' : '/yet/another/path'})
        id2 = data_store.rollback()

        self.assertEqual(os.listdir(cpjoin(DATA_DIR, 'files')), ['9f'])
        self.assertEqual(os.listdir(cpjoin(DATA_DIR, 'files', '9f')), ['86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08'])

        delete_data_dir()
