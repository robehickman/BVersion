import os
from unittest import TestCase
from tests.helpers import DATA_DIR, make_data_dir, delete_data_dir
from shttpfs3.common import cpjoin

from shttpfs3.storage import storage

CONF_DIR   = 'shttpfs'
BACKUP_DIR = 'back'

MANIFEST_FILE      = 'manifest_xzf.json'

class TestStorage(TestCase):
############################################################################################
    def setUp(self):
        delete_data_dir() # Ensure clean start
        make_data_dir()

############################################################################################
    def tearDown(self):
        delete_data_dir()

############################################################################################
    def test_storage_put_rollback(self):
        """ Test that file put rolls back correctly """

        s = storage(DATA_DIR, CONF_DIR)
        s.begin()
        s.file_put_contents('hello', b'test content')
        s.rollback()

        self.assertFalse( os.path.isfile(cpjoin(DATA_DIR, 'hello')),
                          msg = 'File "hello" still exists, put rollback failed')

        self.assertTrue( os.path.isfile(cpjoin(DATA_DIR, CONF_DIR, BACKUP_DIR, '1_hello')),
                         msg = 'Backup file "1_hello" does not exist, put rollback failed')


############################################################################################
    def test_storage_move_rollback(self):
        """ Test file move rolls back correctly """

        s = storage(DATA_DIR, CONF_DIR)
        s.begin()
        s.file_put_contents('hello', b'test content')
        s.commit(True)
        s.move_file('hello', 'hello2')
        s.rollback()

        self.assertFalse( os.path.isfile(cpjoin(DATA_DIR, 'hello2')),
                          msg = 'File "hello2" still exists, move rollback failed')


############################################################################################
    def test_storage_move_overwrite_rollback(self):
        """ Test file move rolls back correctly when move overwrites another file """

        s = storage(DATA_DIR, CONF_DIR)
        s.begin()
        s.file_put_contents('hello', b'test content')
        s.file_put_contents('hello2', b'test content 2')
        s.commit(True)
        s.move_file('hello', 'hello2')
        s.rollback()

        self.assertTrue( os.path.isfile(cpjoin(DATA_DIR, 'hello')),
                         msg = 'File "hello" does not exist, move overwrite rollback failed')

        self.assertTrue( os.path.isfile(cpjoin(DATA_DIR, 'hello2')),
                         msg = 'File "hello2" does not exist, move overwrite rollback failed')


############################################################################################
    def test_storage_delete_rollback(self):
        """ Test file delete rolls back correctly """

        s = storage(DATA_DIR, CONF_DIR)
        s.begin()
        s.file_put_contents('hello', b'test content')
        s.commit(True)
        s.delete_file('hello')
        s.rollback()

        self.assertTrue( os.path.isfile(cpjoin(DATA_DIR, 'hello')),
                         msg = 'error, file "hello" does not exist, delete rollback failed')


############################################################################################
    def test_storage_multiple_rollback(self):
        """ Test rollback of multiple things at once """

        s = storage(DATA_DIR, CONF_DIR)
        s.begin()
        s.file_put_contents('hello', b'test content')
        s.commit(True)
        s.file_put_contents('hello2', b'test content 2')
        s.file_put_contents('hello3', b'test content 3')
        s.move_file('hello', 'goodbye')
        s.move_file('hello2', 'hello3')
        s.delete_file('hello3')
        s.file_put_contents('hello3', b'something else')
        s.rollback()

        self.assertTrue( os.path.isfile(cpjoin(DATA_DIR, 'hello')),
                         msg = 'File "hello" does not exist, multiple rollback failed')

        self.assertFalse( os.path.isfile(cpjoin(DATA_DIR, 'hello3')),
                          msg = 'File "hello3" still exists, multiple rollback failed')

        self.assertFalse( os.path.isfile(cpjoin(DATA_DIR, 'goodbye')),
                          msg = 'File "goodbye" still exists, multiple rollback failed')

        self.assertTrue( os.path.isfile(cpjoin(DATA_DIR, CONF_DIR, BACKUP_DIR, '1_hello3')),
                         msg = 'Backup file "1_hello3" does not exist, multiple rollback failed')

        self.assertTrue( os.path.isfile(cpjoin(DATA_DIR, CONF_DIR, BACKUP_DIR, '2_hello3')),
                         msg = 'Backup file "2_hello3" does not exist, multiple rollback failed')

        self.assertTrue( os.path.isfile(cpjoin(DATA_DIR, CONF_DIR, BACKUP_DIR, '3_hello2')),
                         msg = 'Backup file "3_hello2" does not exist, multiple rollback failed')

