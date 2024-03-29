import os
from unittest import TestCase
from tests.helpers import DATA_DIR, make_data_dir, delete_data_dir
from bversion.common import cpjoin

from bversion.storage.client_db import client_db
from bversion.storage.journaling_filesystem import journaling_filesystem

CONF_DIR   = 'bvn'
BACKUP_DIR = 'back'


class TestJournalingFilesystem(TestCase):
############################################################################################
    def setUp(self):
        delete_data_dir() # Ensure clean start
        make_data_dir()
        os.makedirs(cpjoin(DATA_DIR, CONF_DIR))

############################################################################################
    def tearDown(self):
        delete_data_dir()

############################################################################################
    def test_journaling_filesystem_put_rollback(self):
        """ Test that file put rolls back correctly """

        cdb = client_db(cpjoin(DATA_DIR, CONF_DIR, 'manifest.db'))
        s = journaling_filesystem(cdb, DATA_DIR, CONF_DIR)
        s.begin()
        s.file_put_contents('hello', b'test content')
        s.rollback()

        self.assertFalse( os.path.isfile(cpjoin(DATA_DIR, 'hello')),
                          msg = 'File "hello" still exists, put rollback failed')

        self.assertTrue( os.path.isfile(cpjoin(DATA_DIR, CONF_DIR, BACKUP_DIR, '1_hello')),
                         msg = 'Backup file "1_hello" does not exist, put rollback failed')


############################################################################################
    def test_journaling_filesystem_move_rollback(self):
        """ Test file move rolls back correctly """

        cdb = client_db(cpjoin(DATA_DIR, CONF_DIR, 'manifest.db'))
        s = journaling_filesystem(cdb, DATA_DIR, CONF_DIR)
        s.begin()
        s.file_put_contents('hello', b'test content')
        s.commit()

        s.begin()
        s.move_file('hello', 'hello2')
        s.rollback()

        self.assertFalse( os.path.isfile(cpjoin(DATA_DIR, 'hello2')),
                          msg = 'File "hello2" still exists, move rollback failed')


############################################################################################
    def test_journaling_filesystem_move_overwrite_rollback(self):
        """ Test file move rolls back correctly when move overwrites another file """

        cdb = client_db(cpjoin(DATA_DIR, CONF_DIR, 'manifest.db'))
        s = journaling_filesystem(cdb, DATA_DIR, CONF_DIR)
        s.begin()
        s.file_put_contents('hello', b'test content')
        s.file_put_contents('hello2', b'test content 2')
        s.commit()

        s.begin()
        s.move_file('hello', 'hello2')
        s.rollback()

        self.assertTrue( os.path.isfile(cpjoin(DATA_DIR, 'hello')),
                         msg = 'File "hello" does not exist, move overwrite rollback failed')

        self.assertTrue( os.path.isfile(cpjoin(DATA_DIR, 'hello2')),
                         msg = 'File "hello2" does not exist, move overwrite rollback failed')


############################################################################################
    def test_journaling_filesystem_delete_rollback(self):
        """ Test file delete rolls back correctly """

        cdb = client_db(cpjoin(DATA_DIR, CONF_DIR, 'manifest.db'))
        s = journaling_filesystem(cdb, DATA_DIR, CONF_DIR)

        s.begin()
        s.file_put_contents('hello', b'test content')
        s.commit()

        s.begin()
        s.delete_file('hello')
        s.rollback()

        self.assertTrue( os.path.isfile(cpjoin(DATA_DIR, 'hello')),
                         msg = 'error, file "hello" does not exist, delete rollback failed')


############################################################################################
    def test_journaling_filesystem_multiple_rollback(self):
        """ Test rollback of multiple things at once """

        cdb = client_db(cpjoin(DATA_DIR, CONF_DIR, 'manifest.db'))
        s = journaling_filesystem(cdb, DATA_DIR, CONF_DIR)
        s.begin()
        s.file_put_contents('hello', b'test content')
        s.commit()

        s.begin()
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
