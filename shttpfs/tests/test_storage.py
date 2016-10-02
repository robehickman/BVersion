from test_common import *
from unittest import TestCase

from shttpfs.storage import *

CONF_DIR   = 'shttpfs'
BACKUP_DIR = 'back'

class TestStorage(TestCase):
############################################################################################
    def test_storage_put_rollback(self):
        """ Test that file put rolls back correctly """

        empty_dir(DATA_DIR)

        s = storage(DATA_DIR, CONF_DIR)
        s.begin()
        s.file_put_contents(DATA_DIR + 'hello', 'test content')
        s.rollback()

        self.assertFalse( os.path.isfile(p.join(DATA_DIR, 'hello')),
            msg = 'File "hello" still exists, put rollback failed')  

        self.assertTrue( os.path.isfile(p.join(DATA_DIR, CONF_DIR, BACKUP_DIR, '1_hello')),
            msg = 'Backup file "1_hello" does not exist, put rollback failed')  

        empty_dir(DATA_DIR)


############################################################################################
    def test_storage_move_rollback(self):
        """ Test file move rolls back correctly """
        empty_dir(DATA_DIR)

        s = storage(DATA_DIR, CONF_DIR)
        s.begin()
        s.file_put_contents(DATA_DIR + 'hello', 'test content')
        s.commit(True)
        s.move_file(DATA_DIR + 'hello', DATA_DIR + 'hello2')
        s.rollback()

        self.assertFalse( os.path.isfile(DATA_DIR + 'hello2'),
            msg = 'File "hello2" still exists, move rollback failed')  

        empty_dir(DATA_DIR)


############################################################################################
    def test_storage_move_overwrite_rollback(self):
        """ Test file move rolls back correctly when move overwrites another file """

        empty_dir(DATA_DIR)

        s = storage(DATA_DIR, CONF_DIR)
        s.begin()
        s.file_put_contents(DATA_DIR + 'hello', 'test content')
        s.file_put_contents(DATA_DIR + 'hello2', 'test content 2')
        s.commit(True)
        s.move_file(DATA_DIR + 'hello', DATA_DIR + 'hello2')
        s.rollback()

        self.assertTrue( os.path.isfile(DATA_DIR + 'hello'), 
            msg = 'File "hello" does not exist, move overwrite rollback failed') 

        self.assertTrue( os.path.isfile(DATA_DIR + 'hello2'),
            msg = 'File "hello2" does not exist, move overwrite rollback failed')  

        empty_dir(DATA_DIR)


############################################################################################
    def test_storage_delete_rollback(self):
        """ Test file delete rolls back correctly """

        empty_dir(DATA_DIR)

        s = storage(DATA_DIR, CONF_DIR)
        s.begin()
        s.file_put_contents(DATA_DIR + 'hello', 'test content')
        s.commit(True)
        s.delete_file(DATA_DIR + 'hello')
        s.rollback()

        self.assertTrue( os.path.isfile(DATA_DIR + 'hello'),
            msg = 'error, file "hello" does not exist, delete rollback failed')  

        empty_dir(DATA_DIR)


############################################################################################
    def test_storage_multiple_rollback(self):
        """ Test rollback of multiple things at once """

        empty_dir(DATA_DIR)

        s = storage(DATA_DIR, CONF_DIR)
        s.begin()
        s.file_put_contents(DATA_DIR + 'hello', 'test content')
        s.commit(True)
        s.file_put_contents(DATA_DIR + 'hello2', 'test content 2')
        s.file_put_contents(DATA_DIR + 'hello3', 'test content 3')
        s.move_file(DATA_DIR + 'hello', DATA_DIR + 'goodbye')
        s.move_file(DATA_DIR + 'hello2', DATA_DIR + 'hello3')
        s.delete_file(DATA_DIR + 'hello3')
        s.file_put_contents(DATA_DIR + 'hello3', 'something else')
        s.rollback()

        self.assertTrue( os.path.isfile(DATA_DIR + 'hello'),
            msg = 'File "hello" does not exist, multiple rollback failed')  

        self.assertFalse( os.path.isfile(DATA_DIR + 'hello3'),
            msg = 'File "hello3" still exists, multiple rollback failed')  

        self.assertFalse( os.path.isfile(DATA_DIR + 'goodbye'),
            msg = 'File "goodbye" still exists, multiple rollback failed')  

        self.assertTrue(os.path.isfile(p.join(DATA_DIR, CONF_DIR, BACKUP_DIR, '1_hello3')),
            msg = 'Backup file "1_hello3" does not exist, multiple rollback failed')  

        self.assertTrue( os.path.isfile(p.join(DATA_DIR, CONF_DIR, BACKUP_DIR, '2_hello3')),
            msg = 'Backup file "2_hello3" does not exist, multiple rollback failed')  

        self.assertTrue( os.path.isfile(p.join(DATA_DIR, CONF_DIR, BACKUP_DIR, '3_hello2')),
            msg = 'Backup file "3_hello2" does not exist, multiple rollback failed')  

        empty_dir(DATA_DIR)


