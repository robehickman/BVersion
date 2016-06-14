from test_common import *

import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 

from storage import *


############################################################################################
# Unit tests for storage class
############################################################################################
def test_storage_put_rollback():
    empty_dir(DATA_DIR)

    s = storage(DATA_DIR, JOURNAL_FILE, JOURNAL_STEP_FILE, TMP_DIR, BACKUP_DIR)
    s.begin()
    s.file_put_contents(DATA_DIR + 'hello', 'test content')
    s.rollback()

    if(os.path.isfile(p.join(DATA_DIR, 'hello'))):
        raise Exception('error, file "hello" still exists, put rollback failed')  

    if(not os.path.isfile(p.join(DATA_DIR, BACKUP_DIR, '1_hello'))):
        raise Exception('error, backup file "1_hello" does not exist, put rollback failed')  

    empty_dir(DATA_DIR)

    print "Put rollback pass"


def test_storage_move_rollback():
    empty_dir(DATA_DIR)

    s = storage(DATA_DIR, JOURNAL_FILE, JOURNAL_STEP_FILE, TMP_DIR, BACKUP_DIR)
    s.begin()
    s.file_put_contents(DATA_DIR + 'hello', 'test content')
    s.commit(True)
    s.move_file(DATA_DIR + 'hello', DATA_DIR + 'hello2')
    s.rollback()

    if(os.path.isfile(DATA_DIR + 'hello2')):
        raise Exception('error, file "hello2" still exists, move rollback failed')  

    empty_dir(DATA_DIR)

    print "Move rollback pass"


def test_storage_move_overwrite_rollback():
    empty_dir(DATA_DIR)

    s = storage(DATA_DIR, JOURNAL_FILE, JOURNAL_STEP_FILE, TMP_DIR, BACKUP_DIR)
    s.begin()
    s.file_put_contents(DATA_DIR + 'hello', 'test content')
    s.file_put_contents(DATA_DIR + 'hello2', 'test content 2')
    s.commit(True)
    s.move_file(DATA_DIR + 'hello', DATA_DIR + 'hello2')
    s.rollback()

    if(not os.path.isfile(DATA_DIR + 'hello')):
        raise Exception('error, file "hello" does not exist, move overwrite rollback failed')  

    if(not os.path.isfile(DATA_DIR + 'hello2')):
        raise Exception('error, file "hello2" does not exist, move overwrite rollback failed')  

    empty_dir(DATA_DIR)

    print "Move overwrite rollback pass"


def test_storage_delete_rollback():
    empty_dir(DATA_DIR)

    s = storage(DATA_DIR, JOURNAL_FILE, JOURNAL_STEP_FILE, TMP_DIR, BACKUP_DIR)
    s.begin()
    s.file_put_contents(DATA_DIR + 'hello', 'test content')
    s.commit(True)
    s.delete_file(DATA_DIR + 'hello')
    s.rollback()

    if(not os.path.isfile(DATA_DIR + 'hello')):
        raise Exception('error, file "hello" does not exist, delete rollback failed')  

    empty_dir(DATA_DIR)

    print "Delete rollback pass"


def test_storage_multiple_rollback():
    empty_dir(DATA_DIR)

    s = storage(DATA_DIR, JOURNAL_FILE, JOURNAL_STEP_FILE, TMP_DIR, BACKUP_DIR)
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


    if(not os.path.isfile(DATA_DIR + 'hello')):
        raise Exception('error, file "hello" does not exist, multiple rollback failed')  

    if(os.path.isfile(DATA_DIR + 'hello3')):
        raise Exception('error, file "hello3" still exists, multiple rollback failed')  

    if(os.path.isfile(DATA_DIR + 'goodbye')):
        raise Exception('error, file "goodbye" still exists, multiple rollback failed')  

    if(not os.path.isfile(p.join(DATA_DIR, BACKUP_DIR, '1_hello3'))):
        raise Exception('error, backup file "1_hello3" does not exist, multiple rollback failed')  

    if(not os.path.isfile(p.join(DATA_DIR, BACKUP_DIR, '2_hello3'))):
        raise Exception('error, backup file "2_hello3" does not exist, multiple rollback failed')  

    if(not os.path.isfile(p.join(DATA_DIR, BACKUP_DIR, '3_hello2'))):
        raise Exception('error, backup file "3_hello2" does not exist, multiple rollback failed')  

    empty_dir(DATA_DIR)

    print "Multiple rollback pass"


test_storage_put_rollback()
test_storage_move_rollback()
test_storage_move_overwrite_rollback()
test_storage_delete_rollback()
test_storage_multiple_rollback()


