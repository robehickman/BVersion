from test_common import *

import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 

from versioned_storage import *

def test_versioned_storage_init():
    empty_dir(DATA_DIR)

    s = versioned_storage(DATA_DIR, JOURNAL_FILE, JOURNAL_STEP_FILE, TMP_DIR, BACKUP_DIR)

    empty_dir(DATA_DIR)

    print "Storage init OK"


def test_versioned_storage_step():
    empty_dir(DATA_DIR)

    s = versioned_storage(DATA_DIR, JOURNAL_FILE, JOURNAL_STEP_FILE, TMP_DIR, BACKUP_DIR)
    s.step_version()

    empty_dir(DATA_DIR)

    print "Version step OK"

def test_versioned_storage_put():
    empty_dir(DATA_DIR)

    s = versioned_storage(DATA_DIR, JOURNAL_FILE, JOURNAL_STEP_FILE, TMP_DIR, BACKUP_DIR)
    s.fs_put('test.txt', 'test')

    empty_dir(DATA_DIR)

    print "Put OK"

def test_versioned_storage_put_overwrite():
    empty_dir(DATA_DIR)

    s = versioned_storage(DATA_DIR, JOURNAL_FILE, JOURNAL_STEP_FILE, TMP_DIR, BACKUP_DIR)
    s.fs_put('test.txt', 'test')
    s.fs_put('test.txt', 'test test')

    empty_dir(DATA_DIR)

    print "Put overwrite OK"

def test_versioned_storage_move():
    empty_dir(DATA_DIR)

    s = versioned_storage(DATA_DIR, JOURNAL_FILE, JOURNAL_STEP_FILE, TMP_DIR, BACKUP_DIR)
    s.fs_put('test.txt', 'test')
    s.fs_move('test.txt', 'test2.txt')

    #version should auto step, test should be saved in the parent revision

    empty_dir(DATA_DIR)

    print "Move OK"

def test_versioned_storage_move_overwrite():
    empty_dir(DATA_DIR)

    s = versioned_storage(DATA_DIR, JOURNAL_FILE, JOURNAL_STEP_FILE, TMP_DIR, BACKUP_DIR)
    s.fs_put('test.txt', 'test')
    s.fs_put('test2.txt', 'test test')
    s.fs_move('test.txt', 'test2.txt')

    #version should auto step, test 2 should be saved in the parent revision

    empty_dir(DATA_DIR)

    print "Move OK"


def test_versioned_storage_delete():
    empty_dir(DATA_DIR)

    s = versioned_storage(DATA_DIR, JOURNAL_FILE, JOURNAL_STEP_FILE, TMP_DIR, BACKUP_DIR)
    s.fs_put('test.txt', 'test')
    s.fs_delete('test.txt')

    #version should auto step, test should be saved in the parent revision, head should be empty

    empty_dir(DATA_DIR)

    print "Move OK"


test_versioned_storage_init()
test_versioned_storage_step()
test_versioned_storage_put()
test_versioned_storage_put_overwrite()
test_versioned_storage_move()
test_versioned_storage_move_overwrite()
test_versioned_storage_delete()

