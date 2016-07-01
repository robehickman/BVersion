from test_common import *

import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
parentdir = parentdir + '/lib'
sys.path.insert(0,parentdir) 

from versioned_storage import *


############################################################################################
############################################################################################
def test_versioned_storage_init():
    empty_dir(DATA_DIR)

    s = versioned_storage(DATA_DIR, '', MANIFEST_FILE)

    if not os.path.isdir(p.join(DATA_DIR, 'versions', '1')):
        raise Exception('error, data dir or revision 1 was not created')  

    if not os.path.isfile(p.join(DATA_DIR, 'head')):
        raise Exception('error, head file does not exist')  

    if int(file_get_contents(p.join(DATA_DIR, 'head'))) != 1:
        raise Exception('error, revision stored in head file does not match head (1)')  

    empty_dir(DATA_DIR)

    print "Storage init OK"

############################################################################################
############################################################################################
def test_versioned_storage_step():
    empty_dir(DATA_DIR)

    s = versioned_storage(DATA_DIR, '', MANIFEST_FILE)
    s.step_version()

    if not os.path.isdir(p.join(DATA_DIR, 'versions', '2')):
        raise Exception('error, revision 2 was not created')  

    if not os.path.isfile(p.join(DATA_DIR, 'head')):
        raise Exception('error, head file does not exist')  

    if int(file_get_contents(p.join(DATA_DIR, 'head'))) != 2:
        raise Exception('error, revision stored in head file does not match head (2)')  

    empty_dir(DATA_DIR)

    print "Version step OK"

############################################################################################
############################################################################################
def test_versioned_storage_put():
    empty_dir(DATA_DIR)

    s = versioned_storage(DATA_DIR, '', MANIFEST_FILE)
    s.fs_put('test.txt', 'test')

    if not os.path.isfile(p.join(DATA_DIR, 'versions', '1', 'test.txt')):
        raise Exception('error, put failed, file "test.txt" does not exist')  

    if not os.path.isfile(p.join(DATA_DIR, 'versions', '1', MANIFEST_FILE)):
        raise Exception('error, manifest file not found')  

    try: manifest = json.loads(file_get_contents(p.join(DATA_DIR, 'versions', '1', MANIFEST_FILE)))
    except: raise Exception('error, could not decode manifest')  

    passed = False
    for f in manifest['files']:
        if f['path'] == pfx_path('test.txt'):
            passed = True
            break

    if passed == False:
        raise Exception('error, file addition not recorded in json manifest.')  

    empty_dir(DATA_DIR)

    print "Put OK"

############################################################################################
############################################################################################
def test_versioned_storage_put_overwrite():
    empty_dir(DATA_DIR)

    s = versioned_storage(DATA_DIR, '', MANIFEST_FILE)
    s.fs_put('test.txt', 'test')
    s.fs_put('test.txt', 'test test')


    if not os.path.isfile(p.join(DATA_DIR, 'versions', '1', 'test.txt')):
        raise Exception('error, put failed, version 1 file "test.txt" does not exist')  

    if not os.path.isfile(p.join(DATA_DIR, 'versions', '1', MANIFEST_FILE)):
        raise Exception('error, version 1 manifest file not found')  


    try: manifest = json.loads(file_get_contents(p.join(DATA_DIR, 'versions', '1', MANIFEST_FILE)))
    except: raise Exception('error, could not decode manifest')  

    passed = False
    for f in manifest['files']:
        if f['path'] == pfx_path('test.txt'):
            passed = True
            break

    if passed == False:
        raise Exception('error, file addition not recorded in version 1 json manifest.')  

#-----
    if not os.path.isfile(p.join(DATA_DIR, 'versions', '2', 'test.txt')):
        raise Exception('error, put failed, version 2 file "test.txt" does not exist')  

    if not os.path.isfile(p.join(DATA_DIR, 'versions', '2', MANIFEST_FILE)):
        raise Exception('error, version 2 manifest file not found')  

    try: manifest = json.loads(file_get_contents(p.join(DATA_DIR, 'versions', '2', MANIFEST_FILE)))
    except: raise Exception('error, could not decode manifest')  

    passed = False
    for f in manifest['files']:
        if f['path'] == pfx_path('test.txt'):
            passed = True
            break

    if passed == False:
        raise Exception('error, file addition not recorded in version 2 json manifest.')  


    empty_dir(DATA_DIR)

    print "Put overwrite OK"

############################################################################################
############################################################################################
def test_versioned_storage_move():
    empty_dir(DATA_DIR)

    s = versioned_storage(DATA_DIR, '', MANIFEST_FILE)
    s.fs_put('test.txt', 'test')
    s.fs_move('test.txt', 'test2.txt')

    #version should not auto step

    if not os.path.isfile(p.join(DATA_DIR, 'versions', '1', 'test2.txt')):
        raise Exception('error, move failed, file "test2.txt" does not exist')  

    if os.path.isdir(p.join(DATA_DIR, 'versions', '2')):
        raise Exception('error, version 2 directory exists where no overwrite happened')  

    try: manifest = json.loads(file_get_contents(p.join(DATA_DIR, 'versions', '1', MANIFEST_FILE)))
    except: raise Exception('error, could not decode manifest')  


    if len(manifest['files']) > 1:
        raise Exception('Too meny items in manifest.')  

    passed = False
    for f in manifest['files']:
        if f['path'] == pfx_path('test.txt'):
            passed = True
            break

    if passed == True:
        raise Exception('error, renamed file not removed from manifest.')  


    passed = False
    for f in manifest['files']:
        if f['path'] == pfx_path('test2.txt'):
            passed = True
            break

    if passed == False:
        raise Exception('error, file rename not recorded in manifest.')  

    empty_dir(DATA_DIR)

    print "Move OK"

############################################################################################
############################################################################################
def test_versioned_storage_move_overwrite():
    empty_dir(DATA_DIR)

    s = versioned_storage(DATA_DIR, '', MANIFEST_FILE)
    s.fs_put('test.txt', 'test')
    s.fs_put('test2.txt', 'test test')
    s.fs_move('test.txt', 'test2.txt')

    #version should auto step, test 2 should be saved in the parent revision

    if not os.path.isfile(p.join(DATA_DIR, 'versions', '1', 'test2.txt')):
        raise Exception('error, version 1 does not contain overwritten version of test_2.txt')  

    try: manifest = json.loads(file_get_contents(p.join(DATA_DIR, 'versions', '1', MANIFEST_FILE)))
    except: raise Exception('error, could not decode manifest')  

    if len(manifest['files']) > 1:
        raise Exception('Too meny items in version 1 manifest.')  


## ++++
    passed = False
    for f in manifest['files']:
        if f['path'] == pfx_path('test.txt'):
            passed = True
            break

    if passed == True:
        raise Exception('error, renamed file not removed from manifest.')  

## ++++
    passed = False
    for f in manifest['files']:
        if f['path'] == pfx_path('test2.txt'):
            passed = True
            break

    if passed == False:
        raise Exception('error, file addition not recorded in version 1 json manifest.')  

# -------------------------
    if not os.path.isfile(p.join(DATA_DIR, 'versions', '2', 'test2.txt')):
        raise Exception('error, version 2does not contain renamed version of text.txt (renamed to test_2.txt)')  

    try: manifest = json.loads(file_get_contents(p.join(DATA_DIR, 'versions', '2', MANIFEST_FILE)))
    except: raise Exception('error, could not decode manifest')  


    if len(manifest['files']) > 1:
        raise Exception('Too meny items in version 2 manifest.')  

    passed = False
    for f in manifest['files']:
        if f['path'] == pfx_path('test2.txt'):
            passed = True
            break

    if passed == False:
        raise Exception('error, file addition not recorded in version 1 json manifest.')  

    empty_dir(DATA_DIR)

    print "Move overwrite OK"

############################################################################################
############################################################################################
def test_versioned_storage_delete():
    empty_dir(DATA_DIR)

    s = versioned_storage(DATA_DIR, '', MANIFEST_FILE)
    s.fs_put('test.txt', 'test')
    s.fs_delete('test.txt')

    #version should auto step, test should be saved in the parent revision, head should be empty

    if not os.path.isfile(p.join(DATA_DIR, 'versions', '1', 'test.txt')):
        raise Exception('error, version 1 does not contain deleated version of test.txt')  

    if os.path.isfile(p.join(DATA_DIR, 'versions', '2', 'test.txt')):
        raise Exception('error, version 2 still contains test.txt')  


    try: manifest = json.loads(file_get_contents(p.join(DATA_DIR, 'versions', '1', MANIFEST_FILE)))
    except: raise Exception('error, could not decode manifest')  

    if len(manifest['files']) > 1:
        raise Exception('Too meny items in version 1 manifest.')  

    passed = False
    for f in manifest['files']:
        if f['path'] == pfx_path('test.txt'):
            passed = True
            break

    if passed == False:
        raise Exception('error, file addition not recorded in version 1 json manifest.')  

## --------
    try: manifest = json.loads(file_get_contents(p.join(DATA_DIR, 'versions', '2', MANIFEST_FILE)))
    except: raise Exception('error, could not decode manifest')  

    if len(manifest['files']) > 0:
        raise Exception('Too many items in version 2 manifest, deleted file not removed.')  

    empty_dir(DATA_DIR)

    print "Delete OK"


############################################################################################
############################################################################################
def test_versioned_storage_get_info():
    empty_dir(DATA_DIR)

    s = versioned_storage(DATA_DIR, '', MANIFEST_FILE)
    s.fs_put('test.txt', 'test')
    s.get_single_file_info('test.txt', 'test.txt')


test_versioned_storage_init()
test_versioned_storage_step()
test_versioned_storage_put()
test_versioned_storage_put_overwrite()
test_versioned_storage_move()
test_versioned_storage_move_overwrite()
test_versioned_storage_delete()
test_versioned_storage_get_info()

