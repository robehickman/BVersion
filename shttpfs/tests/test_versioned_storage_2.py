from helpers import *
from unittest import TestCase

from shttpfs.versioned_storage_2 import *

class TestVersionedStorage2(TestCase):
############################################################################################
    def test_versioned_storage_2_init(self):
        return
        empty_dir(DATA_DIR)

        s = versioned_storage_2(DATA_DIR, '', MANIFEST_FILE)

        if not os.path.isdir(p.join(DATA_DIR, 'versions')):
            raise Exception('error, versions directory was not created')  

        empty_dir(DATA_DIR)


############################################################################################
    def test_versioned_storage_2_begin_staging(self):
        return
        empty_dir(DATA_DIR)

        s = versioned_storage_2(DATA_DIR, '', MANIFEST_FILE)
        s.begin_staging()

        if not os.path.isdir(p.join(DATA_DIR, 'stage')):
            raise Exception('error, staging directory was not created')  

        if not os.path.isfile(p.join(DATA_DIR, 'stage', MANIFEST_FILE)):
            raise Exception('error, manifest was not created')  

        try:
            s.begin_staging()
            raise Exception('error, double staging should fail')  
        except: pass

        empty_dir(DATA_DIR)


############################################################################################
    def test_versioned_storage_2_put(self):
        return

        empty_dir(DATA_DIR)

        s = versioned_storage_2(DATA_DIR, '', MANIFEST_FILE)
        s.begin_staging()
        s.fs_put('test.txt', 'test')

        if not os.path.isfile(p.join(DATA_DIR, 'stage', 'test.txt')):
            raise Exception('error, put failed, file "test.txt" does not exist')  

        if not os.path.isfile(p.join(DATA_DIR, 'stage', MANIFEST_FILE)):
            raise Exception('error, manifest file not found')  

        try: manifest = json.loads(file_get_contents(p.join(DATA_DIR, 'stage', MANIFEST_FILE)))
        except: raise Exception('error, could not decode manifest')  

        passed = False
        for f in manifest['files']:
            if f['path'] == pfx_path('test.txt'):
                passed = True
                break

        if passed == False:
            raise Exception('error, file addition not recorded in json manifest.')  

        empty_dir(DATA_DIR)


############################################################################################
    def test_versioned_storage_2_get(self):
        return
        empty_dir(DATA_DIR)

        s = versioned_storage_2(DATA_DIR, '', MANIFEST_FILE)

        empty_dir(DATA_DIR)

############################################################################################
    def test_versioned_storage_2_move(self):
        return
        empty_dir(DATA_DIR)

        s = versioned_storage_2(DATA_DIR, '', MANIFEST_FILE)

        empty_dir(DATA_DIR)

############################################################################################
    def test_versioned_storage_2_delete(self):
        return
        empty_dir(DATA_DIR)

        s = versioned_storage_2(DATA_DIR, '', MANIFEST_FILE)

        empty_dir(DATA_DIR)

############################################################################################
    def test_versioned_storage_2_commit(self):
        return
        empty_dir(DATA_DIR)

        s = versioned_storage_2(DATA_DIR, '', MANIFEST_FILE)

        empty_dir(DATA_DIR)


