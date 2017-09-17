from helpers import *
from shttpfs.common import *
from unittest import TestCase
import subprocess
from pprint import pprint

############################################################################################
class TestCommon(TestCase):
    def test_hash_file(self):
        """ Test that file hash returns the correct result. """

        file_path = cpjoin(DATA_DIR, 'test')
        file_put_contents(file_path, 'some file contents')

        p1 = subprocess.Popen (['sha256sum', file_path], stdout=subprocess.PIPE)
        result1= p1.communicate()[0].split(' ')[0]
        result2 = hash_file(file_path)

        self.assertEqual(result1, result2,
            msg = 'Hashes are not the same')

    def test_apply_diffs_new_to_empty(self):
        manifest = []
        diff = [{'path'   : '/file1',
                 'status' : 'new'}]

        result = apply_diffs([diff], manifest)
        self.assertEqual(result, [{'path'   : '/file1'}])

    def test_apply_diffs_new_not_empty(self):
        manifest = [{'path' : '/file1'}]
        diff = [{'path'   : '/file2',
                 'status' : 'new'}]

        result = apply_diffs([diff], manifest)
        self.assertEqual(result, [{'path'   : '/file1'}, {'path'   : '/file2'}])

    def test_apply_diffs_changed(self):
        manifest = [{'path' : '/file1'}]
        diff = [{'path'   : '/file1',
                 'new'    : True,
                 'status' : 'changed'}]

        result = apply_diffs([diff], manifest)
        self.assertEqual(result, [{'path'   : '/file1', 'new' : True}])

    def test_apply_diffs_moved(self):
        manifest = [{'path' : '/file1'}]
        diff = [{'path'       : '/file2',
                 'moved_from' : '/file1',
                 'status' : 'moved'}]

        result = apply_diffs([diff], manifest)
        self.assertEqual(result, [{'path'   : '/file2', 'moved_from' : '/file1'}])

    def test_apply_diffs_deleted(self):
        manifest = [{'path' : '/file1'}]
        diff = [{'path'   : '/file1',
                 'status' : 'deleted'}]

        result = apply_diffs([diff], manifest)
        self.assertEqual(result, [])
