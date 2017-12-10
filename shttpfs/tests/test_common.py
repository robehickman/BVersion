from helpers import *
from shttpfs.common import *
from unittest import TestCase
import subprocess
from pprint import pprint

def move_helper(status, path, hsh):
    return {'status'   : status,
            'path'     : path,
            'created'  : '',
            'last_mod' : '',
            'hash'     : hsh}

def res_helper(status, moved_from, path, hsh):
    return {'status'    : status,
            'path'      : path,
            'moved_from': moved_from,
            'created'   : '',
            'last_mod'  : '',
            'hash'      : hsh}

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

    def test_detect_moved_files_one(self):
        file_manifest = {'files' : [{'hash' : '12345',
                                    'path' : '/test'}]}

        diff          = [move_helper('delete', '/test', '12345'),
                         move_helper('new', '/test2', '12345')]

        result = detect_moved_files(file_manifest, diff)

        self.assertEqual(result, [res_helper('moved', '/test', '/test2', '12345')])

    def test_detect_moved_files_multiple(self):
        file_manifest = {'files' : [{'hash' : '12345',
                                    'path' : '/test'},
                                    {'hash' : 'a12345',
                                    'path' : '/test2'}]}

        diff          = [move_helper('delete', '/test', '12345'),
                         move_helper('new', '/a/test', '12345'),
                         move_helper('delete', '/test2', 'a12345'),
                         move_helper('new', '/a/test2', 'a12345')]

        result = detect_moved_files(file_manifest, diff)

        self.assertEqual(result, [res_helper('moved', '/test2', '/a/test2', 'a12345'),
                                  res_helper('moved', '/test', '/a/test', '12345')])

    def test_detect_moved_files_duplicates(self):
        file_manifest = {'files' : [{'hash' : '12345',
                                    'path' : '/test'},
                                    {'hash' : '12345',
                                    'path' : '/test2'}]}

        diff          = [move_helper('delete', '/test', '12345'),
                         move_helper('new', '/a/test', '12345'),
                         move_helper('delete', '/test2', '12345'),
                         move_helper('new', '/a/test2', '12345')]

        result = detect_moved_files(file_manifest, diff)

        self.assertEqual(result, [res_helper('moved', '/test2', '/a/test2', '12345'),
                                  res_helper('moved', '/test', '/a/test', '12345')])

    def test_detect_moved_files_duplicates_with_rename(self):
        file_manifest = {'files' : [{'hash' : '12345',
                                    'path' : '/test'},
                                    {'hash' : '12345',
                                    'path' : '/test2'}]}

        diff          = [move_helper('delete', '/test', '12345'),
                         move_helper('new', '/a/test', '12345'),
                         move_helper('delete', '/test2', '12345'),
                         move_helper('new', '/a/test2n', '12345')]

        result = detect_moved_files(file_manifest, diff)

        self.assertEqual(result, [res_helper('moved', '/test', '/a/test', '12345'),
                                  res_helper('moved', '/test2', '/a/test2n', '12345')])

    def test_detect_moved_files_new_duplicate_not_moved(self):
        file_manifest = {'files' : [{'path' : '/test',
                                      'hash' : '12345'}]}

        diff          = [move_helper('new', '/a/test', '12345')]

        result = detect_moved_files(file_manifest, diff)

        self.assertEqual(result, [move_helper('new', '/a/test', '12345')])


    def test_detect_moved_files_duplicates_and_no_duplicates(self):
        file_manifest = {'files' : [{'hash' : '12345',
                                    'path' : '/test'},
                                    {'hash' : '12345',
                                    'path' : '/test2'},
                                    {'hash' : 'a12345',
                                    'path' : '/test3'},
                                    {'hash' : 'b12345',
                                    'path' : '/test4'},
                                    {'hash' : 'c12345',
                                    'path' : '/test5'}]}

        diff          = [move_helper('delete', '/test', '12345'),
                         move_helper('new', '/a/test', '12345'),
                         move_helper('delete', '/test2', '12345'),
                         move_helper('new', '/a/test2', '12345'),
                         move_helper('delete', '/test3', 'a12345'),
                         move_helper('new', '/a/test3', 'a12345'),
                         move_helper('delete', '/test4', 'b12345'),
                         move_helper('new', '/a/test4', 'b12345'),
                         move_helper('delete', '/test5', 'c12345'),
                         move_helper('new', '/a/test5n', 'c12345')]

        result = detect_moved_files(file_manifest, diff)

        self.assertEqual(result, [res_helper('moved', '/test', '/a/test', '12345'),
                                  res_helper('moved', '/test5', '/a/test5n', 'c12345'),
                                  res_helper('moved', '/test3', '/a/test3', 'a12345'),
                                  res_helper('moved', '/test2', '/a/test2', '12345'),
                                  res_helper('moved', '/test4', '/a/test4', 'b12345')])

