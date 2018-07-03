from helpers import *
from shttpfs.common import *
from unittest import TestCase
import subprocess

def get_state(path, last_mod):
    return {'path'     : path, 'last_mod' : last_mod}

#===============================================================================
class TestCommon(TestCase):
    def test_hash_file(self):
        """ Test that file hash returns the correct result. """

        make_data_dir()

        file_path = cpjoin(DATA_DIR, 'test')
        file_put_contents(file_path, 'some file contents')

        p1 = subprocess.Popen (['sha256sum', file_path], stdout=subprocess.PIPE)
        result1= p1.communicate()[0].split(' ')[0]
        result2 = hash_file(file_path)

        self.assertEqual(result1, result2,
            msg = 'Hashes are not the same')

        delete_data_dir()

#===============================================================================
    def test_find_manifest_changes(self):
        def to_dict(lst): return {f['path'] : f for f in lst}
        state_1 = []
        state_2 = [get_state('/file_1', 10)]
        state_3 = [get_state('/file_1', 20)]
        state_4 = [get_state('/file_1', 20), get_state('/file_2', 10)]
        state_5 = [get_state('/file_2', 20)]

        # Do some diffs
        diff_2 = find_manifest_changes(state_2, to_dict(state_1))
        diff_3 = find_manifest_changes(state_3, to_dict(state_2))
        diff_4 = find_manifest_changes(state_4, to_dict(state_3))
        diff_5 = find_manifest_changes(state_5, to_dict(state_4))

        self.assertEqual(diff_2, {'/file_1': {'status': 'new', 'path': '/file_1', 'last_mod': 10}})
        self.assertEqual(diff_3, {'/file_1': {'status': 'changed', 'path': '/file_1', 'last_mod': 20}})
        self.assertEqual(diff_4, {'/file_2': {'status': 'new', 'path': '/file_2', 'last_mod': 10}})
        self.assertEqual(diff_5, {'/file_2': {'status': 'changed', 'path': '/file_2', 'last_mod': 20},
                                  '/file_1': {'status': 'deleted', 'path': '/file_1', 'last_mod': 20}})

