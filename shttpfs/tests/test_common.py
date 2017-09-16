from helpers import *
from shttpfs.common import *
from unittest import TestCase
import subprocess

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


