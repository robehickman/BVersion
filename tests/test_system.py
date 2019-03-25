#from helpers import *
import shutil, os, json
from unittest import TestCase
from shttpfs.common import cpjoin, file_get_contents, file_put_contents, make_dirs_if_dont_exist

import shttpfs.client as client
import shttpfs.server as server

DATA_DIR           = os.path.dirname(__file__) + '/filesystem_tests/'
private_key = "bkUg07WLoxKcsWaupuVIyyMrVyWMdX8q8Zvta+wwKi6kmF7pCyklcIoNAOkfo1YR7O/Fb/Z0bJJ1j/lATtkKQ6c="
public_key  = "mF7pCyklcIoNAOkfo1YR7O/Fb/Z0bJJ1j/lATtkKQ6c="

############################################################################################
def setup():
    """ create test dirs with two clients and a server """
    def make_client(name):
        make_dirs_if_dont_exist(DATA_DIR + name + '/.shttpfs')
        file_put_contents(DATA_DIR +  name + '/.shttpfs/client_configuration.json', json.dumps({
            "server_domain"  : "none",
            "user"           : "test",
            "repository"     : "test",
            "private_key"    : private_key}))

    make_client('client1')
    make_client('client2')
    make_dirs_if_dont_exist(DATA_DIR + 'server')

############################################################################################
def setup_client(name):
    client.working_copy_base_path = cpath = DATA_DIR + name
    client.init()

    # Override the server connection with a test implementation that passes
    # requests directly to server code
    server.config = {
        "repositories" : {
            "test" : {
                "path" : DATA_DIR + 'server'
            }
        },
        "users" : {
            "test" : {
                "public_key" : public_key,
                "uses_repositories" : ["test"]
            }
        }
    }

    server.app.config['TESTING'] = True
    server_app = server.app.test_client()

    class test_connection(object):
        def request(self, url, headers):
            res = server_app.post(url, headers=headers, json={})
            return res.get_json(), dict(res.headers)

        def send_file(self, url, headers, fle):
            res = server_app.post(url, headers=headers, data='test', content_type = 'application/octet-stream')
            return res.get_json(), dict(res.headers)

    client.server_connection = test_connection()


class TestSystem(TestCase):
############################################################################################
    def test_system(self):
        setup()
        setup_client('client1')
        cpath = client.working_copy_base_path

        #==================================================
        # test_initial commit
        #==================================================
        test_content = 'test file'
        file_put_contents(cpath + '/test', test_content)

        # commit the files
        session_token = client.authenticate()
        version_id = client.commit(session_token, 'test commit')
        print version_id #TODO this should be vers id not none

        # file should show up in list_changes
        req_result, headers = client.server_connection.request("list_files", {
            'session_token' : session_token,
            'repository'    : 'test',
            'version_id'    : version_id })

        print 'should contain the new file'
        print req_result

        # file should exist in server fs
        self.assertEqual(test_content, file_get_contents(DATA_DIR + 'server/files/9f/86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08'))

        #==================================================
        # test update
        #==================================================
        setup_client('client2')
        cpath = client.working_copy_base_path
        client.update()

        # TODO test item shows up

        #==================================================
        # test delete and add
        #==================================================
        # TODO
        # delete a file in second repo
        # add a file to second repo
        # commit
        # update first repo, file should be deleted and new file added

        #==================================================
        # test conflict resolution
        #==================================================
        # TODO
        # make the same change on two clients
        # commit both
        # second to commit should error 
        #test resolving it


        #==================================================
        # TODO clean up test files

