# -*- coding: utf-8 -*-
#from helpers import *
import shutil, os, json, struct, hashlib, time
from unittest import TestCase
from shttpfs.common import cpjoin, file_get_contents, file_put_contents, make_dirs_if_dont_exist

from helpers import *
import shttpfs.client as client
import shttpfs.server as server

DATA_DIR           = os.path.dirname(__file__) + '/filesystem_tests/'
private_key = "bkUg07WLoxKcsWaupuVIyyMrVyWMdX8q8Zvta+wwKi6kmF7pCyklcIoNAOkfo1YR7O/Fb/Z0bJJ1j/lATtkKQ6c="
public_key  = "mF7pCyklcIoNAOkfo1YR7O/Fb/Z0bJJ1j/lATtkKQ6c="
repo_name   = 'test_repo'

############################################################################################
def setup():
    """ create test dirs with two clients and a server """
    def make_client(name):
        make_dirs_if_dont_exist(DATA_DIR + name + '/.shttpfs')
        file_put_contents(DATA_DIR +  name + '/.shttpfs/client_configuration.json', json.dumps({
            "server_domain"  : "none",
            "user"           : "test",
            "repository"     : repo_name,
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
            repo_name : {
                "path" : DATA_DIR + 'server'
            }
        },
        "users" : {
            "test" : {
                "public_key" : public_key,
                "uses_repositories" : [repo_name]
            }
        }
    }

    server.app.config['TESTING'] = True
    server_app = server.app.test_client()

    class test_connection(object):
        def request(self, url, headers, data = None, gen=False):
            if data is None: data = {}
            res = server_app.post(url, headers=headers, json=data)
            if gen:
                def writer(path):
                    with open(path, 'wb') as f:
                        f.write(res.get_data())
                return writer, dict(res.headers)

            else:
                return res.get_data(), dict(res.headers)

        def send_file(self, url, headers, file_path):
            res = server_app.post(url, headers=headers, data=file_get_contents(file_path), content_type = 'application/octet-stream')
            return res.get_data(), dict(res.headers)

    client.server_connection = test_connection()


def get_server_file_name(content):
    object_hash = hashlib.sha256(content).hexdigest()
    return object_hash[:2] + '/' + object_hash[2:]

class TestSystem(TestCase):
############################################################################################
    def test_system(self):
        test_content_1   = 'test file jhgrtelkj'
        test_content_2   = b''.join([struct.pack('B', i) for i in range(256)]) # binary string with all byte values
        test_content_2_2 = test_content_2[::-1]
        test_content_3   = 'test content 3 sdavcxreiltlj'
        test_content_4   = 'test content 4 fsdwqtruytuyt'
        test_content_5   = 'test content 5 .,myuitouys'

        #=========
        setup()
        setup_client('client1')

        #==================================================
        # test_initial commit
        #==================================================
        file_put_contents(DATA_DIR +  'client1/test1',        test_content_1)
        file_put_contents(DATA_DIR +  'client1/test2',        test_content_2) # test with a binary blob
        #file_put_contents(DATA_DIR + u'client1/GȞƇØzǠ☸k😒♭',  test_content_2) # test unicode file name

        # commit the files
        session_token = client.authenticate()
        version_id = client.commit(session_token, 'test commit')
        self.assertNotEqual(version_id, None)

        # commit message should be in log
        req_result, headers = client.get_versions(session_token)
        self.assertEqual('test commit', json.loads(req_result)['versions'][0]['commit_message'])

        # file should show up in list_changes
        req_result, headers = client.get_files_in_version(session_token, version_id)
        self.assertTrue('/test1' in json.loads(req_result)['files'])
        self.assertTrue('/test2' in json.loads(req_result)['files'])

        # file should exist in server fs
        self.assertEqual(test_content_1, file_get_contents(DATA_DIR + 'server/files/' + get_server_file_name(test_content_1)))
        self.assertEqual(test_content_2, file_get_contents(DATA_DIR + 'server/files/' + get_server_file_name(test_content_2)))

        # NOTE As change detection is done using access timestamps, need a
        # delay between tests to make sure changes are detected correctly
        time.sleep(0.5)

        #==================================================
        # test update
        #==================================================
        setup_client('client2')
        session_token = client.authenticate()
        client.update(session_token)
        self.assertEqual(test_content_1, file_get_contents(DATA_DIR + 'client2/test1'))
        self.assertEqual(test_content_2, file_get_contents(DATA_DIR + 'client2/test2'))

        time.sleep(0.5) # See above

        #==================================================
        # test delete and add
        #==================================================
        os.unlink(DATA_DIR + 'client2/test1')
        file_put_contents(DATA_DIR + 'client2/test2', test_content_2_2) # test changing an existing file
        file_put_contents(DATA_DIR + 'client2/test3', test_content_3)
        file_put_contents(DATA_DIR + 'client2/test4', test_content_4)

        setup_client('client2')
        session_token = client.authenticate()
        version_id = client.commit(session_token, 'create and delete some files')

        # check change is reflected correctly in the commit log
        req_result, headers = client.get_changes_in_version(session_token, version_id)
        res_index = { v['path'] : v for v in json.loads(req_result)['changes']}
        self.assertEqual('deleted', res_index['/test1']['status'])
        self.assertEqual('new'    , res_index['/test2']['status'])
        self.assertEqual('new'    , res_index['/test3']['status'])
        self.assertEqual('new'    , res_index['/test4']['status'])

        # update first repo, file should be deleted and new file added
        setup_client('client1')
        session_token = client.authenticate()
        client.update(session_token)

        # Verify changes are reflected in FS
        self.assertFalse(os.path.isfile(DATA_DIR + 'client1/test1'))
        self.assertEqual(test_content_2_2, file_get_contents(DATA_DIR + 'client1/test2'))
        self.assertEqual(test_content_3,   file_get_contents(DATA_DIR + 'client1/test3'))
        self.assertEqual(test_content_4,   file_get_contents(DATA_DIR + 'client1/test4'))

        time.sleep(0.5) # See above

        #==================================================
        # setup for next test
        #==================================================
        file_put_contents(DATA_DIR +  'client1/test1',        test_content_1)
        file_put_contents(DATA_DIR +  'client1/test5',        test_content_1)
        file_put_contents(DATA_DIR +  'client1/test6',        test_content_1)

        setup_client('client1')
        client.commit(client.authenticate(), 'test setup')

        setup_client('client2')
        client.update(client.authenticate())

        time.sleep(0.5) # See above

        #==================================================
        # test conflict resolution, both to the server
        # and client version
        #==================================================
        # Delete on client, change on server resolution
        file_put_contents(DATA_DIR + 'client1/test1', test_content_5 + '11')
        os.unlink(        DATA_DIR + 'client2/test1')

        file_put_contents(DATA_DIR + 'client1/test2', test_content_5 + '00')
        os.unlink(        DATA_DIR + 'client2/test2')

        # Delete on server, change on client resolution
        os.unlink(        DATA_DIR + 'client1/test5')
        file_put_contents(DATA_DIR + 'client2/test5', test_content_5 + 'ff')

        os.unlink(        DATA_DIR + 'client1/test6')
        file_put_contents(DATA_DIR + 'client2/test6', test_content_5 + 'gg')

        # Double change resolution
        file_put_contents(DATA_DIR + 'client1/test3', test_content_5 + 'aa')
        file_put_contents(DATA_DIR + 'client2/test3', test_content_5 + 'bb')

        file_put_contents(DATA_DIR + 'client1/test4', test_content_5 + 'cc')
        file_put_contents(DATA_DIR + 'client2/test4', test_content_5 + 'dd')


        # commit both clients second to commit should error
        setup_client('client1')
        session_token = client.authenticate()
        version_id = client.commit(session_token, 'initial commit for conflict test')

        setup_client('client2')
        session_token = client.authenticate()
        try:
            version_id = client.commit(session_token, 'this should conflict')
            self.fail()
        except SystemExit: pass

        # Update should begin conflict resolution process 
        try:
            client.update(session_token, testing=True)
            self.fail()
        except SystemExit: pass

        # test server versions of conflict files downloaded correctly
        self.assertEqual(file_get_contents(DATA_DIR + 'client1/test1'), test_content_5 + '11')
        self.assertEqual(file_get_contents(DATA_DIR + 'client1/test2'), test_content_5 + '00')
        self.assertEqual(file_get_contents(DATA_DIR + 'client1/test3'), test_content_5 + 'aa')
        self.assertEqual(file_get_contents(DATA_DIR + 'client1/test4'), test_content_5 + 'cc')
        # NOTE nothing to download in delete on server case

        #test resolving it
        path = DATA_DIR + 'client2/.shttpfs/conflict_resolution.json'
        resolve = json.loads(file_get_contents(path))
        resolve_index = {v['1_path'] : v for v in resolve}

        resolve_index['/test1']['4_resolution'] = ['client']
        resolve_index['/test2']['4_resolution'] = ['server']
        resolve_index['/test3']['4_resolution'] = ['client']
        resolve_index['/test4']['4_resolution'] = ['server']
        resolve_index['/test5']['4_resolution'] = ['client']
        resolve_index['/test6']['4_resolution'] = ['server']

        file_put_contents(path, json.dumps([v for v in resolve_index.values()]))


        # perform update and test resolve as expected
        client.update(session_token)
        self.assertFalse(                          os.path.isfile(DATA_DIR + 'client2/test1'))
        self.assertEqual(test_content_5 + '00', file_get_contents(DATA_DIR + 'client2/test2'))
        self.assertEqual(test_content_5 + 'bb', file_get_contents(DATA_DIR + 'client2/test3'))
        self.assertEqual(test_content_5 + 'cc', file_get_contents(DATA_DIR + 'client2/test4'))
        self.assertEqual(test_content_5 + 'ff', file_get_contents(DATA_DIR + 'client2/test5'))
        self.assertFalse(                          os.path.isfile(DATA_DIR + 'client2/test6'))

        # This should now commit
        version_id = client.commit(session_token, 'this should be ok')
        self.assertNotEqual(None, version_id)

        req_result, headers = client.get_changes_in_version(session_token, version_id)
        res_index = { v['path'] : v for v in json.loads(req_result)['changes']}

        self.assertEqual('deleted', res_index['/test1']['status'])
        self.assertTrue('/test2' not in res_index)
        self.assertEqual('new', res_index['/test3']['status'])
        self.assertTrue('/test4' not in res_index)
        self.assertEqual('new', res_index['/test5']['status'])
        self.assertTrue('/test6' not in res_index)

        #==================================================
        delete_data_dir()

