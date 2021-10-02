# -*- coding: utf-8 -*-
#from helpers import *
import os, json, struct, hashlib, time, re
from unittest import TestCase
from io import BytesIO
from tests.helpers import DATA_DIR, delete_data_dir

from shttpfs3.common import file_get_contents, file_put_contents, make_dirs_if_dont_exist
import shttpfs3.client as client
import shttpfs3.server as server
from shttpfs3.server import Request, Responce

private_key = "bkUg07WLoxKcsWaupuVIyyMrVyWMdX8q8Zvta+wwKi6kmF7pCyklcIoNAOkfo1YR7O/Fb/Z0bJJ1j/lATtkKQ6c="
public_key  = "mF7pCyklcIoNAOkfo1YR7O/Fb/Z0bJJ1j/lATtkKQ6c="
repo_name   = 'test_repo'

############################################################################################
def setup():
    """ create test dirs with two clients and a server """
    def make_client_dirs(name):
        make_dirs_if_dont_exist(DATA_DIR + name + '/.shttpfs')
        file_put_contents(DATA_DIR +  name + '/.shttpfs/client_configuration.json', bytes(json.dumps({
            "server_domain"  : "none",
            "user"           : "test",
            "repository"     : repo_name,
            "private_key"    : private_key}),  encoding='utf8'))

    make_client_dirs('client1')
    make_client_dirs('client2')
    make_dirs_if_dont_exist(DATA_DIR + 'server')

############################################################################################
def setup_client(name):
    client.working_copy_base_path = DATA_DIR + name

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


    # Mock data reader that reads from a bytesio object instead of a socket
    class mock_reader:
        def __init__(self, reader_data):
            self.reader = BytesIO(reader_data)
            pass

        def read(self, length = None):
            if length is None:
                r = self.reader.read()

            else:
                r = self.reader.read(length)

            if r == b'': return None
            else: return r

        def read_all(self):
            r = self.read()
            if r == b'': return None
            else: return r


    # Override the server connection with a mock implementation that passes
    # requests directly to server code
    class mock_connection(object):
        def request_helper(self, url, headers, reader):
            headers_new = {}
            for k,v in headers.items():
                if isinstance(v, bytes): v = v.decode('utf8')
                headers_new[k] = v

            request = Request(remote_addr = '0.0.0.0',
                              remote_port = 80,
                              uri         = '/' + url,
                              headers     = headers_new,
                              body        = reader)

            return server.endpoint(request)

        def request(self, url, headers, data = None, gen=False):
            if data is None: data = {}
            reader_data = json.dumps(data).encode('utf8') if(isinstance(data, dict)) else data

            reader = mock_reader(reader_data)
            res = self.request_helper(url, headers, reader)

            if gen:
                def writer(path):
                    with open(res.body.path, 'rb') as sf:
                        with open(path, 'wb') as df:
                            df.write(sf.read())
                return writer, dict(res.headers)

            else:
                return res.body, dict(res.headers)

        def send_file(self, url, headers, file_path):
            reader = mock_reader(file_get_contents(file_path))
            res = self.request_helper(url, headers, reader)
            return res.body, dict(res.headers)

    client.server_connection = mock_connection()
    client.init()


def get_server_file_name(content):
    object_hash = hashlib.sha256(content).hexdigest()
    return object_hash[:2] + '/' + object_hash[2:]

class TestSystem(TestCase):
############################################################################################
    def test_system(self):
        test_content_1   = b'test file jhgrtelkj'
        test_content_2   = b''.join([struct.pack('B', i) for i in range(256)]) # binary string with all byte values
        test_content_2_2 = test_content_2[::-1]
        test_content_3   = b'test content 3 sdavcxreiltlj'
        test_content_4   = b'test content 4 fsdwqtruytuyt'
        test_content_5   = b'test content 5 .,myuitouys'

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
        req_result = client.get_versions(session_token)[0]
        self.assertEqual('test commit', json.loads(req_result)['versions'][0]['commit_message'])

        # file should show up in list_changes
        req_result = client.get_files_in_version(session_token, version_id)[0]
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
        # TODO test mid update failiure works as expected
        #==================================================

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
        req_result = client.get_changes_in_version(session_token, version_id)[0]
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
        # setup helper for next test
        #==================================================
        def clean_clients_and_commit():
            # Clean out client1
            res = os.listdir(DATA_DIR +  'client1')
            for it in res:
                if it not in ['.shttpfs']:
                    os.remove(DATA_DIR + 'client1/' + it)

            # client1 init
            file_put_contents(DATA_DIR +  'client1/test1',        test_content_1)
            file_put_contents(DATA_DIR +  'client1/test2',        test_content_1)
            file_put_contents(DATA_DIR +  'client1/test3',        test_content_1)

            setup_client('client1')
            client.update(client.authenticate())
            client.commit(client.authenticate(), 'test setup')

            # client2 init
            setup_client('client2')
            client.update(client.authenticate())

            time.sleep(0.5) # See above
 
            # -----------------------------------------------------------------
            # The next steps diliberately set up changes in both working copies
            # to cause the second client to conflict when an update is run.
            # All permutations of conflict are tested.
            # -----------------------------------------------------------------

            # new on client, new on server
            file_put_contents(DATA_DIR + 'client1/test4', test_content_5 + b'aa')
            file_put_contents(DATA_DIR + 'client2/test4', test_content_5 + b'bb')

            # Delete on client, change on server
            file_put_contents(DATA_DIR + 'client1/test1', test_content_5 + b'cc')
            os.unlink(        DATA_DIR + 'client2/test1')

            # Delete on server, change on client
            os.unlink(        DATA_DIR + 'client1/test2')
            file_put_contents(DATA_DIR + 'client2/test2', test_content_5 + b'ff')

            # Double change resolution
            file_put_contents(DATA_DIR + 'client1/test3', test_content_5 + b'aa')
            file_put_contents(DATA_DIR + 'client2/test3', test_content_5 + b'bb')

            time.sleep(0.5) # See above


        #==================================================
        # test full conflict resolution to the client
        #==================================================
        clean_clients_and_commit()

        # commit both clients second to commit should error
        setup_client('client1')
        session_token = client.authenticate()
        version_id = client.commit(session_token, 'initial commit for conflict test')

        setup_client('client2')
        session_token = client.authenticate()
        try:
            version_id = client.commit(session_token, 'should fail due to not having latest revision')
            self.fail()
        except SystemExit:
            pass

        # Update should begin conflict resolution process
        client.update(session_token, test_overrides = {'resolve_to' : 'client'})

        version_id = client.commit(session_token, 'this should succeed')

        setup_client('client1')

        client.update(session_token)

        # TODO check that client 1 now has the changes from client 2
        # and contents is correct


        #==================================================
        # test full conflict resolution to the server
        #==================================================
        clean_clients_and_commit()

        # commit both clients second to commit should error
        setup_client('client1')
        session_token = client.authenticate()
        version_id = client.commit(session_token, 'initial commit for conflict test')

        setup_client('client2')
        session_token = client.authenticate()
        try:
            version_id = client.commit(session_token, 'should fail due to not having latest revision')
            self.fail()
        except SystemExit:
            pass

        # Update should begin conflict resolution process
        client.update(session_token, test_overrides = {'resolve_to' : 'server'})

        # TODO check that server files have been downloaded to the client and contents is correct

        # This should not do anything as the client should now match the server
        version_id = client.commit(session_token, 'this should do nothing')


        #==================================================
        # test selective conflict resolution
        #==================================================
        clean_clients_and_commit()


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
            client.update(session_token, test_overrides = {'resolve_to' : 'manual'})
            self.fail()
        except SystemExit: pass

        """
        # test server versions of conflict files downloaded correctly
        self.assertEqual(file_get_contents(DATA_DIR + 'client1/test1'), test_content_5 + b'11')
        self.assertEqual(file_get_contents(DATA_DIR + 'client1/test2'), test_content_5 + b'00')
        self.assertEqual(file_get_contents(DATA_DIR + 'client1/test3'), test_content_5 + b'aa')
        self.assertEqual(file_get_contents(DATA_DIR + 'client1/test4'), test_content_5 + b'cc')
        # NOTE nothing to download in delete on server case
        """

        #test resolving it
        """
        path = DATA_DIR + 'client2/.shttpfs/conflict_resolution.json'
        resolve = json.loads(file_get_contents(path))
        resolve_index = {v['1_path'] : v for v in resolve}

        resolve_index['/test1']['4_resolution'] = ['client']
        resolve_index['/test2']['4_resolution'] = ['server']
        resolve_index['/test3']['4_resolution'] = ['client']
        resolve_index['/test4']['4_resolution'] = ['server']
        resolve_index['/test5']['4_resolution'] = ['client']
        resolve_index['/test6']['4_resolution'] = ['server']

        file_put_contents(path, json.dumps([v for v in list(resolve_index.values())]).encode('utf8'))
        """

        resolution_file = file_get_contents(DATA_DIR + 'client2/.shttpfs/conflict_resolution').decode('utf-8')

        resolutions = ['client', 'server', 'client', 'server']
        i = 0
        new_file = ''
        for line in resolution_file.split('\n'):
            if 'Resolution:' in line:
                line += ' ' + resolutions[i]
                i += 1
            new_file += line + '\n'
        resolution_file = new_file

        file_put_contents(DATA_DIR + 'client2/.shttpfs/conflict_resolution', resolution_file.encode('utf-8'))

        # perform update and test resolve as expected
        client.update(session_token)

        #self.assertTrue(                         os.path.isfile(DATA_DIR + 'client2/test1'))
        #self.assertEqual(test_content_5 + b'00', file_get_contents(DATA_DIR + 'client2/test2'))
        #self.assertEqual(test_content_5 + b'bb', file_get_contents(DATA_DIR + 'client2/test3'))
        #self.assertEqual(test_content_5 + b'cc', file_get_contents(DATA_DIR + 'client2/test4'))
        #self.assertEqual(test_content_5 + b'ff', file_get_contents(DATA_DIR + 'client2/test5'))
        #self.assertFalse(                        os.path.isfile(DATA_DIR + 'client2/test6'))

        # This should now commit
        version_id = client.commit(session_token, 'this should be ok')
        #self.assertNotEqual(None, version_id)

        #req_result = client.get_changes_in_version(session_token, version_id)[0]
        #res_index = { v['path'] : v for v in json.loads(req_result)['changes']}

        #self.assertEqual('deleted', res_index['/test1']['status'])
        #self.assertTrue('/test2' not in res_index)
        #self.assertEqual('new', res_index['/test3']['status'])
        #self.assertTrue('/test4' not in res_index)
        #self.assertEqual('new', res_index['/test5']['status'])
        #self.assertTrue('/test6' not in res_index)

        #==================================================
        delete_data_dir()

