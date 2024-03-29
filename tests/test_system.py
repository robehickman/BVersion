# -*- coding: utf-8 -*-
#from helpers import *
import os, json, struct, hashlib, time, shutil
from unittest import TestCase
from io import BytesIO
from tests.helpers import DATA_DIR, delete_data_dir

from bversion.common import file_get_contents, file_put_contents, make_dirs_if_dont_exist
from bversion import client
from bversion import server
from bversion.server import Request, ConnectionContext

private_key = "bkUg07WLoxKcsWaupuVIyyMrVyWMdX8q8Zvta+wwKi6kmF7pCyklcIoNAOkfo1YR7O/Fb/Z0bJJ1j/lATtkKQ6c="
public_key  = "mF7pCyklcIoNAOkfo1YR7O/Fb/Z0bJJ1j/lATtkKQ6c="
repo_name   = 'test_repo'

############################################################################################
def setup():
    """ create test dirs with two clients and a server """
    def make_client_dirs(name):
        make_dirs_if_dont_exist(DATA_DIR + name + '/.bvn')
        file_put_contents(DATA_DIR +  name + '/.bvn/client_configuration.json', bytes(json.dumps({
            "server_domain"  : "none",
            "user"           : "test",
            "repository"     : repo_name,
            "private_key"    : private_key}),  encoding='utf8'))

    make_client_dirs('client1')
    make_client_dirs('client2')
    make_dirs_if_dont_exist(DATA_DIR + 'server')

############################################################################################
def setup_client(name):

    if client.server_connection is not None:
        client.server_connection.context.shutdown_handler()

    # ========================
    os.chdir(DATA_DIR + name)

    server.init_server({
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
    })


    # Mock data reader that reads from a bytesio object instead of a socket
    class mock_reader:
        def __init__(self, reader_data):
            self.reader = BytesIO(reader_data)

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
    class mock_connection:
        def __init__(self):
            self.context = ConnectionContext()

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

            return server.endpoint(request, self.context)

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
        req_result = client.get_files_in_version(session_token, False, version_id)[0]
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
        # Test mid update failiure works as expected
        #==================================================
        file_put_contents(DATA_DIR +  'client1/test1',        test_content_2)
        file_put_contents(DATA_DIR +  'client1/test2',        test_content_3)
        file_put_contents(DATA_DIR +  'client1/test3',        test_content_4)

        setup_client('client1')
        session_token = client.authenticate()
        version_id = client.commit(session_token, 'test partial update')



        # =============================
        setup_client('client2')
        session_token = client.authenticate()
        try:
            client.update(session_token, test_overrides = {'kill_mid_update' : 1})
            self.fail()
        except: pass


        time.sleep(0.5) # See above


        # ===========================
        # We should continue and download only the files we don't already have
        # ===========================
        affected_files = client.update(session_token)

        self.assertEqual(['/test3'], affected_files['pulled_files'])


        time.sleep(0.5) # See above


        #==================================================
        # Test commit failiure resumes correctly
        #==================================================
        file_put_contents(DATA_DIR +  'client1/test1',        test_content_2)
        file_put_contents(DATA_DIR +  'client1/test2',        test_content_3)
        file_put_contents(DATA_DIR +  'client1/test3',        test_content_4)

        setup_client('client1')
        session_token = client.authenticate()

        test_override = {
            'kill_mid_commit' : 1,
            'result'          : []
        }

        try:
            version_id = client.commit(session_token, 'test partial commit', test_overrides = test_override)
            self.fail()
        except: pass

        self.assertEqual(set(test_override['result']), set(['/test2', '/test1']))


        # Manually clear lock, and check it completes as expected
        client.server_connection.context.shutdown_handler()

        # ============
        test_override = {
            'result'          : []
        }

        version_id = client.commit(session_token, 'test partial commit', test_overrides = test_override)

        self.assertEqual(set(test_override['result']), set(['/test3']))


        #==================================================
        # Test push ignore
        #==================================================
        file_put_contents(DATA_DIR +  'client1/push_ignored', test_content_4)
        file_put_contents(DATA_DIR +  'client1/.bvn_ignore', b'/push_ignored\n/.bvn_ignore')

        setup_client('client1')
        session_token = client.authenticate()
        version_id = client.commit(session_token, 'This should not commit')

        self.assertEqual(version_id, None)

        os.remove(DATA_DIR +  'client1/push_ignored')
        os.remove(DATA_DIR +  'client1/.bvn_ignore')

        time.sleep(0.5) # See above

        #==================================================
        # Test pull ignore
        #==================================================
        file_put_contents(DATA_DIR +  'client1/pull_ignored', test_content_4)

        setup_client('client1')
        session_token = client.authenticate()
        version_id = client.commit(session_token, 'add a file that will be ignored in the second client')

        # ----------
        file_put_contents(DATA_DIR +  'client2/.bvn_pull_ignore', b'/pull_ignored')

        setup_client('client2')
        session_token = client.authenticate()
        version_id = client.update(session_token)

        self.assertFalse(os.path.isfile(DATA_DIR + 'client2/pull_ignored'))

        time.sleep(0.5) # See above


        #==================================================
        # test that a file removed from pull ignore
        # is downloaded on next update
        #==================================================
        os.remove(DATA_DIR +  'client2/.bvn_pull_ignore')

        setup_client('client2')
        session_token = client.authenticate()
        version_id = client.update(session_token, include_unchanged = True)

        self.assertTrue(os.path.isfile(DATA_DIR + 'client2/pull_ignored'))

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
        req_result = client.get_changes_in_version(session_token, False, version_id)[0]
        res_index = { v['path'] : v for v in json.loads(req_result)['changes']}
        self.assertEqual('deleted' , res_index['/test1']['status'])
        self.assertEqual('changed' , res_index['/test2']['status'])
        self.assertEqual('changed' , res_index['/test3']['status'])
        self.assertEqual('new'     , res_index['/test4']['status'])

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
                if it not in ['.bvn']:
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


        #===============================================================
        # Diliberately set up changes in both working copies that are
        # in conflict. These are tested by first commiting client1,
        # and the conflict is always created in client 2 by running
        # update. All permutations of conflict are tested.
        #===============================================================
        def setup_test_case():
            # Case 1 - new on client, new on server
            file_put_contents(DATA_DIR + 'client1/test1', test_content_5 + b'aa')
            file_put_contents(DATA_DIR + 'client2/test1', test_content_5 + b'bb')

            # Case 2 - Delete on client, change on server
            file_put_contents(DATA_DIR + 'client1/test2', test_content_5 + b'cc')
            os.unlink(        DATA_DIR + 'client2/test2')

            # Case 3 - Delete on server, change on client
            os.unlink(        DATA_DIR + 'client1/test3')
            file_put_contents(DATA_DIR + 'client2/test3', test_content_5 + b'dd')

            # Case 4 - Double change resolution
            file_put_contents(DATA_DIR + 'client1/test4', test_content_5 + b'ee')
            file_put_contents(DATA_DIR + 'client2/test4', test_content_5 + b'ff')


            time.sleep(0.5) # See above

        #------------------------------------------------------
        def test_resolution_to_client(self, base_path):
            # File test1 should contain the text 'bb'
            self.assertEqual(file_get_contents(base_path + 'test1'), test_content_5 + b'bb')

            # File test2 should be deleted
            self.assertFalse(os.path.isfile(base_path + 'test2'))

            # File test3 should contain the text 'dd'
            self.assertEqual(file_get_contents(base_path + 'test3'), test_content_5 + b'dd')

            # File test4 should contain the text 'ff'
            self.assertEqual(file_get_contents(base_path + 'test4'), test_content_5 + b'ff')

        #------------------------------------------------------
        def test_resolution_to_server(self, base_path):
            # File test1 should contain the text 'aa'
            self.assertEqual(file_get_contents(base_path + 'test1'), test_content_5 + b'aa')

            # File test2 should contain the text 'cc'
            self.assertEqual(file_get_contents(base_path + 'test2'), test_content_5 + b'cc')

            # File test3 should be deleted
            self.assertFalse(os.path.isfile(base_path + 'test3'))

            # File test4 should contain the text 'ee'
            self.assertEqual(file_get_contents(base_path + 'test4'), test_content_5 + b'ee')

        #------------------------------------------------------
        def perform_selective_conflict_resolve(mode = 'full'):
            resolution_file = file_get_contents(DATA_DIR + 'client2/.bvn/conflict_resolution').decode('utf-8')

            resolutions = ['server', 'client', 'server', 'client']

            if mode == 'partial':
                resolutions = [0,2]

            i = 0
            new_file = ''
            for line in resolution_file.split('\n'):
                if 'Resolution:' in line:
                    try:
                        line += ' ' + resolutions[i]
                        i += 1
                    except:
                        pass


                new_file += line + '\n'
            resolution_file = new_file

            file_put_contents(DATA_DIR + 'client2/.bvn/conflict_resolution', resolution_file.encode('utf-8'))

        #------------------------------------------------------
        def test_selective_conflict_resolve(self, base_path):
            # File test1 should contain the text 'aa'
            self.assertEqual(file_get_contents(base_path + 'test1'), test_content_5 + b'aa')

            # File test2 should be deleted
            self.assertFalse(os.path.isfile(base_path + 'test2'))

            # File test3 should be deleted
            self.assertFalse(os.path.isfile(base_path + 'test3'))

            # File test4 should contain the text 'ff'
            self.assertEqual(file_get_contents(base_path + 'test4'), test_content_5 + b'ff')


        #==================================================
        # test full conflict resolution to the client
        #==================================================
        clean_clients_and_commit()
        setup_test_case()

        # commit both clients second to commit should error
        setup_client('client1')
        session_token = client.authenticate()
        version_id = client.commit(session_token, 'initial commit for conflict test')

        #----------------
        setup_client('client2')
        session_token = client.authenticate()
        try:
            version_id = client.commit(session_token, 'should fail due to not having latest revision')
            self.fail()
        except SystemExit:
            pass

        # redo client setup to reset lock
        setup_client('client2')

        # Update and resolve conflicting files to the client
        client.update(session_token, test_overrides = {'resolve_to' : 'client'})
        test_resolution_to_client(self, DATA_DIR + 'client2/')

        # --------------------------
        version_id = client.commit(session_token, 'this should succeed')
        self.assertNotEqual(None, version_id)

        # check that client 1 gets the changes from client 2, both should now match
        setup_client('client1')
        client.update(session_token)
        test_resolution_to_client(self, DATA_DIR + 'client1/')


        #==================================================
        # test full conflict resolution to the server
        #==================================================
        clean_clients_and_commit()
        setup_test_case()

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

        # Update and resolve conflicting files to the server
        client.update(session_token, test_overrides = {'resolve_to' : 'server', 'kill_mid_update' : 0})

        # check that server files have been downloaded to the client and contents is correct
        test_resolution_to_server(self, DATA_DIR + 'client2/')

        # This should not do anything as the client should now match the server
        version_id = client.commit(session_token, 'this should do nothing')

        setup_client('client1')
        session_token = client.authenticate()
        client.update(session_token)

        test_resolution_to_server(self, DATA_DIR + 'client1/')


        #==================================================
        # test selective conflict resolution
        #==================================================
        clean_clients_and_commit()
        setup_test_case()

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
        setup_client('client2')
        try:
            client.update(session_token, test_overrides = {'resolve_to' : 'manual'})
            self.fail()
        except SystemExit: pass

        # Test incomplete resolution file causes an error
        setup_client('client2')
        shutil.copyfile(DATA_DIR + 'client2/.bvn/conflict_resolution', DATA_DIR + 'client2/.bvn/conflict_resolution.back')
        perform_selective_conflict_resolve('partial')

        try:
            client.update(session_token)
            self.fail()
        except SystemExit:
            pass

        # Modify the conflict resolution file to resolve the conflicts
        setup_client('client2')
        shutil.copyfile(DATA_DIR + 'client2/.bvn/conflict_resolution.back', DATA_DIR + 'client2/.bvn/conflict_resolution')
        perform_selective_conflict_resolve()
        client.update(session_token)

        # Check files resolve as expected
        test_selective_conflict_resolve(self, DATA_DIR + 'client2/')

        # This should now commit
        version_id = client.commit(session_token, 'this should be ok')
        self.assertNotEqual(None, version_id)

        # Check the other working copy too
        setup_client('client1')
        session_token = client.authenticate()
        client.update(session_token)

        test_selective_conflict_resolve(self, DATA_DIR + 'client1/')

        # Test change log is correct
        req_result = client.get_changes_in_version(session_token, False, version_id)[0]
        res_index = { v['path'] : v for v in json.loads(req_result)['changes']}

        self.assertEqual('deleted', res_index['/test2']['status'])
        self.assertEqual('changed', res_index['/test4']['status'])

        #==================================================
        delete_data_dir()
