#Client configuration
import __builtin__

__builtin__.DATA_DIR             = './' # dirs must include trailing slash
__builtin__.MANIFEST_FILE        = '.manifest_xzf.json'
__builtin__.REMOTE_MANIFEST_FILE = '.remote_manifest_xzf.json'
__builtin__.IGNORE_FILTER_FILE   = '.pysync_ignore'
__builtin__.PULL_IGNORE_FILE     = '.pysync_pull_ignore'
__builtin__.PRIVATE_KEY_FILE     = 'keys/private.key'

__builtin__.CLIENT_CONF_DIR  = '.shttpfs'
__builtin__.CLIENT_CONF_FILE = 'client.cnf'



# need to implement a 'local delete' command, delete files
# locally, remove from manifest and add to pull ignore

#########################################################
# Imports
#########################################################
import json, os
from poster.encode import multipart_encode
from poster.streaminghttp import register_openers
import urllib2
from base64 import b64encode, b64decode
import sys

from lib.common import *
from lib.crypto import *
from lib.plain_storage import *

data_store = None

# Register the streaming http handlers with urllib2
register_openers()

# private_key = ''

session_id = None

#########################################################
# Request an authentication token from the server, sign
# it with the local private key, then send to server
# for verification.
#########################################################
def authenticate_client(private_key, server_url, repository_name):
    global session_id

    try:
        prior_token = file_get_contents(cpjoin(DATA_DIR, CLIENT_CONF_DIR,'prior_token'))
    except:
        prior_token = ''

    result = do_request("begin_auth", {
        'prior_token' : b64encode(prior_token),
        'repository' : repository_name})

    result = json.loads(result)

    if(result['status'] != 'ok'):
        raise Exception('Count not get auth token from server, the server may be locked.')


    error_not_in_dict(result, 'session_id', 'Server did not return session id')
    tmp_session_id = result['session_id']

    auth_token = sign_data(private_key, str(tmp_session_id))

    result2 = do_request("authenticate", {
        'repository' : repository_name,
        'auth_token' : b64encode(auth_token)})

    result2 = json.loads(result2)

    if(result2['status'] != 'ok'):
        raise Exception('Authentication failed')

    print 'auth ok'
    session_id = tmp_session_id
    file_put_contents(cpjoin(DATA_DIR, CLIENT_CONF_DIR,'prior_token'), session_id)


#########################################################
# Close connection with server.
#########################################################
def disconnect_client():
    pass


#########################################################
# Detect which, if any files have been changed locally
#########################################################
def detect_local_changes(manifest):
    # Obtain previous state from the manifest
    old_f_list = manifest['files']

    # Obtain current state of the file system
    f_list = get_file_list(DATA_DIR)

    # Check for changes since last run
    client_files = find_manifest_changes(f_list, old_f_list)

    return client_files

#########################################################
# Do actual file sync
#########################################################
def sync_files(client_files, repository_name):
    # Get previous server manifest
    remote_manifest = data_store.read_remote_manifest()

    #send list to server, which will return changes
    result = do_request("find_changed", {
        'repository'      : repository_name,
        "session_id"      : session_id,
        "prev_manifest"   : json.dumps(remote_manifest),
        "client_files"    : json.dumps(client_files)})

    result = json.loads(result)

    if(result['status'] == 'ok'):
    #see if there is anything that needs pulling or pushing
        errors = []

        hit = False

        # Push files
        if result['push_files'] != []:
            hit = True
            errors += sync_push_helper(result)
                    
        # Pull files
        if result['pull_files'] != []:
            hit = True
            errors += sync_pull_helper(result)

        if hit == False:
            print 'Nothing to do'


    return errors



#########################################################
# Send files from remote
#########################################################
def sync_push_helper(result):
    errors = []

    for fle in result['push_files']:
        print 'Sending: ' + fle['path']

        req_result = do_request("push_file", {
            'repository'  : repository_name,
            "session_id"  : session_id,
            "file"        : open(cpjoin(DATA_DIR, fle['path']), "rb"), 'path' : fle['path']})

        responce = json.loads(req_result)
        if responce['status'] == 'ok':
            last_change = responce['last_change']

            print 'Uploaded: ' + last_change['path']

            # update local and remote manifest after every upload to not re-upload files
            # if the system fails mid-sync

            data_store.begin()

            manifest = data_store.read_local_manifest()
            manifest['files'].append(get_single_file_info(
                DATA_DIR + last_change['path'], last_change['path']))
            data_store.write_local_manifest(manifest)

            remote_manifest = data_store.read_remote_manifest()
            remote_manifest['files'].append(last_change)
            data_store.write_remote_manifest(remote_manifest)

            data_store.commit()
        else:
            errors.append(responce['last_path'])

    return errors


#########################################################
# Pull files from remote
#########################################################
def sync_pull_helper(result):
    errors = []

# See if pull ignore file exists
    try:
        pull_ignore = file_get_contents(DATA_DIR + PULL_IGNORE_FILE)
        pull_ignore = pull_ignore.splitlines()

        filtered_pull_files = []
        for f in result['pull_files']:
            matched = False
            for i in pull_ignore:
                if fnmatch.fnmatch(f, i):
                    matched = True

            if matched == False:
                filtered_pull_files.append(f)

        result['pull_files'] = filtered_pull_files
    except:
        print 'Warning, pull ignore file does not exist'

# Pull the files
    for fle in result['pull_files']:
        print 'Pulling file: ' + fle['path']

        req_result, headers = do_request_full("pull_file", {
            'repository'  : repository_name,
            "session_id"  : session_id,
            'path'        : fle['path']})

        make_dirs_if_dont_exist(data_store.get_full_file_path(fle['path']))
        data_store.fs_put(fle['path'], req_result)


        # Store remote manifest data, now included in custom header
        last_change = json.loads(headers.get('file_info_json'))

        remote_manifest = data_store.read_remote_manifest()
        remote_manifest['files'].append(last_change)

        data_store.begin()
        data_store.write_remote_manifest(remote_manifest)
        data_store.commit()

        print 'Done' 

    return errors

#########################################################
#########################################################



#########################################################
# Searches for configuration file in path, and every
# parent until reaching root. Path should be absolute.
#########################################################
def find_conf_file(search_path):

    cur_path = search_path

    while True:
        conf_file_path = cpjoin(cur_path, CLIENT_CONF_DIR, CLIENT_CONF_FILE)

        # Look for config file
        if os.path.isfile(conf_file_path):
            return conf_file_path

        if cur_path == os.sep:
            return None

        split_p = cur_path.split(os.sep)
        split_p.pop()
        n_path = '/' + os.path.join(*split_p)

        cur_path = n_path


#########################################################
# Handle CLI arguments.
#########################################################

import argparse

parser = argparse.ArgumentParser(description='Simple HTTP file sync tool.')
parser.add_argument('mode', metavar='mode', type=str, nargs='?',
                   help='System mode ("setup") or nothing.')
parser.add_argument('--url', metavar='url', type=str, nargs='?',
                   help='URL for setup command.')

args = parser.parse_args()

if args.mode != None and args.mode.lower() == 'setup':
    if args.url == None:
        print 'URL is missing'
        quit()

    # split repo name from url and check format
    split = args.url.rpartition('/')

    if split[0] in ['http:/', 'https:/'] or split[2] == '':
        print 'Repo name missing, usage: "http(s)://[repo name]"'
        quit()

    if len(args.url.split('/')) != 4:
        print 'URL format wrong, usage: "http(s)://[repo name]"'
        quit()

    # get private key
    print 'Please enter the private key for this repository, then press enter.'
    encrypted_private = raw_input('> ').strip(' \t\n\r')

    if encrypted_private == '':
        print 'Key is blank, exiting.'
        quit()

    # test private key
    try:
        private_key = decrypt_private(prompt_for_password(), encrypted_private)
    except nacl.exceptions.CryptoError:
        print 'Password error'
        quit()

    # make sure server URL and repo name are formatted corectly
    server_url = split[0]

    if not server_url.endswith('/'):
        server_url += '/'

    repository_name = split[2]

    __builtin__.SERVER_URL = server_url 

    # Make sure key is correct for client
    authenticate_client(private_key, server_url, repository_name)
    if session_id == None:
        raise Exception('Authentication failed')
    disconnect_client()

    # create repo dir
    try: os.makedirs(repository_name)
    except OSError: pass

    # create config file
    try: os.makedirs(cpjoin(repository_name, CLIENT_CONF_DIR))
    except OSError: pass

    # build up conf file content
    conf_file = """[client]
server_url: """ + server_url + """
repository_name: """ + repository_name + """
private_key: """ + encrypted_private

    cur_path = os.getcwd()
    conf_file_path = cpjoin(cur_path, repository_name, CLIENT_CONF_DIR, CLIENT_CONF_FILE)
    file_put_contents(conf_file_path, conf_file)

    print 'setup ok'
    quit()



# Find the config file
# cur_path = os.getcwd()
cur_path = os.path.normpath(os.path.abspath(DATA_DIR))

config_path = find_conf_file(cur_path)

if config_path == None:
    print 'Conf not found'
    quit()

config = read_config(config_path)

encrypted_private = config['client']['private_key']
server_url        = config['client']['server_url']
repository_name   = config['client']['repository_name']

# Init data store
data_store = plain_storage(DATA_DIR, CLIENT_CONF_DIR, MANIFEST_FILE, REMOTE_MANIFEST_FILE)


__builtin__.SERVER_URL = server_url 
print server_url

# Unlock private key
try:
    private_key = decrypt_private(prompt_for_password(), encrypted_private)
except nacl.exceptions.CryptoError:
    print 'Password error'
    quit()

authenticate_client(private_key, server_url, repository_name)

if session_id == None:
    raise Exception('Authentication failed')

manifest = data_store.read_local_manifest()

client_files = detect_local_changes(manifest);

new_files     = []
changed_files = []
deleted_files = []
for itm in client_files.values():
    if itm['status'] == 'new':
        new_files.append(itm)
    if itm['status'] == 'changed':
        changed_files.append(itm)
    if itm['status'] == 'deleted':
        deleted_files.append(itm)

display_list('New: ',     new_files, 'green')
display_list('Changed: ', changed_files, 'yellow')
display_list('Deleted: ', deleted_files, 'red')

sync_files(client_files, repository_name)



