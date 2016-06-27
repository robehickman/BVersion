#Client configuration
import __builtin__

__builtin__.SERVER_URL           = 'http://localhost:8080/'
__builtin__.REPO                 = 'test1'
__builtin__.DATA_DIR             = './client_dir/' # dirs must include trailing slash
__builtin__.MANIFEST_FILE        = '.manifest_xzf.json'
__builtin__.REMOTE_MANIFEST_FILE = '.remote_manifest_xzf.json'
__builtin__.IGNORE_FILTER_FILE   = '.pysync_ignore'
__builtin__.PULL_IGNORE_FILE     = '.pysync_pull_ignore'
__builtin__.PRIVATE_KEY_FILE     = 'keys/private.key'



# need to implement a 'local delete' command, delete files
# locally, remove from manifest and add to pull ignore

#########################################################
# Imports
#########################################################
import json
from poster.encode import multipart_encode
from poster.streaminghttp import register_openers
import urllib2
from base64 import b64encode, b64decode
import sys

sys.path = [ './lib' ] + sys.path

from common import *
from crypto import *

# Register the streaming http handlers with urllib2
register_openers()

private_key = ''

session_id = None

#########################################################
# Request an authentication token from the server, sign
# it with the local private key, then send to server
# for verification.
#########################################################
def authenticate_client():
    global session_id

    try:
        prior_token = file_get_contents('.prior_token')
    except:
        prior_token = ''

    result = do_request("begin_auth", {
        'prior_token' : b64encode(prior_token),
        'repository' : REPO})

    result = json.loads(result)

    if(result['status'] != 'ok'):
        raise Exception('Count not get auth token from server, the server may be locked.')


    error_not_in_dict(result, 'session_id', 'Server did not return session id')
    tmp_session_id = result['session_id']

    auth_token = sign_data(private_key, str(tmp_session_id))

    result2 = do_request("authenticate", {
        'repository' : REPO,
        'auth_token' : b64encode(auth_token)})

    result2 = json.loads(result2)

    if(result2['status'] != 'ok'):
        raise Exception('Authentication failed')

    print 'auth ok'
    session_id = tmp_session_id
    file_put_contents('.prior_token', session_id)


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
def sync_files(client_files):
    # Get previous server manifest
    remote_manifest = read_remote_manifest()

    #send list to server, which will return changes
    result = do_request("find_changed", {
        'repository'      : REPO,
        "session_id"      : session_id,
        "prev_manifest"   : json.dumps(remote_manifest),
        "client_files"    : json.dumps(client_files)})

    result = json.loads(result)

    if(result['status'] == 'ok'):
        """
        if deleated_files != []:
            deleted_dict = make_dict(deleated_files)
            manifest = read_manifest()
            filter_manifest = []
            for f in manifest['files']:
                if f['path'] in deleted_dict:
                    pass # file is deleted, remove it from the manifest
                else:
                    filter_manifest.append(f)
            manifest['files'] = filter_manifest
            write_manifest(manifest) 
        """
        
    #see if there is anything that needs pulling or pushing
        errors = []

        if result['push_files'] == [] and result['pull_files'] == []:
            print 'Nothing to do'

        # Push files
        for fle in result['push_files']:
            print fle

            print 'Sending: ' + fle['path']

            req_result = do_request("push_file", {
                'repository'  : REPO,
                "session_id"  : session_id,
                "file"        : open(cpjoin(DATA_DIR, fle['path']), "rb"), 'path' : fle['path']})

            responce = json.loads(req_result)
            if responce['status'] == 'ok':
                last_change = responce['last_change']

                print 'Uploaded: ' + last_change['path']

                # update local and remote manifest after every upload to not re-upload files
                # if the system fails mid-sync

                manifest = read_manifest()
                manifest['files'].append(get_single_file_info(
                    DATA_DIR + last_change['path'], last_change['path']))
                write_manifest(manifest)

                remote_manifest = read_remote_manifest()
                remote_manifest['files'].append(last_change)
                write_remote_manifest(remote_manifest)
            else:
                errors.append(responce['last_path'])

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
                    
        # Get files
        for fle in result['pull_files']:
            path = DATA_DIR + fle['path']
            print 'Pulling file: ' + path

            req_result = do_request("pull_file", {
                'repository'  : REPO,
                "session_id"  : session_id,
                'path'        : fle['path']})

            try:
                os.makedirs(os.path.dirname(path))
            except:
                pass # dir already exists
            
            file_put_contents(path, req_result)

            manifest = read_manifest()
            manifest['files'].append(get_single_file_info(path, fle['path']))
            write_manifest(manifest)

            print 'Done' 

    """
    # write manifest
    manifest = read_manifest()
    f_list = get_file_list(DATA_DIR)
    manifest['files'] = f_list
    write_manifest(manifest)

    write_remote_manifest(result['remote_manifest'])
    """

    return errors

#########################################################
#########################################################

encrypted_private = file_get_contents(PRIVATE_KEY_FILE)

try:
    private_key = decrypt_private(prompt_for_password(), encrypted_private)
except nacl.exceptions.CryptoError:
    print 'Password error'
    quit()

authenticate_client()

if session_id == None:
    raise Exception('Authentication failed')

manifest = read_manifest()

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

sync_files(client_files)



