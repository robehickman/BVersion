# Server configuration
import __builtin__

__builtin__.DATA_DIR       = './serv_dir/'
__builtin__.MANIFEST_FILE  = '.manifest_xzf.json'
PUBLIC_KEY_FILE            = './keys/public.key'

#########################################################
# Imports
#########################################################
import json
import os
from os import path
from flask import Flask, request, redirect, url_for, send_from_directory
from werkzeug.utils import secure_filename
from base64 import b64encode, b64decode

import sys

sys.path = [ './lib' ] + sys.path


from versioned_storage import *
from common import *
from crypto import *


#########################################################
# Flask init
#########################################################
app = Flask(__name__)
app.debug = True
app.config['UPLOAD_FOLDER'] = DATA_DIR


public_key = file_get_contents(PUBLIC_KEY_FILE)

# d_store = versioned_storage(DATA_DIR, JOURNAL_FILE, JOURNAL_STEP_FILE, TMP_DIR, BACKUP_DIR)



# because we are dealing with a physical file system we cannot allow more than one
# user access at a time or we could corrupt the file system, for example where
# the same file modified by two end points. Because of this access is locked to
# a single client. In case a client crashes in the middle of a sync, the lock will
#time out automatically.
session_lock = 0

# Current Session ID
session_id     = None

issued_tokens = {}

def gc_tokens():
    global issued_tokens
    filtered_dict = {}
    for key, value in issued_tokens.iteritems():
        cur_time = time.time()
        filtered_dict = {}
        if value > cur_time: # keys which are still valid
            filtered_dict[key] = value

    issued_tokens = filtered_dict



#########################################################
# Request authentication token to sign
#########################################################
@app.route('/begin_auth', methods=['POST'])
def begin_auth():
    global issued_tokens

    gc_tokens()

    if int(time.time()) > session_lock:

        auth_token = random_bytes(35)
        issued_tokens[auth_token] = time.time() + (60 * 5)

        return json.dumps({
            'status'     : 'ok',
            'session_id' : auth_token})

    print 'locked'
    return json.dumps({'status' : 'fail'})

#########################################################
# Authenticate
#########################################################
@app.route('/authenticate', methods=['POST'])
def authenticate():
    global session_lock
    global issued_tokens

    error_not_in_dict(request.form, 'auth_token', 'Signed authentication token missing')

    auth_token = b64decode(request.form['auth_token'])

    valid = False
    if varify_signiture(public_key, auth_token):
        print 'signiture ok'
        for key, value in issued_tokens.iteritems():
            print key
            print auth_token
            if auth_token.find(key):
                del issued_tokens[key]
                valid = True
                break

    if valid == True:
        session_lock = int(time.time()) + (60 * 10)

        result = json.dumps({
            'status' : 'ok'})

    else:
        result = json.dumps({'status' : 'fail'})

    gc_tokens()
    print result
    return result


#########################################################
# Get Manifest
#########################################################
@app.route('/get_manifest', methods=['POST'])
def get_manifest():
    manifest = read_server_manifest()
    return json.dumps(manifest)

#########################################################
# Find what files have changed on a client
#########################################################
"""
-- Client push

For each file:

- File does not exist on the server
-- Tell client to push the file

- File does exist on the server
-- Has file been modified on the server or client?
--- File has not been modified on the server
---- Tell client to push the file
--- File has been modified on the server
---- Notify client of the conflict and do nothing
--- File is in client 'pull ignore', so already exists on server, but a same-name file was created  
---- ?
- Client requesting file delete
-- ?


-- Client pull

For each file:

- File does not exist on the client
-- Is it matched by a rule in client ignore?
--- File does match
---- Do nothing
--- File does not match
---- Save the file

- File does exist on the client
-- File has not been modified since prior pull
--- Update file on the client
-- File has been modified since prior pull
--- Notify client of the collision

- File has been deleted on the server
-- ?


"""

@app.route('/find_changed', methods=['POST'])
def find_changed():
    # Need to lock to stop concurrent access, which could cause data corruption
    if d_store.is_locked():
        return 'Currently locked'

    d_store.lock()


# Validate passed data
    error_not_in_dict(request.form, 'client_manifest', 'client manifest missing')
    error_not_in_dict(request.form, 'prev_manifest',   'previous manifest missing')
    error_not_in_dict(request.form, 'new_files',       'new files list missing')
    error_not_in_dict(request.form, 'changed_files',   'new files list missing')
    error_not_in_dict(request.form, 'deleted_files',   'deleted files list missing')

    manifest      = read_server_manifest()

    remote_deleated_files = json.loads(request.form['deleted_files'])
    remote_deleated_dict = make_dict(remote_deleated_files)

# handle delete files
    filter_manifest = []
    for itm in manifest['files']:
        if itm['path'] in remote_deleated_dict:
            try:
                os.remove(DATA_DIR + itm['path']) # file has been deleted from remote, remove it locally
            except OSError:
                pass # file does not exist locally anyway
        else:
            filter_manifest.append(itm)

    manifest['files'] = filter_manifest

    write_manifest(manifest)



# Compare current and previous manifests for changes, looking
# for files which have changed on the server in the time between
# when the client last synced, and the current sync request
# The server manifest should always reflect the current state
# of the server FS as this should never be changed manually
    prev_manifest = json.loads(request.form['prev_manifest'])

    prev_manifest_dict = make_dict(prev_manifest['files'])

    client_manifest = json.loads(request.form['client_manifest'])
    client_manifest_dict = make_dict(client_manifest['files'])

    server_new_files = []
    server_modified_files = []
    server_deleated_files = []
    for itm in manifest['files']:
        if itm['path'] in prev_manifest_dict:
            d_itm = prev_manifest_dict.pop(itm['path'])
            if itm['last_mod'] == d_itm['last_mod']:
                if itm['path'] in client_manifest_dict:
                    pass # file has not changed and already exists on client
                else:
                    server_new_files.append(itm['path'])# File does not exist on client
                
            else:
                server_modified_files.append(itm) # file has changed
        else:
            print 'unknown file: ' + itm['path']

#TODO need to test if detecting files which are new/modified on the server is working

# any files remaining in the remote manifest have been deleted locally
    for key, value in prev_manifest_dict.iteritems():
        server_deleated_files.append(value)

# File arrays

    push_files     = [] # list of files which client needs to push to server
    pull_files     = [] # list of files which client needs to pull from the server
    conflict_files = [] # list of files which have been edited on both ends

    for itm in server_new_files:
        pull_files.append(itm)

# handle new files from client, check if any new files conflict with existing ones.
# If there is a conflict notify the client.

    remote_new_files = json.loads(request.form['new_files'])
    manifest_dict = make_dict(manifest['files'])

    for itm in remote_new_files:
        if itm['path'] in manifest_dict:
            conflict_files.append(itm['path'])
        else :
            push_files.append(itm['path'])
            

# handle files which have changed on the client.
    # has any file also been changed on the server?
        #if yes, notify client of the conflict
        #if no, tell client to do upload

    remote_changed_files = json.loads(request.form['changed_files'])

    server_modified_dict = make_dict(server_modified_files)

    for itm in remote_changed_files:
        if itm['path'] in server_modified_dict:
            server_modified_dict.pop(itm['path'])
            conflict_files.append(itm['path'])
        else:
            push_files.append(itm['path'])


    return json.dumps({
        'status'          : 'ok',
        'remote_manifest' : manifest,
        'push_files'      : push_files,
        'pull_files'      : pull_files,
        'conflict_files'  : conflict_files
    })


#########################################################
# Push a file to the server
#########################################################
@app.route('/push_file', methods=['POST'])
def push_file():
    #try:
    validate_request(request)

    # need to make sure remote path is clean

    path = request.form['path']
    file = request.files['file']

    last_change = versioned_storage.fs_save_upload(file, path)

    return json.dumps({
        'status'          : 'ok',
        'last_change'     : last_change
    })

"""
    except:
        return json.dumps({
            'status'          : 'fail',
        })
"""


#########################################################
# Get a file from the server
#########################################################
@app.route('/pull_file', methods=['POST'])
def pull_file():
    if 'path' not in request.form:
        e = 'file list does not exist'
        print e
        raise Exception(e)

    path = versioned_storage.get_file_path(request.form['path'])

    l_dir =  os.path.dirname(path)
    file  =  os.path.basename(path)

    return send_from_directory(l_dir, file)

#########################################################
# Delete a file on the server
#########################################################
@app.route('/delete_file', methods=['POST'])
def delete_file():
    #versioned_storage.fs_delete()
    pass


if __name__ == "__main__":
    app.run()
