# Server configuration
import __builtin__

__builtin__.DATA_DIR       = './srv_dir/'
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
from merge_file_tree   import *
from common import *
from crypto import *


#########################################################
# Flask init
#########################################################
app = Flask(__name__)
app.debug = True
app.config['UPLOAD_FOLDER'] = DATA_DIR


public_key = file_get_contents(PUBLIC_KEY_FILE)

data_store = versioned_storage(DATA_DIR, MANIFEST_FILE)



# Tokens issued
issued_tokens = {}

# because we are dealing with a physical file system we cannot allow more than one
# user access at a time or we could corrupt the file system, for example where
# the same file modified by two end points. Because of this access is locked to
# a single client. In case a client crashes in the middle of a sync, the lock will
#time out automatically.
session_lock = 0

# The currently active session
active_session = None

def gc_tokens():
    global issued_tokens
    filtered_dict = {}
    for key, value in issued_tokens.iteritems():
        cur_time = time.time()
        filtered_dict = {}
        if value > cur_time: # keys which are still valid
            filtered_dict[key] = value

    issued_tokens = filtered_dict


def check_session_auth(form):
    global session_lock
    global actice_session

    error_not_in_dict(form, 'session_id', 'session id missing')

    if(actice_session != None and form['session_id'] == actice_session):
        raise Exception('Session ID does not match active session')

    if not (int(time.time()) > session_lock):
        raise Exception('Session lock has expired')


#########################################################
# Request authentication token to sign
#########################################################
@app.route('/begin_auth', methods=['POST'])
def begin_auth():
    global issued_tokens
    global session_lock

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
    session_id = None
    if varify_signiture(public_key, auth_token):
        print 'signiture ok'
        for key, value in issued_tokens.iteritems():
            if auth_token.find(key):
                session_id = key
                del issued_tokens[key]
                valid = True
                break

    if valid == True:
        session_lock = int(time.time()) + (60 * 10)

        active_session = session_id

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
    # check_session_auth(request.form)
    global data_store

    manifest = data_store.read_local_manifest()
    return json.dumps(manifest)

#########################################################
# Find what files have changed on a client
#########################################################
@app.route('/find_changed', methods=['POST'])
def find_changed():
    #check_session_auth(request.form)

# Validate passed data
    error_not_in_dict(request.form, 'prev_manifest',   'previous manifest missing')
    error_not_in_dict(request.form, 'client_files',       'new files list missing')

    client_files = json.loads(request.form['client_files'])
    server_files = {}

# Find files which have changed on the server since the last time this client synced
    manifest      = data_store.read_local_manifest()['files']
    prev_manifest = json.loads(request.form['prev_manifest'])['files']

    server_files = find_manifest_changes(manifest, prev_manifest)

# Do merge on remote and local file trees
    push_files, pull_files, conflict_files, local_delete_files, remote_delete_files \
        = merge_file_tree(client_files, server_files)

    return json.dumps({
        'status'          : 'ok',
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

    data_store.fs_save_upload(path, file)
    last_change = data_store.get_single_file_info(path, path)

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

    path = data_store.get_full_file_path(request.form['path'])

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
