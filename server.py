# Server configuration
import __builtin__

__builtin__.SERVER_CONF_FILE = 'server.ini'
__builtin__.MANIFEST_FILE  = '.manifest.json'

#########################################################
# Imports
#########################################################
import json
import os
from os import path
from flask import Flask, request, redirect, url_for, send_from_directory, make_response
from werkzeug.utils import secure_filename
from base64 import b64encode, b64decode

import sys

sys.path = [ './lib' ] + sys.path


from versioned_storage import *
from merge_file_tree   import *
from common import *
from crypto import *

#########################################################
# Load server config
#########################################################
config = read_config(SERVER_CONF_FILE)

if 'global' in config:
    glb = config.pop('global')

    if 'manifest_file' in glb:
        __builtin__.MANIFEST_FILE  = glb['manifest_file']


if config == {}:
    print 'Configuration file is empty'
    quit()

repositories = {}
for key, val in config.iteritems():

    repositories[key] = {
        # The root path of the repository
        'path'          : val['repository_path'],

        # Data store object for the above path
        'data_store'    : versioned_storage(val['repository_path'], '', MANIFEST_FILE),

        # The public key for accessing this repository
        'public_key'    : val['public_key'],

        # Tokens are issued by the authentication system, the client signs one with it's
        # private key, which is verified by the server using the matching public key.
        # Dict key is the token, the second element is a validity timestamps. If timestamps
        # less than current system time, the token has expired.
        'issued_tokens' : {},

        # because we are dealing with a physical file system we cannot allow more than one
        # user to access at a time or we could corrupt the file system, for example where
        # the same file modified by two clients, conflict detection would be a race condition.
        # Because of this access is locked to a single client. This lock is a timestamps, valid
        # so long as it is more than current system time. In case a client crashes in the
        # middle of a sync, the lock will time out automatically.
        'session_lock' : 0,

        # The currently active session
        'active_session' : None
    }

#########################################################
# Flask init
#########################################################
app = Flask(__name__)
app.debug = True

#########################################################
# Obtain repository named in http request
#########################################################
def get_repository():
    global repositories
    error_not_in_dict(request.form, 'repository', 'Repository identifier is missing')

    if request.form['repository'] in repositories:
        return (request.form['repository'], repositories[request.form['repository']])
    else:
        raise Exception('Repository does not exist')

#########################################################
# Update repository dict
#########################################################
def set_repository(repo_name, repo):
    global repositories

    if repo_name not in repositories:
        raise Exception('Repository does not exist')

    repositories['repo_name'] = repo

#########################################################
# Garbage collection for expired authentication tokens
#########################################################
def gc_tokens():
    global repositories

    filtered_repos = {}
    for key, value in repositories.iteritems():
        issued_tokens = value['issued_tokens']

        filtered_toknes = {}
        for key1, value1 in issued_tokens.iteritems():
            if value1 > time.time(): # keys which are still valid
                filtered_toknes[key1] = value1

        new_value = value.copy()
        new_value['issued_tokens'] = filtered_toknes
        filtered_repos[key] = new_value

    repositories = filtered_repos

#########################################################
# Check a session is valid
#########################################################
def check_session_auth(form):
    repo_name, repo = get_repository()

    error_not_in_dict(form, 'session_id', 'session id missing')

    if(repo['active_session'] != None and form['session_id'] != repo['active_session']):
        raise Exception('Session ID does not match active session')

    if (int(time.time()) > repo['session_lock']):
        raise Exception('Session lock has expired')


#########################################################
# Extend session auth into the future
#########################################################
def extend_session_auth():
    repo_name, repo = get_repository()

    repo['session_lock'] = int(time.time()) + 600 # 30 second validity

    set_repository(repo_name, repo)

#########################################################
# Request authentication token to sign
#########################################################
@app.route('/begin_auth', methods=['POST'])
def begin_auth():
    gc_tokens()

    repo_name, repo = get_repository()

# Client sends its past authentication token in order to allow the client
# that locked the server to unlock it again.
    prior_token = b64decode(request.form['prior_token'])

    if prior_token == repo['active_session']:
        repo['session_lock'] = 0
        set_repository(repo_name, repo)


# Issue a new token if the server is not locked
    if int(time.time()) > repo['session_lock']:
        auth_token = random_bytes(35)
        repo['issued_tokens'][auth_token] = time.time() + (60 * 5) # 5 min token validity

        set_repository(repo_name, repo)

        return json.dumps({
            'status'     : 'ok',
            'session_id' : auth_token})

    print 'locked'
    return json.dumps({'status' : 'fail', 'msg' : 'The repository is locked.'})

#########################################################
# Authenticate
#########################################################
@app.route('/authenticate', methods=['POST'])
def authenticate():
    repo_name, repo = get_repository()

    error_not_in_dict(request.form, 'auth_token', 'Signed authentication token missing')

    auth_token = b64decode(request.form['auth_token'])

    public_key = repo['public_key']

    valid = False
    session_id = None
    if varify_signiture(public_key, auth_token):
        print 'signiture ok'
        for key, value in repo['issued_tokens'].iteritems():
            if auth_token.find(key):
                session_id = key
                del repo['issued_tokens'][key]
                valid = True
                break

    if valid == True:
        repo['session_lock'] = int(time.time()) + 600 # 30 second validity
        repo['active_session'] = session_id

        result = json.dumps({
            'status' : 'ok'})

    else:
        result = json.dumps({'status' : 'fail'})

    set_repository(repo_name, repo)

    gc_tokens()
    return result


#########################################################
# Get Manifest
#########################################################
@app.route('/get_manifest', methods=['POST'])
def get_manifest():
    check_session_auth(request.form)
    repo_name, repo = get_repository()

    manifest = repo['data_store'].read_local_manifest()


    extend_session_auth()
    return json.dumps(manifest)

#########################################################
# Find what files have changed on a client
#########################################################
@app.route('/find_changed', methods=['POST'])
def find_changed():
    check_session_auth(request.form)
    repo_name, repo = get_repository()

    data_store = repo['data_store'] 

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

    extend_session_auth()
    return json.dumps({
        'status'              : 'ok',
        'push_files'          : push_files,
        'pull_files'          : pull_files,
        'conflict_files'      : conflict_files,
        'local_delete_files'  : local_delete_files,
        'remote_delete_files' : remote_delete_files,
    })

#########################################################
# Push a file to the server
#########################################################
@app.route('/push_file', methods=['POST'])
def push_file():
    check_session_auth(request.form)
    repo_name, repo = get_repository()

    data_store = repo['data_store'] 

    validate_request(request)

    # Set the upload dir, don't think this is needed
    # app.config['UPLOAD_FOLDER'] = DATA_DIR

    path = request.form['path']
    file = request.files['file']

    dirpath = os.path.dirname(data_store.get_full_file_path(path))
    if not os.path.isdir(dirpath):
        os.makedirs(dirpath)

    data_store.fs_save_upload(path, file)
    last_change = data_store.get_single_file_info(path, path)

    extend_session_auth()
    return json.dumps({
        'status'          : 'ok',
        'last_change'     : last_change
    })


#########################################################
# Get a file from the server
#########################################################
@app.route('/pull_file', methods=['POST'])
def pull_file():
    check_session_auth(request.form)
    repo_name, repo = get_repository()

    data_store = repo['data_store'] 

    if 'path' not in request.form:
        e = 'file list does not exist'
        print e
        raise Exception(e)

    sys_path = request.form['path']
    path = data_store.get_full_file_path(sys_path)

    l_dir =  os.path.dirname(path)
    file  =  os.path.basename(path)

    response = make_response(send_from_directory(l_dir, file))
    response.headers['file_info_json'] = json.dumps(data_store.get_single_file_info(sys_path, sys_path))

    extend_session_auth()
    return response


#########################################################
# Delete a file on the server
#########################################################
@app.route('/delete_file', methods=['POST'])
def delete_file():
    check_session_auth(request.form)
    repo_name, repo = get_repository()

    data_store = repo['data_store'] 

    if 'path' not in request.form:
        e = 'file list does not exist'
        print e
        raise Exception(e)

    sys_path = request.form['path']

    data_store.fs_delete(sys_path)

    extend_session_auth()
    return json.dumps({'status': 'ok'})

#############################################################
if __name__ == "__main__":
    app.run('0.0.0.0', port=8080)
