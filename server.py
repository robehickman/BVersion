# Server configuration
import __builtin__

__builtin__.DATA_DIR       = './uploads/'
__builtin__.MANIFEST_FILE  = '.manifest_xzf.json'

#########################################################
# Imports
#########################################################
import json
import os
from os import path
from flask import Flask, request, redirect, url_for, send_from_directory
from werkzeug.utils import secure_filename

from common import *


#########################################################
# Flask init
#########################################################
app = Flask(__name__)
app.debug = True
app.config['UPLOAD_FOLDER'] = DATA_DIR


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
@app.route('/find_changed', methods=['POST'])
def find_changed():
# Validate passed data
    error_not_in_dict(request.form, 'prev_manifest',     'new files list missing')
    error_not_in_dict(request.form, 'new_files',     'new files list missing')
    error_not_in_dict(request.form, 'changed_files', 'new files list missing')
    error_not_in_dict(request.form, 'deleted_files', 'deleted files list missing')

    manifest      = read_server_manifest()

    remote_deleated_files = json.loads(request.form['deleted_files'])
    remote_deleated_dict = make_dict(remote_deleated_files)

# handle delete files
    filter_manifest = []
    for itm in manifest['files']:
        if itm['path'] in remote_deleated_dict:
            os.remove(DATA_DIR + itm['path']) # file has been deleted from remote, remove it locally
        else:
            filter_manifest.append(itm)

    manifest['files'] = filter_manifest

# TODO filter previous manifest


# Compare current and previous manifests for changes, looking
# for files which have changed on the server in the time between
# when the client last synced, and the current sync request
# The server manifest should always reflect the current state
# of the server FS as this should never be changed manually
    prev_manifest = json.loads(request.form['prev_manifest'])

    prev_manifest_dict = make_dict(prev_manifest['files'])

    server_new_files = []
    server_modified_files = []
    server_deleated_files = []
    for itm in manifest['files']:
        if itm['path'] in prev_manifest_dict:
            d_itm = prev_manifest_dict.pop(itm['path'])
            if itm['last_mod'] == d_itm['last_mod']:
                pass # file has not changed
            else:
                server_modified_files.append(itm) # file has changed

#TODO need to test if detecting files which are new/modified on the server is working

# any files remaining in the remote manifest have been deleted locally
    for key, value in prev_manifest_dict.iteritems():
        server_deleated_files.append(value)

# File arrays

    push_files     = [] # list of files which client needs to push to server
    pull_files     = [] # list of files which client needs to pull from the server
    conflict_files = [] # list of files which have been edited on both ends


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

    manifest['files'] = get_file_list(DATA_DIR)
    write_manifest(manifest)

    return json.dumps({
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
    validate_request(request)

    path = DATA_DIR + request.form['path']

    if not os.path.isdir(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))

    file = request.files['file']

    if file:
        file.save(path)

        # need to set file change time
        return path

    return 'fail'


#########################################################
# Get a file from the server
#########################################################
@app.route('/pull_file', methods=['POST'])
def pull_file():
    if 'path' not in request.form:
        e = 'file list does not exist'
        print e
        raise Exception(e)

    path = DATA_DIR + request.form['path']

    l_dir =  os.path.dirname(path)
    file  =  os.path.basename(path)

    return send_from_directory(l_dir, file)



if __name__ == "__main__":
    app.run()
