from test_common import *
from copy import deepcopy

import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
parentdir = parentdir + '/lib'
sys.path.insert(0,parentdir) 

#from storage import *



def resolve_files(remote, local):

    remote_copy = remote.copy()
    local_copy  = local.copy()

    push_files          = []
    pull_files          = []
    conflict_files      = []
    local_delete_files  = []
    remote_delete_files = []

    # First handle file change detection from the servers perspective
    for key, value in remote.iteritems():
        remote_copy.pop(key)
        if key in local_copy:
            local_copy.pop(key)

        # If file new on client and does not exist on the server, push it to the server
        if key not in local and value['status'] == 'new':
            push_files.append(value)

        # If file changed on client and unchanged the server, push it to the server
        # Server unchanged files are filtered out before this step, so only changed
        # files will be listed in the 'local' duct.
        if key not in local and value['status'] == 'changed':
            push_files.append(value)

        # If file deleted on client and unchanged on the server, delete it on the server
        if key not in local and value['status'] == 'deleted':
            local_delete_files.append(value)

        # If file was added on the client which conflicts with one added to the server by another client
        if key in local and value['status'] == 'new' and local[key]['status'] == 'new':
            conflict_files.append(value)

        # If file was added on the client which conflicts with one added to the server by another client
        if key in local and value['status'] == 'changed' and local[key]['status'] == 'new':
            conflict_files.append(value)

        # If file was added on the client which conflicts with one added to the server by another client
        if key in local and value['status'] == 'deleted' and local[key]['status'] == 'new':
            conflict_files.append(value)

        # If file was added on the client which conflicts with one changed on the server by another client
        if key in local and value['status'] == 'new' and local[key]['status'] == 'changed':
            conflict_files.append(value)

        # If file was changed on the client which conflicts with one changed on the server by another client
        if key in local and value['status'] == 'changed' and local[key]['status'] == 'changed':
            conflict_files.append(value)

        # If file was deleted on the client which conflicts with one changed on the server by another client
        if key in local and value['status'] == 'deleted' and local[key]['status'] == 'changed':
            conflict_files.append(value)

        # If file was added on the client which conflicts with one deleted on the server by another client
        if key in local and value['status'] == 'new' and local[key]['status'] == 'deleted':
            conflict_files.append(value)

        # If file was changed on the client which conflicts with one deleted on the server by another client
        if key in local and value['status'] == 'changed' and local[key]['status'] == 'deleted':
            conflict_files.append(value)

        # If file was changed on the client which conflicts with one deleted on the server by another client
        if key in local and value['status'] == 'deleted' and local[key]['status'] == 'deleted':
            pass

    # remote_copy should now be empty
    print remote_copy

    # Secondly deal with any files left over from the client, these will mainly be new files
    for key, value in local_copy.iteritems():
        # If file new on server and does not exist on the client, get client to pull it
        if key not in remote_copy and value['status'] == 'new':
            pull_files.append(value)

        # If file changed on server and unchanged the client, push it to the client
        if key not in remote_copy and value['status'] == 'changed':
            pull_files.append(value)

        # If file deleted on server and unchanged on the client, delete it on the client
        if key not in remote_copy and value['status'] == 'deleted':
            remote_delete_files.append(value)

    return (push_files, pull_files, conflict_files, local_delete_files, remote_delete_files)


def l_empty(*args):
    for i in args:
        if i != []:
            return False
    return True


# If file new on server and does not exist on the client, get client to pull it
server_files = {'/name/file' : {
    'path'     : '/name/file',
    'created'  : 100,
    'last_mod' : 100,
    'status'   : 'new'}}

client_files = {}

push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = resolve_files(client_files, server_files)

if not l_empty(push_files, conflict_files, local_delete_files, remote_delete_files):
    raise Exception('Lists that should be empty are not')
    
if len(pull_files) != 1:
    raise Exception('Item not in pull files')


# If file changed on server and unchanged on the client, get client to pull it
server_files = {'/name/file' : {
    'path'     : '/name/file',
    'created'  : 100,
    'last_mod' : 100,
    'status'   : 'changed'}}

client_files = {}

push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = resolve_files(client_files, server_files)

if not l_empty(push_files, conflict_files, local_delete_files, remote_delete_files):
    raise Exception('Lists that should be empty are not')
    
if len(pull_files) != 1:
    raise Exception('Item not in pull files')


#If file deleted on server and unchanged on client, delete it on the client
server_files = {'/name/file' : {
    'path'     : '/name/file',
    'created'  : 100,
    'last_mod' : 100,
    'status'   : 'deleted'}}

client_files = {}

push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = resolve_files(client_files, server_files)

if not l_empty(push_files, pull_files, conflict_files, local_delete_files):
    raise Exception('Lists that should be empty are not')
    
if len(remote_delete_files) != 1:
    raise Exception('Item not in remote delete files')


# If file new on client and does not exist on the server, push it to the server
server_files = {}

client_files = {'/name/file' : {
    'path'     : '/name/file',
    'created'  : 100,
    'last_mod' : 100,
    'status'   : 'new'}}

push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = resolve_files(client_files, server_files)

if not l_empty(pull_files, conflict_files, local_delete_files, remote_delete_files):
    raise Exception('Lists which should be empty are not')

if len(push_files) != 1:
    raise Exception('Item not in push files')


# If file changed on client and unchanged the server, push it to the server
server_files = {}

client_files = {'/name/file' : {
    'path'     : '/name/file',
    'created'  : 100,
    'last_mod' : 100,
    'status'   : 'changed'}}

push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = resolve_files(client_files, server_files)

if not l_empty(pull_files, conflict_files, local_delete_files, remote_delete_files):
    raise Exception('Lists which should be empty are not')

if len(push_files) != 1:
    raise Exception('Item not in push files')


# If file deleted on client and unchanged on the server, delete it on the server
server_files = {}

client_files = {'/name/file' : {
    'path'     : '/name/file',
    'created'  : 100,
    'last_mod' : 100,
    'status'   : 'deleted'}}

push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = resolve_files(client_files, server_files)

if not l_empty(push_files, pull_files, conflict_files, remote_delete_files):
    raise Exception('Lists which should be empty are not')

if len(local_delete_files) != 1:
    raise Exception('Item not in local delete files')


# -----------

# If file was created on the client which conflicts with one added to the server by another client
server_files = {'/name/file' : {
    'path'     : '/name/file',
    'status'   : 'new'}}

client_files = {'/name/file' : {
    'path'     : '/name/file',
    'status'   : 'new'}}

push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = resolve_files(client_files, server_files)

if not l_empty(push_files, pull_files, local_delete_files, remote_delete_files):
    raise Exception('Lists which should be empty are not')

if len(conflict_files) != 1:
    raise Exception('Item not in conflict files')


# If file was changed on the client which conflicts with one added to the server by another client
server_files = {'/name/file' : {
    'path'     : '/name/file',
    'status'   : 'new'}}

client_files = {'/name/file' : {
    'path'     : '/name/file',
    'status'   : 'changed'}}

push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = resolve_files(client_files, server_files)

if not l_empty(push_files, pull_files, local_delete_files, remote_delete_files):
    raise Exception('Lists which should be empty are not')

if len(conflict_files) != 1:
    raise Exception('Item not in conflict files')


# If file was deleted on the client which conflicts with one added to the server by another client
server_files = {'/name/file' : {
    'path'     : '/name/file',
    'status'   : 'new'}}

client_files = {'/name/file' : {
    'path'     : '/name/file',
    'status'   : 'deleted'}}

push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = resolve_files(client_files, server_files)

if not l_empty(push_files, pull_files, local_delete_files, remote_delete_files):
    raise Exception('Lists which should be empty are not')

if len(conflict_files) != 1:
    raise Exception('Item not in conflict files')


# -----------

# If file was added on the client which conflicts with one changed on the server by another client
server_files = {'/name/file' : {
    'path'     : '/name/file',
    'status'   : 'changed'}}

client_files = {'/name/file' : {
    'path'     : '/name/file',
    'status'   : 'new'}}

push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = resolve_files(client_files, server_files)

if not l_empty(push_files, pull_files, local_delete_files, remote_delete_files):
    raise Exception('Lists which should be empty are not')

if len(conflict_files) != 1:
    raise Exception('Item not in conflict files')


# If file was changed on the client which conflicts with one changed on the server by another client
server_files = {'/name/file' : {
    'path'     : '/name/file',
    'status'   : 'changed'}}

client_files = {'/name/file' : {
    'path'     : '/name/file',
    'status'   : 'changed'}}

push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = resolve_files(client_files, server_files)

if not l_empty(push_files, pull_files, local_delete_files, remote_delete_files):
    raise Exception('Lists which should be empty are not')

if len(conflict_files) != 1:
    raise Exception('Item not in conflict files')


# If file was deleted on the client which conflicts with one changed on the server by another client
server_files = {'/name/file' : {
    'path'     : '/name/file',
    'status'   : 'changed'}}

client_files = {'/name/file' : {
    'path'     : '/name/file',
    'status'   : 'deleted'}}

push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = resolve_files(client_files, server_files)

if not l_empty(push_files, pull_files, local_delete_files, remote_delete_files):
    raise Exception('Lists which should be empty are not')

if len(conflict_files) != 1:
    raise Exception('Item not in conflict files')

# -----------

# If file was added on the client which conflicts with one deleted on the server by another client
server_files = {'/name/file' : {
    'path'     : '/name/file',
    'status'   : 'deleted'}}

client_files = {'/name/file' : {
    'path'     : '/name/file',
    'status'   : 'new'}}

push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = resolve_files(client_files, server_files)

if not l_empty(push_files, pull_files, local_delete_files, remote_delete_files):
    raise Exception('Lists which should be empty are not')

if len(conflict_files) != 1:
    raise Exception('Item not in conflict files')


# If file was changed on the client which conflicts with one deleted on the server by another client
server_files = {'/name/file' : {
    'path'     : '/name/file',
    'status'   : 'deleted'}}

client_files = {'/name/file' : {
    'path'     : '/name/file',
    'status'   : 'changed'}}

push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = resolve_files(client_files, server_files)

if not l_empty(push_files, pull_files, local_delete_files, remote_delete_files):
    raise Exception('Lists which should be empty are not')

if len(conflict_files) != 1:
    raise Exception('Item not in conflict files')


# If file was deleted on the client which conflicts with one deleted on the server by another client
server_files = {'/name/file' : {
    'path'     : '/name/file',
    'status'   : 'deleted'}}

client_files = {'/name/file' : {
    'path'     : '/name/file',
    'status'   : 'deleted'}}

push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = resolve_files(client_files, server_files)

if not l_empty(push_files, pull_files, conflict_files, local_delete_files, remote_delete_files):
    raise Exception('Lists which should be empty are not')

