from test_common import *
from copy import deepcopy
from unittest import TestCase

from shttpfs.merge_file_tree import *


# Check if an arbitrary number of lists are empty
def l_empty(*args):
    for i in args:
        if i != []:
            return False
    return True

class TestMerge(TestCase):
############################################################################################
    def test_file_tree_merge(self):
        # If file new on server and does not exist on the client, get client to pull it
        server_files = {'/name/file' : {
            'path'     : '/name/file',
            'created'  : 100,
            'last_mod' : 100,
            'status'   : 'new'}}

        client_files = {}

        push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = merge_file_tree(client_files, server_files)

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

        push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = merge_file_tree(client_files, server_files)

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

        push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = merge_file_tree(client_files, server_files)

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

        push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = merge_file_tree(client_files, server_files)

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

        push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = merge_file_tree(client_files, server_files)

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

        push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = merge_file_tree(client_files, server_files)

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

        push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = merge_file_tree(client_files, server_files)

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

        push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = merge_file_tree(client_files, server_files)

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

        push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = merge_file_tree(client_files, server_files)

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

        push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = merge_file_tree(client_files, server_files)

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

        push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = merge_file_tree(client_files, server_files)

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

        push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = merge_file_tree(client_files, server_files)

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

        push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = merge_file_tree(client_files, server_files)

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

        push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = merge_file_tree(client_files, server_files)

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

        push_files, pull_files, conflict_files, local_delete_files, remote_delete_files = merge_file_tree(client_files, server_files)

        if not l_empty(push_files, pull_files, conflict_files, local_delete_files):
            raise Exception('Lists which should be empty are not')

        if len(remote_delete_files) != 1:
            raise Exception('Item not in remote delete files')
