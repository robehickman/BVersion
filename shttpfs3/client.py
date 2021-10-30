from pprint import pprint
import os, sys, time, json, base64, fnmatch, shutil, fcntl, errno, urllib.parse

from termcolor import colored
from collections import defaultdict
from typing import List, Dict, Tuple
from typing_extensions import TypedDict

import pysodium #type: ignore

#=================================================
from shttpfs3.common import (cpjoin, get_file_list, find_manifest_changes, make_dirs_if_dont_exist,
                             manifestFileDetails, get_single_file_info, file_or_default, question_user,
                             file_put_contents, file_get_contents, ignore, find_shttpfs_dir)

from shttpfs3.http.client_http_request import client_http_request
from shttpfs3.storage.client_db   import client_db
from shttpfs3.storage.journaling_filesystem       import journaling_filesystem
from shttpfs3.storage.client_filesystem_interface import client_filesystem_interface
from shttpfs3 import crypto
from shttpfs3 import version_numbers

#===============================================================================
class clientConfiguration(TypedDict, total=False):
    server_domain:       str
    user:                str
    repository:          str
    private_key:         str # base64 encoded private key
    ignore_filters:      List[str]
    pull_ignore_filters: List[str]
    data_dir:            str

#===============================================================================
config:            clientConfiguration         = None
cdb:               client_db                   = None
data_store:        client_filesystem_interface = None
server_connection: client_http_request         = None

working_copy_base_path: str = find_shttpfs_dir()

#===============================================================================
def init(unlocked = False):
    global cdb, data_store, server_connection, config
    try: config = json.loads(file_get_contents(cpjoin(working_copy_base_path, '.shttpfs', 'client_configuration.json')))
    except IOError:    raise SystemExit('No shttpfs configuration found')
    except ValueError: raise SystemExit('Configuration file syntax error')

    # Lock for sanity check, only one client can use the working copy at any time
    try:
        lockfile = open(cpjoin(working_copy_base_path, '.shttpfs', 'lock_file'), 'w')
        fcntl.flock(lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError: raise SystemExit('Could not lock working copy')

    #-----------
    ignore_filters:      str = file_or_default(cpjoin(working_copy_base_path, '.shttpfs_ignore'), b'').decode('utf8')
    pull_ignore_filters: str = file_or_default(cpjoin(working_copy_base_path, '.shttpfs_pull_ignore'), b'').decode('utf8')

    #-----------
    config['ignore_filters']:      List[str] = ['/.shttpfs/*', '/.shttpfs_pull_ignore'] + ignore_filters.splitlines()
    config['pull_ignore_filters']: List[str] = pull_ignore_filters.splitlines()
    config['data_dir']:            str       = working_copy_base_path

    config['conflict_comparison_file_root']  = cpjoin(config['data_dir'], '.shttpfs', 'conflict_files')
    config['conflict_resolution_file']       = cpjoin(config['data_dir'], '.shttpfs', 'conflict_resolution')



    if not unlocked: config["private_key"] = crypto.unlock_private_key(config["private_key"])

    cdb = client_db(cpjoin(config['data_dir'], '.shttpfs', 'manifest.db'))
    journaling_fs = journaling_filesystem(cdb, config['data_dir'], '.shttpfs')
    data_store = client_filesystem_interface(cdb, journaling_fs)

    if server_connection is None:
        server_connection = client_http_request(config['server_domain'])


#===============================================================================
def update_manifest(session_token):

    old_manifest_path = cpjoin(config['data_dir'], '.shttpfs', 'manifest.json')

    if os.path.isfile(old_manifest_path):
        old_manifest = json.loads(file_get_contents(old_manifest_path))

        # As the old manifest format did not store server file hashes, we need to get
        # them from the server and add them to the manifest
        req_result, headers = get_files_in_version(session_token, old_manifest['have_revision'])

        if headers['status'] != 'ok':
            raise Exception('server error')

        # Get file hashes from the server
        server_file_info_for_version = json.loads(req_result)['files']

        # Apply pull ignore filters to the server data
        new_server_file_info_for_version = {}
        for path, file_info in server_file_info_for_version.items():
            if not next((True for flter in config['pull_ignore_filters'] if fnmatch.fnmatch(path, flter)), False):
                new_server_file_info_for_version[path] = file_info
        server_file_info_for_version = new_server_file_info_for_version

        # The number of items in the local manifest may be more than the
        # remote in case the client did a partial update, then shttpfs version
        # was updated, but should never be less
        if len(old_manifest['files']) < len(server_file_info_for_version):
            raise Exception('local manifest error')

        new_files = []
        for path, file_info in server_file_info_for_version.items():
            item = old_manifest['files'][path]
            item['server_file_hash'] = file_info['hash']
            old_manifest['files'].pop(path)
            new_files.append(item)

        if len(old_manifest['files']) != 0:
            print('Warning: There are files in the local manifest that are not on the server. These will be ' +
                  'removed from the manifest. Probably an update failed without completing, or additional filters ' +
                  'have been added to the pull ignore file. Should resolve on next update.')

        for item in new_files:
            cdb.add_file_to_manifest(item)

        # ========================
        meta = cdb.get_system_meta()

        cdb.update_system_meta({
            'have_revision'  : old_manifest['have_revision'],
            'format_version' : meta['format_version']
        })

        cdb.commit()

        shutil.move(old_manifest_path, old_manifest_path + '.old.back')


#===============================================================================
def authenticate(previous_token: str = None) -> str:
    """ Authenticate the client to the server """

    # if we already have a session token, try to authenticate with it
    if previous_token is not None:
        headers = server_connection.request("authenticate", {
            'client_version' : str(version_numbers.client_version),
            'session_token'  : previous_token,
            'repository'     : config['repository']})[1] # Only care about headers

        if headers['status'] == 'ok':
            if int(headers['server_version']) < version_numbers.minimum_server_version:
                raise SystemExit("Please update your SHTTPFS client")

            return previous_token

        else:
            raise SystemExit(headers['msg'])

    # If the session token has expired, or if we don't have one, re-authenticate
    headers = server_connection.request("begin_auth", {'repository' : config['repository']})[1] # Only care about headers

    if headers['status'] == 'ok':
        signature: bytes = base64.b64encode(pysodium.crypto_sign_detached(headers['auth_token'], config['private_key']))
        headers = server_connection.request("authenticate", {
            'client_version' : str(version_numbers.client_version),
            'auth_token'     : headers['auth_token'],
            'signature'      : signature,
            'user'           : config['user'],
            'repository'     : config['repository']})[1] # Only care about headers

        if headers['status'] == 'ok':
            if int(headers['server_version']) < version_numbers.minimum_server_version:
                raise SystemExit("Please update your SHTTPFS client")

            else:
                return headers['session_token']

        else:
            raise SystemExit(headers['msg'])


    raise SystemExit('Authentication failed')


#===============================================================================
def find_local_changes(include_unchanged : bool = False) -> Tuple[dict, Dict[str, manifestFileDetails]]:
    """ Find things that have changed since the last run, applying ignore filters """

    old_state = cdb.get_manifest()
    current_state = get_file_list(config['data_dir'])

    current_state = [fle for fle in current_state if not
                     next((True for flter in config['ignore_filters']
                           if fnmatch.fnmatch(fle['path'], flter)), False)]
    changed_files = find_manifest_changes(current_state, old_state, include_unchanged = include_unchanged)
    return changed_files


#===============================================================================
def checkout():
    plain_input = get_if_set_or_quit(args, 1, 'URL is missing')

    result = urllib.parse.urlparse(plain_input)
    repository_name = [f for f in result.path.split('/') if f != ""]
    server_domain = result.scheme + '://' + result.netloc
    if result.scheme not in ['http', 'https'] or result.netloc == '' or len(repository_name) != 1:
        raise SystemExit ("Invalid URL, usage: http(s)//:domain:optional port/[repository name]. Repository names cannot contain '/'.")

    # get user
    print('Please enter the user name for this repository, then press enter.')
    user = input('> ').strip(' \t\n\r')
    if user == '': raise SystemExit('Key is blank, exiting.')

    # get private key
    print('Please enter the private key for this repository, then press enter.')
    private_key = input('> ').strip(' \t\n\r')
    if private_key == '': raise SystemExit('Key is blank, exiting.')

    #---------------
    config = {"server_domain" : server_domain,
                "repository"    : repository_name[0],
                "user"          : "test",
                "private_key"   : private_key}

    # Validate info is correct by attempting to authenticate
    server_connection = client_http_request(config['server_domain'])
    unlocked_key = config["private_key"] = crypto.unlock_private_key(config["private_key"])
    session_token: str = authenticate()
    config["private_key"] = private_key

    # create repo dir
    try: os.makedirs(repository_name)
    except: raise SystemExit('Directory already exists')
    os.makedirs(cpjoin(repository_name, '.shttpfs'))
    file_put_contents(cpjoin(repository_name, '.shttpfs', 'client_configuration.json'), json.dumps(config, indent=4))

    config["private_key"] = unlocked_key
    os.chdir(repository_name); init(True)
    update(session_token)


#===============================================================================
def resolve_update_conflicts(session_token: str, changes: list, testing: bool, test_overrides: dict):

    # The unit test code needs to be able to override some of the function
    # of this code, and cause partial checkout failiures. This var allows
    # that to happen.

    # ----------------------------------------------------------------------
    # If this code is being run for the second time after the user has
    # chosen to create a conflict resolution file to resolve conflicts
    # manually, we need to get the contents of said file, and use it
    # to resolve the conflicting files.
    # ----------------------------------------------------------------------
    seperator = '------------'
    conflict_resolutions          = file_or_default(config['conflict_resolution_file'], None)

    if conflict_resolutions is not None:
        # Parse the conflict resolution file
        conflict_resolutions = conflict_resolutions.decode('utf-8')
        split_file = conflict_resolutions.split('\n')

        chunk  = []
        chunks = []
        for line in split_file:
            if line == seperator:
                chunks.append(chunk)
                chunk  = []
            else:
                chunk.append(line)

        if chunk != []:
            chunks.append(chunk)

        chunks = chunks[1:]

        resolutions = []
        for chunk in chunks:
            path_sect = chunk[0].split(':')
            res_sect  = chunk[2].split(':')

            if 'Path' not in path_sect[0]:
                raise SystemExit('Corrupted resolution file')

            if 'Resolution' not in res_sect[0]:
                raise SystemExit('Corrupted resolution file')

            resolution = res_sect[1].strip().lower()

            if resolution not in ['client', 'server']:
                raise SystemExit('specified resolution must be either "client" or "server"')

            resolutions.append({
                'path' : path_sect[1].strip(),
                'resolution' : resolution
            })

        # Verify that the file resolves all conflicts, and that the paths match exactly the paths of conflicting items
        conflicting_paths = {f['file_info']['path'] for f in changes['conflict_files']}
        resolved_paths    = {f['path'] for f in resolutions}

        unresolved_paths = conflicting_paths ^ resolved_paths

        if len(unresolved_paths) != 0:
            raise SystemExit('The conflict resolution file does not resolve all conflicts')


        resolution_index = {}
        for resolution in resolutions:
            resolution_index [resolution['path']] = resolution['resolution']

        # ===========================

        for fle in changes['conflict_files']:
            resolution = resolution_index[fle['file_info']['path']]

            if resolution   == 'client':
                # We don't need to do anything in this case as when the version id the client has
                # in the manifest gets set to the latest revision on the server, prior server
                # changes will be ignored, and the clients versions will be committed on the next
                # commit.
                pass

            elif resolution == 'server':
                # Download the changed files from the server

                if fle['server_status'] == 'Changed':
                    changes['client_pull_files'].append({'path' : fle['file_info']['path']})

                elif fle['server_status'] == 'Deleted':
                    changes['to_delete_on_client'].append({'path' : fle['file_info']['path']})

                else:
                    raise Exception('Unknown server change state')

        changes['conflict_files'] = []


    # ----------------------------------------------------------------------
    # If no conflict resolution file exists, we need to ask the user how
    # they want to resolve the conflict
    # ----------------------------------------------------------------------
    else:
        # Print out all conflicting files
        for fle in changes['conflict_files']:
            print('Path:  ' + fle['file_info']['path'])
            print('Client ' + fle['client_status'] + ' server ' + fle['server_status'])
            print()

        print("The above files are in conflict!\n")


        # Offer the choice between resolving conflicts to the server,
        # resolving conflicts to the client, or manually resolving
        # the conflicting files.
        choice = None
        if not testing:
            choice = question_user(
                "type 'server' to discard client versions and accept server versions\n" +
                "type 'client' to discard server versions and accept client versions\n" +
                "type 'manual' to manually resolve conflicts\n",
                valid_choices=['server', 'client', 'manual'])
        else:
            choice = test_overrides['resolve_to']

        #-----------------------------------
        if choice == 'server':
            for fle in changes['conflict_files']:
                if fle['server_status'] == 'Changed':
                    changes['client_pull_files'].append({'path' : fle['file_info']['path']})

                elif fle['server_status'] == 'Deleted':
                    changes['to_delete_on_client'].append({'path' : fle['file_info']['path']})

                else:
                    raise Exception('Unknown server change state')

            changes['conflict_files'] = []

        #-----------------------------------
        if choice == 'client':
            print('Client files will override server files the next time you commit.')

            # ignore the files on the server, and upload the files from the client
            # don't think we need to actually do anything as this should happen
            # during commit

            changes['conflict_files'] = []

        #-----------------------------------
        if choice == 'manual':
            # Generate a conflict resolution file
            resolution_file = 'Please write either "client" or "server" after each resolution block \n\n'

            for fle in changes['conflict_files']:
                resolution_file += seperator + '\n'
                resolution_file += 'Path:  ' + fle['file_info']['path'] + '\n'
                resolution_file += 'Client ' + fle['client_status'] + ' server ' + fle['server_status'] + '\n'
                resolution_file += 'Resolution: ' + '\n'
                resolution_file += '\n'

            # Offer to download files that have been changed on the server in order to compare them
            server_versions = []
            for fle in changes['conflict_files']:
                if fle['server_status'] == 'Changed': server_versions.append(fle['file_info'])


            choice = 'y' if testing else question_user('Download server versions for comparison? (Y/N)', ['y', 'n'])

            if server_versions != [] and choice == 'y':
                errors = []
                for fle in server_versions:
                    print('Pulling file: ' + fle['path'])

                    result, headers = server_connection.request("pull_file", {
                        'session_token' : session_token,
                        'repository'    : config['repository'],
                        'path'          : fle['path']}, gen = True)

                    if headers['status'] != 'ok':
                        errors.append(fle['path'])

                    else:
                        make_dirs_if_dont_exist(cpjoin(config['conflict_comparison_file_root'], *fle['path'].split('/')[:-1]) + '/')
                        result(cpjoin(config['conflict_comparison_file_root'], fle['path']))

                print('Server versions of conflicting files written to .shttpfs/conflict_files\n')

            # ====================
            file_put_contents(config['conflict_resolution_file'], resolution_file.encode('utf-8'))
            raise SystemExit("Conflict resolution file written to .shttpfs/conflict_resolution\n" +
                            "Please edit this file removing 'client', or 'server' to choose which\n" +
                            "version to retain, and then re-run shttpfs update.")

    if changes['conflict_files'] != []:
        raise Exception('Not all conflicting files have been resolved')

    return changes



#===============================================================================
def update(session_token: str, test_overrides = None, include_unchanged = False):
    """ Compare changes on the client to changes on the server and update local files
    which have changed on the server. """

    # ===========================
    if test_overrides is None:
        testing = False
        test_overrides = {
            'resolve_to_override' : '',
            'kill_mid_update'     : 0
        }
    else:
        testing = True


    # Send the changes and the revision of the most recent update to the server to find changes
    client_changes = find_local_changes(include_unchanged)

    manifest_meta = cdb.get_system_meta()
    req_result, headers = server_connection.request("find_changed", {
        "session_token"        : session_token,
        'repository'           : config['repository'],
        "previous_revision"    : manifest_meta['have_revision'],
        "include_unchanged"    : str(int(include_unchanged))
        }, {
            "client_changes"       : json.dumps(client_changes)
        })

    if headers['status'] != 'ok':
        raise SystemExit('Server error:' + headers['msg'])

    result = json.loads(req_result)
    changes = result['sorted_changes']

    # Are there any changes?
    if all(v == [] for k,v in changes.items()):
        print('Nothing to update')
        return

    # Files which are in conflict
    if changes['conflict_files'] != []:
        changes = resolve_update_conflicts(session_token, changes, testing, test_overrides)

    # Pull and delete from remote to local
    affected_files = {
        'pulled_files'  : [],
        'deleted_files' : []
    }

    if changes['client_pull_files'] != []:
        # Filter out pull ignore files
        filtered_pull_files = []
        for fle in changes['client_pull_files']:
            if not next((True for flter in config['pull_ignore_filters'] if fnmatch.fnmatch(fle['path'], flter)), False):
                filtered_pull_files.append(fle)

        if filtered_pull_files != []:
            print('Pulling files from server...')

        #----------
        pulled_items = 0
        for fle in filtered_pull_files:
            req_result, headers = server_connection.request("pull_file", {
                'session_token' : session_token,
                'repository'    : config['repository'],
                'path'          : fle['path']}, gen = True)

            if headers['status'] != 'ok':
                raise SystemExit('Failed to pull file')
            else:
                make_dirs_if_dont_exist(data_store.jfs.get_full_file_path(cpjoin(*fle['path'].split('/')[:-1]) + '/'))
                file_hash = json.loads(headers['file_info_json'])['hash']

                # ==============
                file_in_manifest = cdb.get_single_file_from_manifest(fle['path'])

                # Chech we don't already have the file due to a previous run that failed mid-process
                have_file = False
                if file_in_manifest is not None and file_in_manifest['server_file_hash'] == file_hash:
                    have_file = True

                if not have_file:
                    print('Pulling file: ' + fle['path'])
                    data_store.fs_put(fle['path'], req_result, additional_manifest_data = {'server_file_hash' : file_hash})
                    affected_files['pulled_files'].append(fle['path'])

            # test override to allow testing of checkout being killed part completed
            if test_overrides['kill_mid_update'] > 0:
                if pulled_items == test_overrides['kill_mid_update']:
                    raise Exception('killed part way through update for testing')

            pulled_items += 1

    # Files which have been deleted on server and need deleting on client
    if changes['to_delete_on_client'] != []:
        print('Removing files deleted on the server...')

        for fle in changes['to_delete_on_client']:
            print('Deleting file: ' + fle['path'])

            try: data_store.fs_delete(fle['path'])
            except OSError: print('Warning: remote deleted file does not exist locally.')

            affected_files['deleted_files'].append(fle['path'])

            # Delete the folder if it is now empty
            try: os.removedirs(os.path.dirname(data_store.jfs.get_full_file_path(fle['path'])))
            except OSError as e:
                if e.errno not in [errno.ENOTEMPTY, errno.ENOENT]: raise


    # Update the latest revision in the manifest
    manifest_meta = cdb.get_system_meta()
    manifest_meta['have_revision'] = result['head']
    cdb.update_system_meta(manifest_meta)
    cdb.commit()


    # Delete the conflicts resolution file and recursively delete any conflict files downloaded for comparison
    ignore(os.remove, config['conflict_resolution_file'])
    ignore(shutil.rmtree, config['conflict_comparison_file_root'])


    # ==================
    if changes['to_delete_on_server'] != [] or changes['client_push_files'] != []:
        print('There are local changes to commit')
    else:
        print('Update OK')

    return affected_files


#===============================================================================
def commit(session_token: str, commit_message = ''):
    client_changes = find_local_changes()

    changes = {'to_delete_on_server' : [], 'client_push_files' : []} # type: ignore
    for change in list(client_changes.values()):
        if change['status'] in ['new', 'changed']: changes['client_push_files'].append(change)
        elif change['status'] == 'deleted':        changes['to_delete_on_server'].append(change)
        else: raise Exception('Unknown status type')



    # TODO check if any added files would be pull ignored, and also hit files which exist
    # on the server.

    if all(v == [] for k,v in changes.items()):
        print('Nothing to commit'); return None


    # Acquire the commit lock and check we still have the latest revision
    manifest_meta = cdb.get_system_meta()
    headers = server_connection.request("begin_commit", {
        "session_token"     : session_token,
        'repository'        : config['repository'],
        "previous_revision" : manifest_meta['have_revision']})[1] # Only care about headers


    if headers['status'] != 'ok': raise SystemExit(headers['msg'])

    #======================
    errors: List[str] = []
    changes_made: List[Dict[str, str]] = []

    # Files which have been deleted on the client and need deleting on server
    if changes['to_delete_on_server'] != []:
        for fle in changes['to_delete_on_server']:
            print('Deleting: ' + fle['path'])

        headers = server_connection.request("delete_files", {
            'session_token' : session_token,
            'repository'    : config['repository']
            }, {
                'files'         : json.dumps(changes['to_delete_on_server'])})[1] # Only care about headers

        if headers['status'] == 'ok':
            changes_made += [{'status' : 'deleted', 'path' : fle['path']} for fle in changes['to_delete_on_server']]
        else:
            raise Exception('delete failed')
            errors.append('Delete failed')


    # Push files
    if changes['client_push_files'] != [] and errors == []:
        for fle in changes['client_push_files']:
            print('Sending: ' + fle['path'])

            headers = server_connection.send_file("push_file", {
                'session_token' : session_token,
                'repository'    : config['repository'],
                'path'          : fle['path'],
            }, cpjoin(config['data_dir'], fle['path']))[1] # Only care about headers

            if headers['status'] == 'ok':
                changes_made.append({
                    'status'    : 'new/changed',
                    'path'      : fle['path'],
                    'file_info' : json.loads(headers['file_info_json'])
                })
            else:
                print(headers)
                errors.append({
                    'error' : headers,
                    'file' : fle['path']})
                break



    # commit and release the lock. If errors occurred roll back and release the lock
    mode = 'commit' if errors == [] else 'abort'

    headers = server_connection.request("commit", {
        "session_token"  : session_token,
        'repository'     : config['repository'],
        'commit_message' : commit_message,
        'mode'           : mode})[1] # Only care about headers

    if mode == 'abort':
        print('Something went wrong, errors:')
        pprint(errors)
        return None

    elif headers['status'] == 'ok':
        print('Commit ok')

        # Update the manifest
        manifest_meta = cdb.get_system_meta()
        manifest_meta['have_revision'] = headers['head']
        cdb.update_system_meta(manifest_meta)

        for change in changes_made:
            if change['status'] == 'deleted':
                cdb.remove_file_from_manifest(change['path'])

            elif change['status'] == 'new/changed':
                file_info = get_single_file_info(cpjoin(config['data_dir'], change['path']), change['path'])
                file_info['server_file_hash'] = change['file_info']['hash']
                cdb.add_file_to_manifest(file_info)

        cdb.commit()

        return headers['head']


#===============================================================================
def pull_ignore(filters):
    print(filters)
    quit()

    affected_local_files = []
    for fle in cdb.get_manifest():
        affects_existing = next((True for flter in filters if fnmatch.fnmatch(fle['path'], flter)), False)
        if affects_existing:
            affected_local_files.append(fle['path'])

    if affected_local_files != []:
        for fle in affected_local_files:
            print(fle)

        print('\nThe files listed above would be affected by this opperation.\n' +
                'Should these files be deleted from the working copy, added to the\n ' +
                '".shttpfs_ignore"  file, or left as is? Note that leaving the \n ' +
                'will cause them to be removed from the server the next time commit is run')

    # Ask the user if we should delete the local files
    choice = question_user(
        "type 'ignore'  to add files to '.shttpfs_ignore'\n" +
        "type 'delete'  to delete the files from the working copy\n" +
        "type 'nothing' to remove the files from the local manifest,\n" +
        "and leave the files alone\n",
        valid_choices=['ignore', 'delete', 'nothing'])

    # read args
    if choice == 'ignore':
        ignore_fle = file_get_contents(working_copy_base_path + '.shttpfs_ignore')
        ignore_fle += "\n".join(filters)
        file_put_contents(working_copy_base_path + '.shttpfs_ignore', ignore_fle)

    elif choice == 'delete':
        for path in affected_local_files:
            os.remove(cpjoin(working_copy_base_path, path))

    elif choice == 'nothing':
        # We do not need to do anything in this case
        pass

    # Remove the affected files from the manifest
    for path in affected_local_files:
        cdb.remove_file_from_manifest(path)


#===============================================================================
def get_versions(session_token: str):
    req_result, headers = server_connection.request("list_versions", {
        'session_token' : session_token,
        'repository'    : config['repository']})
    return req_result, headers

#===============================================================================
def get_changes_in_version(session_token: str, version_id):
    req_result, headers = server_connection.request("list_changes", {
        'session_token' : session_token,
        'repository'    : config['repository'],
        'version_id'    : version_id })
    return req_result, headers

#===============================================================================
def get_files_in_version(session_token, version_id):
    req_result, headers = server_connection.request("list_files", {
        'session_token' : session_token,
        'repository'    : config['repository'],
        'version_id'    : version_id})

    return req_result, headers


#===============================================================================
#===============================================================================
#===============================================================================
#===============================================================================
#===============================================================================
def get_if_set_or_quit(array, item, error):
    try: return array[item]
    except IndexError: raise SystemExit(error)

def get_if_set_or_default(array, item, default):
    try: return array[item]
    except IndexError: return default

def draw_changeset(changes):
    binned_changes = defaultdict(list)

    for path, file_info in changes.items():
        binned_changes[file_info['status']].append(file_info)

    for status, group in binned_changes.items():
        for item in group:
            if status == 'deleted':
                color = 'red'
            elif status == 'changed':
                color = 'yellow'
            else:
                color = 'green'

            print(colored(status + ' : ' +  item['path'], color))

#===============================================================================
def run():
    global server_connection, config

    args = list(sys.argv)[1:]

    #----------------------------
    if len(args) == 0 or args[0] == '-h': print("""
    Optional paramiters are shown after the command they can be used with.

    Usage: shttpfs [command] [data or flags]


    Setup commands:

    keygen                       : Generate a new public and private keypiar.
    checkout                     : Check out a working copy from a server.


    Common commands:

    update                       : Update the working copy to the latest revision on the server.

           -f                    - Optionally perform a full comparison including unchanged
                                   files, must be used when downloading previously pull
                                   ignored files. 


    commit                       : Commit any changes to the working copy to the server

           -m "<your msg>"       - Append an optional commit message


    revert [path] ...            : Revert the specified files to the version stored on the server.


    status                       : Display a list of files that have been changed locally since
                                   the most recient commit.

    pull-ignore


    list-versions                : Lists all revisions on the server;


    list-files   [version id]    : Lists all files in the specified revision
    

    list-changes [version id]    : Lists all changes in the specified revision

    """)

    #----------------------------
    elif args[0] == 'keygen':
        private_key, public_key = crypto.make_keypair()
        print('\nPrivate key:\n' + private_key.decode('utf8'))
        print('\nPublic key: \n' + public_key.decode('utf8') + '\n')

    #----------------------------
    elif args [0] == 'checkout':
        checkout()


    #----------------------------
    elif args [0] == 'update':
        # Should we do a full comparison, including unchanged files?
        include_unchanged = bool(len(args) > 1 and args [1] == '-f')

        init()
        session_token: str = authenticate()
        update_manifest(session_token)

        print()
        update(session_token, include_unchanged = include_unchanged)
        print()

    #----------------------------
    elif args [0] == 'commit':
        commit_message = ''
        if get_if_set_or_default(args, 1, '') == '-m': commit_message = get_if_set_or_quit(args, 2, 'Please specify a commit message after -m')

        init()
        session_token: str = authenticate()
        update_manifest(session_token)

        print()
        commit(session_token, commit_message)
        print()

    #----------------------------
    elif args [0] == 'revert':
        # Revert file or files to a prior version

        # -v version number, or head if omitted            

        # list of one or more files to revert
        pass # TODO

    #----------------------------
    elif args [0] == 'status':
        init()

        client_changes = find_local_changes()

        print()
        
        if len(client_changes) != 0:
            draw_changeset(client_changes)

        else:
            print('Nothing changed')

        print()

    #----------------------------
    elif args [0] == 'pull-ignore':
        # TODO needs testing

        init()
        session_token: str = authenticate()
        update_manifest(session_token)

        # ===============
        filters = args [2:]

        if filters == []:
            raise SystemExit('No ignore filters provided')

        # ===============
        pull_ignore(session_token, filters)


    #----------------------------
    elif args [0] == 'list-versions':
        init()
        session_token: str = authenticate()
        update_manifest(session_token)

        req_result, headers = get_versions(session_token)

        if headers['status'] == 'ok':
            versions = list(reversed(json.loads(req_result)['versions']))

            print()

            if len(versions) != 0:
                for vers in versions:
                    print(colored('Commit:  ' + vers['id'], 'yellow'))
                    print('Date:    ' + vers['utc_date_time'] + ' (UTC) ')
                    print('By user: ' + vers['commit_by'])
                    print('\n'        + vers['commit_message'])
                    print()
            else:
                print ('There are no commits')


    #----------------------------
    elif args [0] == 'list-files':
        init()
        session_token: str = authenticate()
        update_manifest(session_token)

        version_id = get_if_set_or_quit(args, 1, 'Please specify a version id')
        req_result, headers = get_files_in_version(session_token, version_id)

        if headers['status'] == 'ok':
            print()
            for fle in json.loads(req_result)['files']:
                print(fle)
            print()


    #----------------------------
    elif args [0] == 'list-changes':
        init()
        session_token: str = authenticate()
        update_manifest(session_token)

        version_id = get_if_set_or_quit(args, 1, 'Please specify a version id')
        req_result, headers = get_changes_in_version(session_token, version_id)

        if headers['status'] == 'ok':
            changes = json.loads(req_result)['changes']
            changes = {item['path'] : item for item in changes}

            print()
            draw_changeset(changes)
            print()
