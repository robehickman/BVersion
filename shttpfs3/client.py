from pprint import pprint
import os, sys, time, json, base64, fnmatch, shutil, fcntl, errno, urllib.parse

from typing import List, Dict, Tuple
from typing_extensions import TypedDict

import pysodium #type: ignore

#=================================================
from shttpfs3.common import (cpjoin, get_file_list, find_manifest_changes, make_dirs_if_dont_exist,
                             manifestFileDetails, get_single_file_info, file_or_default, question_user,
                             file_put_contents, file_get_contents, ignore, find_shttpfs_dir)

from shttpfs3.client_http_request import client_http_request
from shttpfs3.plain_storage import plain_storage
import shttpfs3.crypto as crypto

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
config:            clientConfiguration = None
data_store:        plain_storage       = None
server_connection: client_http_request = None

working_copy_base_path: str = find_shttpfs_dir()

#===============================================================================
def init(unlocked = False):
    global data_store, server_connection, config
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
    config['ignore_filters']:      List[str] = ['/.shttpfs*'] + ignore_filters.splitlines()
    config['pull_ignore_filters']: List[str] = pull_ignore_filters.splitlines()
    config['data_dir']:            str       = working_copy_base_path

    if not unlocked: config["private_key"] = crypto.unlock_private_key(config["private_key"])

    data_store = plain_storage(config['data_dir'])
    if server_connection is None:
        server_connection = client_http_request(config['server_domain'])


#===============================================================================
def authenticate(previous_token: str = None) -> str:
    """ Authenticate the client to the server """

    # if we already have a session token, try to authenticate with it
    if previous_token is not None:
        headers = server_connection.request("authenticate", {
            'session_token' : previous_token,
            'repository'    : config['repository']})[1] # Only care about headers

        if headers['status'] == 'ok':
            return previous_token

    # If the session token has expired, or if we don't have one, re-authenticate
    headers = server_connection.request("begin_auth", {'repository' : config['repository']})[1] # Only care about headers

    if headers['status'] == 'ok':
        signature: bytes = base64.b64encode(pysodium.crypto_sign_detached(headers['auth_token'], config['private_key']))
        headers = server_connection.request("authenticate", {
            'auth_token' : headers['auth_token'],
            'signature'  : signature,
            'user'       : config['user'],
            'repository' : config['repository']})[1] # Only care about headers

        if headers['status'] == 'ok': return headers['session_token']
    raise SystemExit('Authentication failed')


#===============================================================================
def find_local_changes() -> Tuple[dict, Dict[str, manifestFileDetails]]:
    """ Find things that have changed since the last run, applying ignore filters """

    manifest = data_store.read_local_manifest()
    old_state = manifest['files']
    current_state = get_file_list(config['data_dir'])
    current_state = [fle for fle in current_state if not
                     next((True for flter in config['ignore_filters']
                           if fnmatch.fnmatch(fle['path'], flter)), False)]
    return manifest, find_manifest_changes(current_state, old_state)


#===============================================================================
def update(session_token: str, test_overrides = None):
    """ Compare changes on the client to changes on the server and update local files
    which have changed on the server. """

    # The unit test code needs to be able to override some of the function
    # of this code, and cause partial checkout failiures. This var allows
    # that to happen.
    testing = False
    if test_overrides == None:
        test_overrides = {
            'resolve_to_override' : ''
        }
    else:
        testing = True

    # Send the changes and the revision of the most recent update to the server to find changes
    manifest, client_changes = find_local_changes()

    req_result, headers = server_connection.request("find_changed", {
        "session_token"        : session_token,
        'repository'           : config['repository'],
        "previous_revision"    : manifest['have_revision'],
        }, {
            "client_changes"       : json.dumps(client_changes)
        })

    if headers['status'] != 'ok':
        raise SystemExit('Server error:' + headers['msg'])

    # TODO If there are any conflict files, do not send them to the server any more.
    # instead resolve conflicts on the client. Notify that conflicts exist immidiately
    # instead of at the end, and offer choices between resolving all conflicts to the server,
    # all conflicts to the client, or generating a resolution file so the user can choose
    # as we used to do. Tell the user where this file is if we do make one, and what they need
    # to do with it
    result = json.loads(req_result)
    changes = result['sorted_changes']


    # Files which are in conflict
    if changes['conflict_files'] != []:

        # If this code is being run for the second time after the user has
        # chosen to create a conflict resolution file to resolve conflicts
        # manually, we need to get the contents of said file, and use it
        # to resolve the conflicting files.
        seperator = '------------'
        conflict_comparison_file_dest = cpjoin(config['data_dir'], '.shttpfs', 'conflict_files')
        conflict_resolution_path      = cpjoin(config['data_dir'], '.shttpfs', 'conflict_resolution')
        conflict_resolutions          = file_or_default(conflict_resolution_path, None)

        if conflict_resolutions is not None:
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

            import pprint
            pprint.pprint(chunks)

            for chunk in chunks:
                path = chunk[0].split(':')[1]
                resolution = chunk[2].split(':')[1]
                resolution = resolution.lower().strip()

                if resolution not in ['client', 'server']:
                    raise SystemExit('specified resolution must be either "client" or "server"')

                if resolution   == 'client':
                    # Ignore items changed on the server and push the client changes to overwrite them
                    # TODO does this need to pass data about the files to commit?
                    # dont think so, as when we update the local version to latest, it will automatically
                    # discard server changes. May need to handle this when feature to allow downloading
                    # of pull ignored files gets added
                    pass

                elif resolution == 'server':
                    # Download the changed files from the server

                    for fle in changes['conflict_files']:
                        if fle['server_status'] == 'Changed': 
                            changes['client_pull_files'].append({'path' : fle['file_info']['path']})

                        elif fle['server_status'] == 'Deleted':
                            changes['to_delete_on_client'].append({'path' : fle['file_info']['path']})

                        else:
                            raise Exception('Unknown server change state')


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

            #-----------------------------------
            if choice == 'client':
                print('Client files will override server files the next time you commit.')

                # ignore the files on the server, and upload the files from the client
                # don't think we need to actually do anything as this should happen
                # during commit
                pass

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
                            make_dirs_if_dont_exist(cpjoin(conflict_comparison_file_dest, *fle['path'].split('/')[:-1]) + '/')
                            result(cpjoin(conflict_comparison_file_dest, fle['path']))

                    print('Server versions of conflicting files written to .shttpfs/conflict_files\n')

                # ====================
                file_put_contents(conflict_resolution_path, resolution_file.encode('utf-8'))
                raise SystemExit("Conflict resolution file written to .shttpfs/conflict_resolution\n" +
                                "Please edit this file removing 'client', or 'server' to choose which\n" +
                                "version to retain, and then re-run shttpfs update.")

    # Are there any changes?
    if all(v == [] for k,v in changes.items()):
        print('Nothing to update')
        return

    # Pull and delete from remote to local
    if changes['client_pull_files'] != []:
        # Filter out pull ignore files
        filtered_pull_files = []
        for fle in changes['client_pull_files']:
            if not next((True for flter in config['pull_ignore_filters'] if fnmatch.fnmatch(fle['path'], flter)), False):
                filtered_pull_files.append(fle)
            else: # log ignored items to give the opportunity to pull them in the future
                with open(cpjoin(working_copy_base_path, '.shttpfs', 'pull_ignored_items'), 'a') as pull_ignore_log:
                    pull_ignore_log.write(json.dumps((result['head'], fle)))
                    pull_ignore_log.flush()

        if filtered_pull_files != []:
            print('Pulling files from server...')

        #----------
        for fle in filtered_pull_files:
            print('Pulling file: ' + fle['path'])

            req_result, headers = server_connection.request("pull_file", {
                'session_token' : session_token,
                'repository'    : config['repository'],
                'path'          : fle['path']}, gen = True)

            if headers['status'] != 'ok':
                raise SystemExit('Failed to pull file')
            else:
                make_dirs_if_dont_exist(data_store.get_full_file_path(cpjoin(*fle['path'].split('/')[:-1]) + '/'))
                data_store.fs_put(fle['path'], req_result)

    # Files which have been deleted on server and need deleting on client
    if changes['to_delete_on_client'] != []:
        print('Removing files deleted on the server...')

        for fle in changes['to_delete_on_client']:
            print('Deleting file: ' + fle['path'])

            try: data_store.fs_delete(fle['path'])
            except OSError: print('Warning: remote deleted file does not exist locally.')

            # Delete the folder if it is now empty
            try: os.removedirs(os.path.dirname(data_store.get_full_file_path(fle['path'])))
            except OSError as e:
                if e.errno not in [errno.ENOTEMPTY, errno.ENOENT]: raise

    # Update the latest revision in the manifest only if there are no conflicts
    data_store.begin()
    manifest = data_store.read_local_manifest()
    manifest['have_revision'] = result['head']
    data_store.write_local_manifest(manifest)
    data_store.commit()

    """
    #delete the conflicts resolution file and recursively delete any conflict files downloaded for comparison
    ignore(os.remove, conflict_resolution_path)
    ignore(shutil.rmtree, conflict_comparison_file_dest)
    """

    if changes['to_delete_on_server'] != [] or changes['client_push_files'] != []:
        print('There are local changes to commit')
    else:
        print('Update OK')


#===============================================================================
def commit(session_token: str, commit_message = ''):
    manifest, client_changes = find_local_changes()

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
    headers = server_connection.request("begin_commit", {
        "session_token"     : session_token,
        'repository'        : config['repository'],
        "previous_revision" : manifest['have_revision']})[1] # Only care about headers


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

        if headers['status'] == 'ok': changes_made += [{'status' : 'deleted', 'path' : fle['path']} for fle in changes['to_delete_on_server']]
        else:                         errors.append('Delete failed')


    # Push files
    if changes['client_push_files'] != [] and errors == []:
        for fle in changes['client_push_files']:
            print('Sending: ' + fle['path'])

            headers = server_connection.send_file("push_file", {
                'session_token' : session_token,
                'repository'    : config['repository'],
                'path'          : fle['path'],
            }, cpjoin(config['data_dir'], fle['path']))[1] # Only care about headers

            if headers['status'] == 'ok': changes_made.append({'status' : 'new/changed', 'path' : fle['path']})
            else:                         errors.append(fle['path']); break



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
        data_store.begin()
        manifest = data_store.read_local_manifest()
        manifest['have_revision'] = headers['head']

        for change in changes_made:
            if change['status'] == 'deleted':
                del manifest['files'][change['path']]
            elif change['status'] == 'new/changed':
                manifest['files'][change['path']] = get_single_file_info(cpjoin(config['data_dir'], change['path']), change['path'])

        data_store.write_local_manifest(manifest)
        data_store.commit()
        return headers['head']

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

#===============================================================================
def run():
    global server_connection, config

    args = list(sys.argv)[1:]

    #----------------------------
    if len(args) == 0 or args[0] == '-h': print("""
    update,   update the working copy to the current state on the server
    commit,   commit any changes to the working copy to the server
    sync,     periodically sync the working copy with the server automatically

    Setup:
    keygen,   generate a new public and private keypiar
    checkout, check out a working copy from a server
        """)

    #----------------------------
    elif args[0] == 'keygen':
        private_key, public_key = crypto.make_keypair()
        print('\nPrivate key:\n' + private_key.decode('utf8'))
        print('\nPublic key: \n' + public_key.decode('utf8') + '\n')

    #----------------------------
    elif args [0] == 'checkout':
        # TODO If we do not already have a working copy, this should allow the user to setup a new repo
        # if we do have a checkout, it should allow the user to revert one or more files to a previous
        # version.

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

    #----------------------------
    elif args [0] == 'update':
        init(); update(authenticate())

    #----------------------------
    elif args [0] == 'commit':
        commit_message = ''
        if get_if_set_or_default(args, 1, '') == '-m': commit_message = get_if_set_or_quit(args, 2, 'Please specify a commit message after -m')
        init(); commit(authenticate(), commit_message)

    #----------------------------
    elif args [0] == 'sync':
        init(); session_token: str = authenticate()

        commit_message = ''
        if get_if_set_or_default(args, 1, '') == '-m': commit_message = get_if_set_or_quit(args, 2, 'Please specify a commit message after -m')

        update(session_token)
        commit(session_token, commit_message)

    #----------------------------
    elif args [0] == 'autosync':
        init(); session_token: str = None
        while True:
            session_token = authenticate(session_token)
            update(session_token)
            commit(session_token)
            time.sleep(60)

    #----------------------------
    elif args [0] == 'status':
        init(); 
        manifest, client_changes = find_local_changes()

        if headers['status'] == 'ok':
            for fle in json.loads(req_result)['files']:
                print(fle)

        for item in client_changes:
            print(item)

    #----------------------------
    elif args [0] == 'ignore_server_files':
        init(); 
        manifest, client_changes = find_local_changes()

        filters = args [1:]

        filtered_files       = []
        affected_local_files = []
        for fle in manifest['files']:
            affects_existing = next((True for flter in filters if fnmatch.fnmatch(fle['path'], flter)), False)
            if affects_existing:
                affected_local_files.append(fle['path'])
            else:
                filtered_files.append(fle)

        if affected_local_files != []:
            for fle in affected_local_files:
                print(fle)

            print('\nThe files listed above would be affected by this opperation.\n' +
                  'Should these files be deleted from the working copy, added to the\n ' +
                  '".shttpfs_ignore"  file, or left as is? Note that leaving the \n ' +
                  'will cause them to be removed from the server the next time commit is run')

        # Ask the user if we should delete the local files, 
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
        data_store.begin()
        manifest = data_store.read_local_manifest()
        manifest['files'] = filtered_files
        data_store.write_local_manifest(manifest)
        data_store.commit()


    #----------------------------
    elif args [0] == 'list_versions':
        init(); session_token: str = authenticate()
        req_result, headers = get_versions(session_token)

        if headers['status'] == 'ok':
            for vers in reversed(json.loads(req_result)['versions']):
                print('Commit:  ' + vers['id'])
                print('Date:    ' + vers['utc_date_time'] + ' (UTC) ')
                print('By user: ' + vers['commit_by'])
                print('\n'        + vers['commit_message'])
                print()

    #----------------------------
    elif args [0] == 'list_changes':
        init(); session_token: str = authenticate()
        version_id = get_if_set_or_quit(args, 1, 'Please specify a version id')
        req_result, headers = get_changes_in_version(session_token, version_id)

        if headers['status'] == 'ok':
            for change in json.loads(req_result)['changes']:
                print(change['status'] + '     ' + change['path'])

    #----------------------------
    elif args [0] == 'list_files':
        init(); session_token: str = authenticate()
        version_id = get_if_set_or_quit(args, 1, 'Please specify a version id')
        req_result, headers = get_files_in_version(session_token, version_id)

        if headers['status'] == 'ok':
            for fle in json.loads(req_result)['files']:
                print(fle)
