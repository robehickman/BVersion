from typing import Dict, Callable, Union, Optional
import json, base64, re
import pysodium # type: ignore

#====
from bversion.http.http_server import Request, Responce, ServeFile, ConnectionContext
from bversion.common import cpjoin
from bversion.storage.versioned_storage import versioned_storage
from bversion.merge_client_and_server_changes import merge_client_and_server_changes
from bversion.storage.server_db import get_server_db_instance_for_thread

from bversion import version_numbers


#===============================================================================
config = {} # type: ignore

#===============================================================================
lock_fail_msg         = 'Could not acquire exclusive lock'
no_such_repo_msg      = "The requested repository does not exist"
user_auth_fail_msg    = "Could not authenticate user"
conflict_msg          = 'Please resolve conflicts'
need_to_update_msg    = "Please update to latest revision"
no_active_commit_msg  = "A commit must be started before attempting this operation."
unsuported_client_msg = "Please update your BVersion client."

extend_session_duration = (60 * 60) * 2 # 2 hours

#===============================================================================
# Ensure that the servers transient database tables are created
#===============================================================================
def init_server(new_config : dict):
    global config
    config = new_config

    for data in config['repositories'].values():
        repository_path = data['path']

        if 'transient_db_path_override' not in data:
            data['transient_db_path_override'] = None

        sdb = get_server_db_instance_for_thread(repository_path,
                                                path_override = data['transient_db_path_override'])
        sdb.db_init()


#===============================================================================
# Decorator to make defining routes easy
#===============================================================================
routes: Dict[str, Callable[[Request], Responce]] = {}
def route(path: str):
    def route_wrapper(func): routes[path] = func
    return route_wrapper

#===============================================================================
# Main HTTP endpoint
#===============================================================================
def endpoint(request: Request, context: ConnectionContext):
    request_action: str = request.uri.split('/')[1]

    if request_action not in routes:
        raise Exception('request error')

    responce: Responce = routes[request_action](request, context)

    return responce


#===============================================================================
def server_responce(headers: Dict[str, str], body: Union[bytes, ServeFile]):
    return Responce(headers, body)


#===============================================================================
def fail(msg: str = ''):
    """ Generate fail JSON to send to client """
    return server_responce({'status' : 'fail', 'msg' : msg}, b'')


#===============================================================================
def success(headers: Optional[Dict[str, str]] = None, data: Union[dict, bytes, ServeFile] = b''):
    """ Generate success JSON to send to client """
    passed_headers: Dict[str, str] = {} if headers is None else headers
    if isinstance(data, dict): data = json.dumps(data).encode('utf8')
    ret_headers = {'status' : 'ok'}
    ret_headers.update(passed_headers)
    return server_responce(ret_headers, data)


#===============================================================================
# Authentication
#===============================================================================
@route('begin_auth')
def begin_auth(request: Request, context: ConnectionContext) -> Responce: # pylint: disable=W0613
    """ Request authentication token to sign """

    repository    = request.headers['repository']
    if repository not in config['repositories']:
        return fail(no_such_repo_msg)

    # ==
    repository_path = config['repositories'][repository]['path']
    sdb = get_server_db_instance_for_thread(repository_path,
                                            path_override = config['repositories'][repository]['transient_db_path_override'])
    sdb.gc_tokens()

    # Issue a new token
    auth_token = sdb.issue_token(request.remote_addr)

    return success({'auth_token' : auth_token})


#===============================================================================
@route('authenticate')
def authenticate(request: Request, context: ConnectionContext) -> Responce: # pylint: disable=W0613
    """ This does two things, either validate a pre-existing session token
    or create a new one from a signed authentication token. """

    if int(request.headers['client_version']) < version_numbers.minimum_client_version:
        return fail(unsuported_client_msg)

    # ==
    client_ip     = request.remote_addr
    repository    = request.headers['repository']
    if repository not in config['repositories']:
        return fail(no_such_repo_msg)

    # ==
    repository_path = config['repositories'][repository]['path']
    sdb = get_server_db_instance_for_thread(repository_path,
                                            path_override = config['repositories'][repository]['transient_db_path_override'])
    sdb.gc_tokens()

    # Allow resume of an existing session
    if 'session_token' in request.headers:
        session_token = request.headers['session_token']

        if sdb.validate_session(session_token, client_ip) != []:
            return success({
                'server_version' : str(version_numbers.server_version).encode('utf8'),
                'session_token'  : session_token
            })
        else:
            return fail(user_auth_fail_msg)

    # Create a new session
    else:
        user       = request.headers['user']
        auth_token = request.headers['auth_token']
        signiture  = request.headers['signature']

        try:
            public_key = config['users'][user]['public_key']

            # signature
            pysodium.crypto_sign_verify_detached(base64.b64decode(signiture), auth_token, base64.b64decode(public_key))

            # Validate token matches one we sent
            res = sdb.get_token(auth_token, client_ip)
            if res == [] or len(res) > 1:
                return fail(user_auth_fail_msg)

            # Does the user have permission to use this repository?
            if repository not in config['users'][user]['uses_repositories']:
                return fail(user_auth_fail_msg)


            # Everything OK, clean up transient token and
            # generate a session token for the client
            sdb.delete_token(auth_token)

            session_token = sdb.create_session(extend_session_duration, client_ip, user)

            return success({
                'server_version' : str(version_numbers.server_version).encode('utf8'),
                'session_token'  : session_token
            })

        except Exception: # pylint: disable=broad-except
            return fail(user_auth_fail_msg)


#===============================================================================
def have_authenticated_user(client_ip: str, repository: str, session_token: bytes):
    """ check user submitted session token against the db and that ip has not changed """

    if repository not in config['repositories']: return False

    repository_path = config['repositories'][repository]['path']
    sdb = get_server_db_instance_for_thread(repository_path,
                                            path_override = config['repositories'][repository]['transient_db_path_override'])

    # Garbage collect session tokens. We must not garbage collect the authentication token of the client
    # which is currently doing a commit. Large files can take a long time to upload and during this time,
    # the locks expiration is not being updated thus can expire. This is a problem here as session tokens
    # table is garbage collected every time a user authenticates. It does not matter if the user_lock
    # expires while the client also holds the flock, as it is updated to be in the future at the end of
    # the current operation. We exclude any tokens owned by the client which currently doing a commit
    # for this reason.
    active_commit = sdb.get_active_commit()

    if active_commit is None:
        sdb.gc_session_tokens(None)
    else:
        sdb.gc_session_tokens(active_commit['session_token'])

    # Get the session token
    res = sdb.get_session_token(session_token, client_ip)

    if res != [] and repository in config['users'][res[0]['username']]['uses_repositories']:
        sdb.update_session_token_expiry(extend_session_duration, session_token, client_ip)
        sdb.con.commit() # we commit at the end to make sure that all opperations have the same view

        return res[0]

    sdb.con.commit()
    return False


#===============================================================================
# Main System
#===============================================================================
@route('find_changed')
def find_changed(request: Request, context: ConnectionContext) -> Responce: # pylint: disable=W0613
    """ Find changes since the revision it is currently holding """

    session_token = request.headers['session_token'].encode('utf8')
    repository    = request.headers['repository']

    #===
    current_user = have_authenticated_user(request.remote_addr, repository, session_token)
    if current_user is False: return fail(user_auth_fail_msg)

    #===
    repository_path = config['repositories'][repository]['path']
    body_data = request.get_json()

    #===
    data_store = versioned_storage(repository_path,
                                   path_override = config['repositories'][repository]['transient_db_path_override'])

    head = data_store.get_head()
    if head == 'root': return success({}, {'head' : 'root', 'sorted_changes' : {'none' : []}})

    # Find changed items
    client_changes = json.loads(body_data['client_changes'])
    server_changes = data_store.get_changes_since(request.headers["previous_revision"], head)

    # Figure out which files have not been changed
    if bool(int(request.headers['include_unchanged'])):
        unchanged = data_store.get_commit_files(head)

        for path, info in server_changes.items():
            if path in unchanged:
                unchanged.pop(path)

        for path, info in unchanged.items():
            info['status'] = 'unchanged'
            server_changes[path] = info

    # ==================
    sorted_changes = merge_client_and_server_changes(server_changes, client_changes)

    commit_details = data_store.read_commit_index_object(head)

    return success({}, {
        'head'           : head,
        'commit_by'      : commit_details['commit_by'],
        'commit_time'    : commit_details['utc_date_time'],
        'commit_msg'     : commit_details['commit_message'],
        'sorted_changes' : sorted_changes
    })


#===============================================================================
@route('pull_file')
def pull_file(request: Request, context: ConnectionContext) -> Responce: # pylint: disable=W0613
    """ Get a file from the server """

    session_token = request.headers['session_token'].encode('utf8')
    repository    = request.headers['repository']

    #===
    current_user = have_authenticated_user(request.remote_addr, repository, session_token)
    if current_user is False: return fail(user_auth_fail_msg)


    #===
    data_store = versioned_storage(config['repositories'][repository]['path'],
                                   path_override = config['repositories'][repository]['transient_db_path_override'])

    use_head = bool(int(request.headers['use_head']))

    if use_head:
        version_id = data_store.get_head()
    else:
        version_id = request.headers['version_id']


    # =============
    file_info = data_store.get_file_info_from_path(request.headers['path'], version_id = version_id)

    full_file_path: str = cpjoin(data_store.get_file_directory_path(file_info['hash']), file_info['hash'][2:])
    return success({'file_info_json' : json.dumps(file_info)}, ServeFile(full_file_path))


#===============================================================================
@route('list_versions')
def list_versions(request: Request, context: ConnectionContext) -> Responce: # pylint: disable=W0613
    try:
        session_token = request.headers['session_token'].encode('utf8')
        repository    = request.headers['repository']

        #===
        current_user = have_authenticated_user(request.remote_addr, repository, session_token)
        if current_user is False: return fail(user_auth_fail_msg)

        #===
        data_store = versioned_storage(config['repositories'][repository]['path'],
                                       path_override = config['repositories'][repository]['transient_db_path_override'])

        return success({}, {'versions' : data_store.get_commit_chain()})

    except IOError:
        return fail('Invalid object hash')


#===============================================================================
@route('list_changes')
def list_changes(request: Request, context: ConnectionContext) -> Responce: # pylint: disable=W0613
    try:
        session_token = request.headers['session_token'].encode('utf8')
        repository    = request.headers['repository']

        #===
        current_user = have_authenticated_user(request.remote_addr, repository, session_token)
        if current_user is False: return fail(user_auth_fail_msg)

        #===
        data_store = versioned_storage(config['repositories'][repository]['path'],
                                       path_override = config['repositories'][repository]['transient_db_path_override'])

        show_head = bool(int(request.headers['show_head']))

        if show_head:
            version_id = data_store.get_head()
        else:
            version_id = request.headers['version_id']

        #===
        return success({'version_id' : version_id}, {'changes' : data_store.get_commit_changes(version_id)})

    except IOError:
        return fail('Invalid object hash')


#===============================================================================
@route('list_files')
def list_files(request: Request, context: ConnectionContext) -> Responce: # pylint: disable=W0613
    try:
        session_token = request.headers['session_token'].encode('utf8')
        repository    = request.headers['repository']

        #===
        current_user = have_authenticated_user(request.remote_addr, repository, session_token)
        if current_user is False: return fail(user_auth_fail_msg)

        #===
        data_store = versioned_storage(config['repositories'][repository]['path'],
                                       path_override = config['repositories'][repository]['transient_db_path_override'])

        show_head = bool(int(request.headers['show_head']))

        if show_head:
            version_id = data_store.get_head()
        else:
            version_id = request.headers['version_id']

        return success({'version_id' : version_id}, {'files' : data_store.get_commit_files(version_id)})

    except IOError:
        return fail('Invalid object hash')


#===============================================================================
@route('begin_commit')
def begin_commit(request: Request, context: ConnectionContext) -> Responce:
    """ Allow a client to begin a commit and acquire the write lock """

    session_token = request.headers['session_token'].encode('utf8')
    repository    = request.headers['repository']

    #===
    current_user = have_authenticated_user(request.remote_addr, repository, session_token)
    if current_user is False: return fail(user_auth_fail_msg)

    #===
    repository_path = config['repositories'][repository]['path']

    data_store = versioned_storage(repository_path,
                                   path_override = config['repositories'][repository]['transient_db_path_override'])

    # ==
    have_lock = data_store.lock()
    if not have_lock:
        return fail(lock_fail_msg)

    else:
        context.lock = True
        def unlock():
            if context.lock is True:
                data_store.unlock()
                context.lock = False

        context.shutdown_handler = unlock

    # Commits can only take place if the committing user has the latest revision,
    # as committing from an outdated state could cause unexpected results, and may
    # have conflicts. Conflicts are resolved during a client update so they are
    # handled by the client, and a server interface for this is not needed.
    if data_store.get_head() != request.headers["previous_revision"]:
        if data_store.get_active_commit() is not None: data_store.rollback()
        return fail(need_to_update_msg)

    # if the last active commit was by the same user and ip, get the partial committed files
    # and send them back to allow resume, otherwise do rollback if needed.
    active_commit = data_store.get_active_commit()

    commit_files = {}

    if active_commit is not None:
        if active_commit['user'] == request.headers['user'] and active_commit['ip'] == request.remote_addr:
            commit_files = data_store.get_active_commit_changes()
            commit_files = [it for it in commit_files if it['status'] != 'deleted']
            data_store.begin(request.headers['user'], request.remote_addr, session_token, resume = True)

        else:
            data_store.rollback()
            data_store.begin(request.headers['user'], request.remote_addr, session_token)

    else:
        data_store.begin(request.headers['user'], request.remote_addr, session_token)

    #------------
    return success(data={'partial_commit' : commit_files})


#===============================================================================
@route('push_file')
def push_file(request: Request, context: ConnectionContext) -> Responce:
    """ Push a file to the server """ #NOTE beware that reading post data in flask causes hang until file upload is complete

    session_token = request.headers['session_token'].encode('utf8')
    repository    = request.headers['repository']

    #===
    current_user = have_authenticated_user(request.remote_addr, repository, session_token)
    if current_user is False: return fail(user_auth_fail_msg)

    #===
    repository_path = config['repositories'][repository]['path']

    #===
    if not context.lock:
        return fail(lock_fail_msg)

    #===
    data_store = versioned_storage(repository_path,
                                   path_override = config['repositories'][repository]['transient_db_path_override'])

    if data_store.get_active_commit() is None: return fail(no_active_commit_msg)

    # There is no valid reason for path traversal characters to be in a file path within this system
    file_path = request.headers['path']
    if any(True for item in re.split(r'\\|/', file_path) if item in ['..', '.']): return fail()

    #===
    tmp_path = cpjoin(repository_path, 'tmp_file')
    with open(tmp_path, 'wb') as f:
        while True:
            chunk = request.body.read(1000 * 1000)
            if chunk is None: break
            f.write(chunk)

    #===
    file_info = data_store.fs_put_from_file(tmp_path, {'path' : file_path})

    return success({'file_info_json' : json.dumps(file_info)})


#===============================================================================
@route('delete_files')
def delete_files(request: Request, context: ConnectionContext) -> Responce:
    """ Delete one or more files from the server """

    session_token = request.headers['session_token'].encode('utf8')
    repository    = request.headers['repository']

    #===
    current_user = have_authenticated_user(request.remote_addr, repository, session_token)
    if current_user is False: return fail(user_auth_fail_msg)

    #===
    repository_path = config['repositories'][repository]['path']
    body_data = request.get_json()

    #===
    if not context.lock:
        return fail(lock_fail_msg)

    #===
    try:
        data_store = versioned_storage(repository_path,
                                       path_override = config['repositories'][repository]['transient_db_path_override'])

        if data_store.get_active_commit() is None: return fail(no_active_commit_msg)

        #-------------
        to_delete = json.loads(body_data['files'])
        data_store.fs_delete(to_delete)

        return success()

    except Exception: # pylint: disable=broad-except
        return fail()


#===============================================================================
@route('commit')
def commit(request: Request, context: ConnectionContext) -> Responce:
    """ Commit changes and release the write lock """

    session_token = request.headers['session_token'].encode('utf8')
    repository    = request.headers['repository']

    #===
    current_user = have_authenticated_user(request.remote_addr, repository, session_token)
    if current_user is False: return fail(user_auth_fail_msg)

    #===
    repository_path = config['repositories'][repository]['path']

    #===
    if not context.lock:
        return fail(lock_fail_msg)

    #===
    data_store = versioned_storage(repository_path,
                                   path_override = config['repositories'][repository]['transient_db_path_override'])

    if data_store.get_active_commit() is None: return fail(no_active_commit_msg)

    result = {}
    if request.headers['mode'] == 'commit':
        new_head = data_store.commit(request.headers['commit_message'], current_user['username'])
        result = {'head' : new_head}
    else:
        data_store.rollback()

    # --------------------------
    context.shutdown_handler()
    return success(result)
