#===============================================================================
# Authentication
#===============================================================================
auth_db_initilised = False
def auth_db_connect(db_path: str) -> db.Connection:
    """ An SQLite database is used to store authentication transient data,
    this is tokens, strings of random data which are signed by the client,
    and session_tokens which identify authenticated users """

    global auth_db_initilised

    def dict_factory(cursor, row): return {col[0] : row[idx] for idx,col in enumerate(cursor.description)}
    conn = db.connect(db_path)
    conn.row_factory = dict_factory
    if not auth_db_initilised:
        conn.execute('create table if not exists tokens (expires int, token text, ip text)')
        conn.execute('create table if not exists session_tokens (expires int, token text, ip text, username text)')
        auth_db_initilised = True
    return conn



#===============================================================================
def issue_token(conn):
    conn = auth_db_connect(cpjoin(repository_path, 'auth_transient.db')); gc_tokens(conn)

    # Issue a new token
    auth_token = base64.b64encode(pysodium.randombytes(35)).decode('utf-8')
    conn.execute("insert into tokens (expires, token, ip) values (?,?,?)",
                 (time.time() + 30, auth_token, request.remote_addr))
    conn.commit()

    return auth_token


#===============================================================================
def validate_token():
    conn.execute("delete from session_tokens where expires < ?", (time.time(),)); conn.commit()
    res = conn.execute("select * from session_tokens where token = ? and ip = ?", (session_token, client_ip)).fetchall()
    if res != []: return success({'session_token'  : session_token})
    else:         return fail(user_auth_fail_msg)


#===============================================================================
def create_session(conn):

    # Everything OK
    conn.execute("delete from tokens where token = ?", (auth_token,)); conn.commit()

    # generate a session token and send it to the client
    session_token = base64.b64encode(pysodium.randombytes(35))
    conn.execute("insert into session_tokens (token, expires, ip, username) values (?,?,?, ?)",
                 (session_token, time.time() + extend_session_duration, client_ip, user))
    conn.commit()


#===============================================================================
def gc_tokens(conn):
    """ Garbage collection for expired authentication tokens """

    conn.execute("delete from tokens where expires < ?", (time.time(),))
    conn.commit()
# commit transient data


#===============================================================================
# Storage for transient commit data
#===============================================================================
def begin_commit():
    # TODO should be storing this transient data in sqlite as it would perform way better

    # Active commit files stores all of the files which will be in this revision,
    # including ones carried over from the previous revision
    sfs.file_put_contents(sfs.cpjoin(self.base_path, 'active_commit_files'), bytes(json.dumps(active_files), encoding='utf8'))

    # Active commit changes stores a log of files which have been added, changed
    # or deleted in this revision
    sfs.file_put_contents(sfs.cpjoin(self.base_path, 'active_commit_changes'), bytes(json.dumps([]), encoding='utf8'))

    # Store that there is an active commit
    sfs.file_put_contents(sfs.cpjoin(self.base_path, 'active_commit'), b'true')


#===============================================================================
def begin_commit():
    pass




#===============================================================================
def gc_log_item(self, item_type: str, item_hash: str) -> None:
    with open(sfs.cpjoin(self.base_path, 'gc_log'), 'a') as gc_log:
        gc_log.write(item_type + ' ' + item_hash + '\n'); gc_log.flush()


#===============================================================================
def begin_commit():
    #=======================================================
    # Update commit changes
    #=======================================================
    def helper(contents):
        file_info['status'] = 'changed' if file_info['path'] in contents else 'new'
        return  contents + [file_info]
    self.update_system_file('active_commit_changes', helper)

    #=======================================================
    # Update commit files
    #=======================================================
    def helper2(contents):
        contents[file_info['path']] = file_info
        return contents
    self.update_system_file('active_commit_files', helper2)

    return file_info


#===============================================================================
def fs_delete():
    #=======================================================
    # Check if the file actually exists in the commit
    #=======================================================
    file_exists = False
    def helper2(contents):
        nonlocal file_exists
        file_exists = file_info['path'] in contents
        return contents
    self.update_system_file('active_commit_files', helper2)

    if not file_exists: return

    #=======================================================
    # Update commit changes
    #=======================================================
    def helper(contents):
        file_info['status'] = 'deleted'
        return  contents + [file_info]
    self.update_system_file('active_commit_changes', helper)

    #=======================================================
    # Update commit files
    #=======================================================
    def helper2(contents):
        del contents[file_info['path']]
        return contents
    self.update_system_file('active_commit_files', helper2)


#===============================================================================
def get_changes():
    current_changes = json.loads(sfs.file_get_contents(sfs.cpjoin(self.base_path, 'active_commit_changes')))
    active_files    = json.loads(sfs.file_get_contents(sfs.cpjoin(self.base_path, 'active_commit_files')))


#===============================================================================
def commit_changes():
    #and clean up working state
    os.remove(sfs.cpjoin(self.base_path, 'active_commit_changes'))
    os.remove(sfs.cpjoin(self.base_path, 'active_commit_files'))
    sfs.ignore(os.remove, sfs.cpjoin(self.base_path, 'gc_log'))
    os.remove(sfs.cpjoin(self.base_path, 'active_commit'))


#===============================================================================
def get_gc_log():
    gc_log_contents: str = sfs.file_or_default(sfs.cpjoin(self.base_path, 'gc_log'), b'').decode('utf8')


#===============================================================================
def rollback_cleanup():
    sfs.ignore(os.remove, sfs.cpjoin(self.base_path, 'active_commit_changes'))
    sfs.ignore(os.remove, sfs.cpjoin(self.base_path, 'active_commit_files'))
    sfs.ignore(os.remove, sfs.cpjoin(self.base_path, 'gc_log'))
    os.remove(sfs.cpjoin(self.base_path, 'active_commit')) # if this is being called, this file should always exist


