import sqlite3, base64, pysodium, threading, time
from threading import current_thread

#=====================================================
threadLocal = threading.local()

def get_server_db_instance_for_thread(db_file_path):
    db_instance = getattr(threadLocal, 'db_instance', None)
    if db_instance is None:
        threadLocal.db_instance = server_db(db_file_path)

    return threadLocal.db_instance
    

#=====================================================
class server_db:
    def __init__(self, db_file_path : str):

        def dict_factory(cursor, row):
            d = {}
            for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
            return d

        con = sqlite3.connect(db_file_path)
        con.row_factory = dict_factory

        cur = con.cursor()

        self.con = con
        self.cur = cur

#===============================================================================
# Set up tables if they don't exist already.
#===============================================================================
    def db_init(self):
        # Authentication tables
        self.con.execute("""
            create table if not exists tokens (
                expires int,
                token text,
                ip text
            )
            """)

        self.con.execute("""
            create table if not exists session_tokens (
                expires int,
                token text,
                ip text,
                username text
            )
            """)

        # Stores a list of all files in the current commit
        self.con.execute("""
            create table if not exists active_commit_files (
                path     Text,
                last_mod Text,
                created  Text
            )
            """)

        # Active commit changes stores a log of files which have been added, changed
        # or deleted in this revision
        self.con.execute("""
            create table if not exists active_commit_changes (
                path     Text,
                last_mod Text,
                created  Text
            )
            """)

        # Store that there is an active commit
        self.con.execute("""
            create table if not exists active_commit_changes (
                user     Text
            )
            """)

        self.con.commit()

#===============================================================================
# Authentication tokens
#===============================================================================
    def issue_token(self, remote_addr):
        auth_token = base64.b64encode(pysodium.randombytes(35)).decode('utf-8')

        self.con.execute("insert into tokens (expires, token, ip) values (?,?,?)",
                        (time.time() + 30, auth_token, remote_addr))
        self.con.commit()

        return auth_token

    #===============================================================================
    def get_token(self, auth_token, client_ip):
        return self.con.execute("select * from tokens where token = ? and ip = ? ", (auth_token, client_ip)).fetchall()

    #===============================================================================
    def delete_token(self, auth_token):
        self.con.execute("delete from tokens where token = ?", (auth_token,))
        self.con.commit()

    #===============================================================================
    def gc_tokens(self):
        """ Garbage collection for expired authentication tokens """

        self.con.execute("delete from tokens where expires < ?", (time.time(),))
        self.con.commit()

        
#===============================================================================
# Session management
#===============================================================================
    def create_session(self, extend_session_duration, client_ip, user):
        session_token = base64.b64encode(pysodium.randombytes(35))
        self.con.execute("insert into session_tokens (token, expires, ip, username) values (?,?,?,?)",
                        (session_token, time.time() + extend_session_duration, client_ip, user))
        self.con.commit()

        return session_token


    #===============================================================================
    def get_session_token(self, session_token, client_ip):
        return self.con.execute("select * from session_tokens where token = ? and ip = ?",
                            (session_token, client_ip)).fetchall()


    #===============================================================================
    def update_session_token_expiry(self, extend_session_duration, session_token, client_ip):
        self.con.execute("update session_tokens set expires = ? where token = ? and ip = ?",
                         (time.time() + extend_session_duration, session_token, client_ip))


    #===============================================================================
    def gc_session_tokens(self, active_commit):
        if active_commit is not None:
            self.con.execute("delete from session_tokens where expires < ? and token != ?",
                             (time.time(), active_commit))

        else:
            self.con.execute("delete from session_tokens where expires < ?",
                             (time.time(),))


    #===============================================================================
    def validate_session(self, session_token : str, client_ip : str):
        self.con.execute("delete from session_tokens where expires < ?", (time.time(),))
        self.con.commit()

        res = self.con.execute("select * from session_tokens where token = ? and ip = ?", (session_token, client_ip)).fetchall()
        return res


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


