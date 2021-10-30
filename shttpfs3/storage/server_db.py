import sqlite3, base64, pysodium, threading, time
from threading import current_thread

from shttpfs3.common import cpjoin, file_get_contents

#=====================================================
threadLocal = threading.local()

def get_server_db_instance_for_thread(db_file_path, need_to_recreate = False):

    old_db_path = getattr(threadLocal, 'old_db_path', '')
    if old_db_path != db_file_path:
        need_to_recreate = True

    db_instance = getattr(threadLocal, 'db_instance', None)
    if db_instance is None:
        need_to_recreate = True

    if need_to_recreate:
        threadLocal.db_instance = server_db(db_file_path)
        threadLocal.old_db_path = db_file_path

    return threadLocal.db_instance
    

#=====================================================
class server_db:
    def __init__(self, base_path : str):

        def dict_factory(cursor, row):
            d = {}
            for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
            return d

        db_file_path = cpjoin(base_path, 'server_transient.db')
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
                token text unique,
                ip text
            )
            """)

        self.con.execute("""
            create table if not exists session_tokens (
                expires int,
                token text unique,
                ip text,
                username text
            )
            """)


        # Stores a list of all files in the current commit
        self.con.execute("""
            create table if not exists gc_log (
                item_type  Text,
                item_hash  Text
            )
            """)

        # Stores a list of all files in the current commit
        self.con.execute("""
            create table if not exists active_commit_files (
                hash    Text,
                path    Text unique,
                status  Text
            )
            """)

        # Active commit changes stores a log of files which have been added, changed
        # or deleted in this revision
        self.con.execute("""
            create table if not exists active_commit_changes (
                hash    Text,
                path    Text unique,
                status  Text
            )
            """)

        # Table to store if there is an active commit
        self.con.execute("""
            create table if not exists active_commit_exists (
                state  int
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
    def have_active_commit(self):
        res = self.con.execute("select * from active_commit_exists").fetchall()
        return res != []


    #===============================================================================
    def begin_commit(self, active_files):
        # Add the existing files to the commit
        for path, file_info in active_files.items():
            res = self.con.execute("insert into active_commit_files (hash, path, status) values (?, ?, ?)",
                                   (file_info['hash'], file_info['path'], file_info['status']))

        # Ensure commit changes is empty
        res = self.con.execute("delete from active_commit_changes")

        # flag that there is a commit in progress
        self.con.execute("insert into active_commit_exists (state) values (1)")

        # ----------
        self.con.commit()
        

    #===============================================================================
    def file_exists_in_commit(self, file_path):
        res = self.con.execute("select * from active_commit_files where path = ?", (file_path,))
        return res.fetchall()
        

    #===============================================================================
    def add_to_commit(self, file_info):

        file_info['status'] = 'changed' if self.file_exists_in_commit (file_info['path']) != [] else 'new'

        # Update commit changes
        res = self.con.execute("insert into active_commit_changes (hash, path, status) values (?, ?, ?)",
                               (file_info['hash'], file_info['path'], file_info['status']))

        # Update commit files
        if self.file_exists_in_commit(file_info['path']) == []:
            res = self.con.execute("insert into active_commit_files (hash, path, status) values (?, ?, ?)",
                                   (file_info['hash'], file_info['path'], file_info['status']))
        else:
            res = self.con.execute("update active_commit_files set hash = ?, status = ? where path = ?",
                                   (file_info['hash'], file_info['status'], file_info['path']))

        self.con.commit()

        return file_info


    #===============================================================================
    def remove_from_commit(self, file_path):

        # We don't need to do anything if the file doesnt exist
        the_file = self.file_exists_in_commit(file_path)
        if the_file == []: return
        file_info = the_file[0]

        # Update commit changes
        file_info['status'] = 'deleted'
        res = self.con.execute("insert into active_commit_changes (hash, path, status) values (?, ?, ?)",
                               (file_info['hash'], file_info['path'], file_info['status']))

        # Update commit files
        res = self.con.execute("delete from active_commit_files where path = ?",
                                (file_info['path'],))

        self.con.commit()


    #===============================================================================
    def get_commit_state(self):
        current_changes = self.con.execute("select * from active_commit_changes").fetchall()
        active_files    = self.con.execute("select * from active_commit_files").fetchall()
        active_files = {fle['path'] : fle for fle in active_files}

        return current_changes, active_files


    #===============================================================================
    def clean_commit_state(self):
        self.con.execute("delete from active_commit_changes")
        self.con.execute("delete from active_commit_files")
        self.con.execute("delete from gc_log")
        self.con.execute("delete from active_commit_exists")


#===============================================================================
# Storage of GC log
#===============================================================================
    def gc_log_item(self, item_type: str, item_hash: str) -> None:
        self.con.execute("insert into gc_log (item_type, item_hash) values (?,?)",
                         (item_type, item_hash))
        self.con.commit()


    #===============================================================================
    def get_gc_log(self):
        return self.con.execute("select * from gc_log").fetchall()