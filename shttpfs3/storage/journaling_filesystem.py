import os.path as p
import os, shutil, json, errno
from collections import deque
from typing import Union, TextIO

from shttpfs3.common import cpjoin, ignore, file_or_default, file_put_contents


############################################################################################
# Journaling filesystem interface which allows changes to multiple files to be performed
# atomically. This code is not thread safe so only use one instance at any time.
############################################################################################
class journaling_filesystem:

############################################################################################
    def __init__(self, client_db, data_dir: str, conf_dir: str):
        self.client_db  = client_db
        self.data_dir   = data_dir if data_dir[-1] == '/' else data_dir + '/'
        self.tmp_dir    = self.get_full_file_path(conf_dir, 'tmp')
        self.backup_dir = self.get_full_file_path(conf_dir, 'back')

        ignore(os.makedirs, self.tmp_dir)    # Make sure tmp dir exists
        ignore(os.makedirs, self.backup_dir) # Make sure backup dir exists

        self.tmp_idx = 0
        self.active_transaction = False

############################################################################################
    def get_full_file_path(self, *args):
        """ make a path relative to DATA DIR from a system relative path """

        return cpjoin(self.data_dir, *args)

############################################################################################
    def new_tmp(self):
        """ Create a new temp file allocation """

        self.tmp_idx += 1
        return p.join(self.tmp_dir, 'tmp_' + str(self.tmp_idx))

############################################################################################
    def new_backup(self, src: str):
        """ Create a new backup file allocation """

        backup_id_file = p.join(self.backup_dir, '.bk_idx')

        backup_num = int(file_or_default(backup_id_file, b'1'))
        backup_name = str(backup_num) + "_" + os.path.basename(src)
        backup_num += 1

        file_put_contents(backup_id_file, bytes(str(backup_num), encoding='utf8'))
        return p.join(self.backup_dir, backup_name)

############################################################################################
    def begin(self):
        """ Begin a transaction """

        if self.active_transaction is True:
            raise Exception('Multiple Begin not allowed')

        # under normal operation journal is deleted at end of transaction
        # if it does exist we need to roll back
        if self.client_db.get_fs_journal() != []: 
            self.rollback()

        self.active_transaction = True

############################################################################################
    def do_action(self, command: dict, write_journal: bool = True):
        """ Implementation for declarative file operations. """


        cmd = 0; src = 1; path = 1; data = 2; dst = 2

        if write_journal:
            self.client_db.write_fs_journal(json.dumps(command['undo']))
            self.client_db.commit()

        d = command['do']
        if   d[cmd] == 'copy':   shutil.copy(d[src], d[dst])
        elif d[cmd] == 'move':   shutil.move(d[src], d[dst])
        elif d[cmd] == 'backup': shutil.move(d[src], self.new_backup(d[src]))
        elif d[cmd] == 'write' :
            if callable(d[data]): d[data](d[path])
            else: file_put_contents(d[path], d[data])

############################################################################################
    def rollback(self):
        """ Do journal rollback """

        journ_list = self.client_db.get_fs_journal()

        for j_itm in reversed(journ_list):
            try: self.do_action({'do' : j_itm['data']}, write_journal = False)
            except IOError: pass

            # As each item is completed remove it from the journal. In case
            # something fails during the rollback we can pick up where it stopped.
            self.client_db.delete_from_fs_journal(j_itm)
            self.client_db.commit()

        # Rollback is complete, ensure that the journal is now empty.
        if self.client_db.get_fs_journal() != []:
            raise Exception('Filesystem rollback is complete but the journal is not empty')


############################################################################################
    def commit(self):
        """ Finish a transaction """

        if not self.active_transaction:
            raise Exception('Must call begin first')

        self.client_db.clear_fs_journal()
        self.client_db.commit()

        # Clear out the tmp files as they are no longer needed
        for itm in os.listdir(self.tmp_dir):
            os.remove(cpjoin(self.tmp_dir, itm))

        # ------------------
        self.active_transaction = False


############################################################################################
    def file_get_contents(self, path: str) -> bytes:
        """ Returns contents of file located at 'path', not changing FS so does
        not require journaling """

        with open(self.get_full_file_path(path), 'rb') as f: return  f.read()

############################################################################################
    def file_put_contents(self, path: str, data: bytes):
        """ Put passed contents into file located at 'path' """

        path = self.get_full_file_path(path)

        # if file exists, create a temp copy to allow rollback
        if os.path.isfile(path):
            tmp_path = self.new_tmp()
            self.do_action({
                'do'   : ['copy', path, tmp_path],
                'undo' : ['move', tmp_path, path]})

        self.do_action(
            {'do'   : ['write', path, data],
             'undo' : ['backup', path]})

############################################################################################
    def move_file(self, src: str, dst: str):
        """ Move file from src to dst """

        src = self.get_full_file_path(src); dst = self.get_full_file_path(dst)

        # record where file moved
        if os.path.isfile(src):
            # if destination file exists, copy it to tmp first
            if os.path.isfile(dst):
                tmp_path = self.new_tmp()
                self.do_action({
                    'do'   : ['copy', dst, tmp_path],
                    'undo' : ['move', tmp_path, dst]})

        self.do_action(
            {'do'   : ['move', src, dst],
             'undo' : ['move', dst, src]})

############################################################################################
    def delete_file(self, path: str):
        """ delete a file """

        path = self.get_full_file_path(path)

        # if file exists, create a temp copy to allow rollback
        if os.path.isfile(path):
            tmp_path = self.new_tmp()
            self.do_action({
                'do'   : ['move', path, tmp_path],
                'undo' : ['move', tmp_path, path]})

        else:
            raise OSError(errno.ENOENT, 'No such file or directory', path)
