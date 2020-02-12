import os.path as p
import os, shutil, json, errno
from collections import deque
from typing import Union, TextIO

from shttpfs3.common import cpjoin, ignore, file_or_default, file_put_contents

############################################################################################
# Journaling file storage subsystem, only use one instance at any time, not thread safe
############################################################################################
class storage(object):

############################################################################################
    def __init__(self, data_dir: str, conf_dir: str):
        self.data_dir    = data_dir if data_dir[-1] == '/' else data_dir + '/'
        self.j_file      = self.get_full_file_path(conf_dir, 'journal.json')
        self.tmp_dir     = self.get_full_file_path(conf_dir, 'tmp')
        self.backup_dir  = self.get_full_file_path(conf_dir, 'back')
        self.journal: Union[None, TextIO] = None
        self.tmp_idx     = 0

        ignore(os.makedirs, self.tmp_dir)    # Make sure tmp dir exists
        ignore(os.makedirs, self.backup_dir) # Make sure backup dir exists

############################################################################################
    def get_full_file_path(self, *args):
        """ make path relative to DATA DIR from a system relative path """

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

        if self.journal != None:
            raise 

        # under normal operation journal is deleted at end of transaction
        # if it does exist we need to roll back
        if os.path.isfile(self.j_file):  self.rollback()

        self.journal = open(self.j_file, 'w')

############################################################################################
    def do_action(self, command: dict, journal: bool = True):
        """ Implementation for declarative file operations. """

        if self.journal is None: Exception('Must call begin first')

        cmd = 0; src = 1; path = 1; data = 2; dst = 2

        if journal is True:
            self.journal.write(json.dumps(command['undo']) + "\n") # type: ignore
            self.journal.flush()                                   # type: ignore

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

        # Close the journal for writing, if this is an automatic rollback following a crash,
        # the file descriptor will not be open, so don't need to do anything.
        if self.journal != None: self.journal.close()
        self.journal = None

        # Read the journal
        journ_list = []
        with open(self.j_file) as fle:
            for l in fle: journ_list.append(json.loads(l))

        journ_subtract = deque(reversed(journ_list))

        for j_itm in reversed(journ_list):
            try: self.do_action({'do' : j_itm}, False)
            except IOError: pass

            # As each item is completed remove it from the journal file, in case
            # something fails during the rollback we can pick up where it stopped.
            journ_subtract.popleft()
            with open(self.j_file, 'w') as f:
                for data in list(journ_subtract):
                    f.write(json.dumps(data) + "\n")
                f.flush()

        # Rollback is complete so delete the journal file
        os.remove(self.j_file)

############################################################################################
    def commit(self, cont: bool = False):
        """ Finish a transaction """

        if self.journal is None: Exception('Must call begin first')

        self.journal.close() # type: ignore
        self.journal = None
        os.remove(self.j_file)

        for itm in os.listdir(self.tmp_dir): os.remove(cpjoin(self.tmp_dir, itm))

        if cont is True: self.begin()

############################################################################################
    def file_get_contents(self, path: str) -> bytes:
        """ Returns contents of file located at 'path', not changing FS so does
        not require journaling """

        with open(self.get_full_file_path(path), 'r') as f: return  f.read()

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

