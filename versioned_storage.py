import __builtin__

__builtin__.DATA_DIR       = './vers_test/'
__builtin__.TMP_DIR        = '.tmp/'
__builtin__.BACKUP_DIR       = 'back/'
__builtin__.MANIFEST_FILE  = '.manifest_xzf.json'
__builtin__.JOURNAL_FILE       = '.journal.json'
__builtin__.JOURNAL_STEP_FILE  = '.journal_step'

import os.path as p
import os, shutil, json, errno
import pdb

from storage import *


############################################################################################
# Relative storage layer, all paths are rooted to DATA_DIR
############################################################################################
class rel_storage(storage):
############################################################################################
# Relative file put contents
############################################################################################
    def r_put(self, rpath, data):
        return self.file_put_contents(self.mkfs_path(rpath), data)

############################################################################################
# Relative file get contents
############################################################################################
    def r_get(self, rpath):
        return self.file_get_contents(self.mkfs_path(rpath))

############################################################################################
# Relative file get contents
############################################################################################
    def r_move(self, src, dst):
        return self.move_file(self.mkfs_path(src), self.mkfs_path(dst))

############################################################################################
# Relative list dir
############################################################################################
    def r_listdir(self, rpath):
        return os.listdir(self.mkfs_path(rpath))

############################################################################################
# Relative is file
############################################################################################
    def r_isfile(self, rpath):
        return os.path.isfile(self.mkfs_path(rpath))

############################################################################################
# Relative make multiple dirs
############################################################################################
    def r_makedirs(self, rpath):
        return os.makedirs(self.mkfs_path(rpath))







############################################################################################
# Versioning data store
############################################################################################
class versioned_storage(rel_storage):
    stepped   = False
    vrs_dir  = ''
    head_file = ''

############################################################################################
# Get the number of the head revision
############################################################################################
    def get_head(self):
        return int(self.r_get(self.head_file))

############################################################################################
# Setup and validate file system structure
############################################################################################
    def __init__(self, data_dir, j_file, j_step_file, tmp_dir, backup_dir):
        storage.__init__(self, data_dir, j_file, j_step_file, tmp_dir, backup_dir)

        self.vrs_dir  = "versions"
        self.head_file = "head"

    # find all existing versions
        versions = None
        try: 
            versions = self.r_listdir(self.vrs_dir)
            versions = [int(ver) for ver in versions] 
        except ValueError:
            raise Exception('Error: non integer directory name in versions.') 
        except: pass

    # If versions is none, dir does not exist. Create it, an initial version and
    # record in the head file.
        actual_head = None
        if versions == None:
            self.r_makedirs(p.join(self.vrs_dir, '1'))

            self.begin()
            self.r_put(self.head_file, '1')
            self.commit()

            actual_head = 1

    # If versions dir does not exist, make sure head file exists and that it contains the latest
    # version number. Correct it if anything is wrong.
        else:
            actual_head = max(versions)

            try:
                head = self.get_head()
                if head != actual_head:
                    self.begin()
                    self.r_put(self.head_file, str(actual_head))
                    self.commit()

            except:
                self.begin()
                self.r_put(self.head_file, str(actual_head))
                self.commit()
            
############################################################################################
# Step version number forward
############################################################################################
    def step_version(self):
        self.begin()

        try:
            head = self.get_head()

            cur_rv = (p.join(self.vrs_dir, str(head)))
            new_rv = (p.join(self.vrs_dir, str(head + 1)))

            # move current head forward one step, then make an empty prior revision
            self.r_move(cur_rv, new_rv)
            self.r_makedirs(cur_rv)

            self.r_put(p.join(cur_rv, MANIFEST_FILE), '')

            head += 1
            self.r_put(self.head_file, str(head))
            self.commit()

        except:
            self.rollback()
            raise

############################################################################################
# If a file exists in the current revision, move it to the parent revision. Step version
# if doing so would overwrite a file in the parent.
############################################################################################
    def step_if_exists(self, rpath):

        head = self.get_head()

        # does target file exist in current rv?
        exists = False
        if self.r_isfile(p.join(self.vrs_dir, str(head), rpath)):
            exists = True
            # Does the file exist in the parent rv?
            if head == 1 or self.r_isfile(p.join(self.vrs_dir, str(head - 1), rpath)):
                # if yes, step revision
                self.step_version()

        # reload head in case we stepped version
        head = self.get_head()


        self.begin()

        # If there is a current file, move it back to prior RV
        if exists == True:
            self.r_move(p.join(self.vrs_dir, str(head), rpath), p.join(self.vrs_dir, str(head - 1), rpath))

        self.commit()

        return exists


############################################################################################
# Add a file to the FS
############################################################################################
    def fs_put(self, rpath, data):
        
        try:
            # does file exist in current rv? If it does move it to prior rv
            self.step_if_exists(rpath)

            self.begin()
            # reload head in case we stepped version
            head = self.get_head()

            # Add the file to the current revision
            self.r_put(p.join(self.vrs_dir, str(head), rpath), data)

            # Deal with the manifests...

            #manifest = json.loads(file_get_contents(p.join(cur_rv, MANIFEST_FILE)))
            #manifest['files'].append(get_file_info(p.join(cur_rv, sys_path)))
            #file_put_contents(p.join(cur_rv, MANIFEST_FILE), json.dumps(manifest))

        except:
            raise
            self.rollback()

        self.commit()

############################################################################################
# Get a files contents from the FS
############################################################################################
    def fs_get(self, rpath):
        return self.r.get(rpath)

############################################################################################
# Move a file in the FS
############################################################################################
    def fs_move(self, r_src, r_dst):
        
        try:
            # does target file exist in current rv? If it does move it to prior rv
            self.step_if_exists(r_dst)

            self.begin()

            # reload head in case we stepped version
            head = self.get_head()

            cur_vrs = p.join(self.vrs_dir, str(head))

            # Move the file
            self.r_move(p.join(cur_vrs, r_src), p.join(cur_vrs, r_dst))

            # Deal with the manifests...

            #manifest = json.loads(file_get_contents(p.join(cur_rv, MANIFEST_FILE)))
            #manifest['files'].append(get_file_info(p.join(cur_rv, sys_path)))
            #file_put_contents(p.join(cur_rv, MANIFEST_FILE), json.dumps(manifest))

        except:
            self.rollback()
            raise

        self.commit()


############################################################################################
# Remove file from current revision
############################################################################################
    def fs_delete(self, rpath):
        try:
            # does target file exist in current rv? If it does move it to prior rv
            self.step_if_exists(rpath)

            # do not need to do anything else, step_if_exists moves the file into the
            # previous revision so it is effectively 'deleted' from the current one.


            #self.begin()

            # reload head in case we stepped version
            #head = self.get_head()

            #cur_vrs = p.join(self.vrs_dir, str(head))

            # Delete the file
            #self.r_delete(p.join(cur_vrs, r_src), p.join(cur_vrs, r_dst))


            # Deal with the manifests...

            #manifest = json.loads(file_get_contents(p.join(cur_rv, MANIFEST_FILE)))
            #manifest['files'].append(get_file_info(p.join(cur_rv, sys_path)))
            #file_put_contents(p.join(cur_rv, MANIFEST_FILE), json.dumps(manifest))


        except:
            raise
            #self.rollback()

        #self.commit()

