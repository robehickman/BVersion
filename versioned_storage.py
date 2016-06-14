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
# Relative list dir
############################################################################################
    def r_listdir(self, rpath):
        return os.listdir(self.mkfs_path(rpath))

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
    vrs_path  = ''
    head_file = ''

############################################################################################
# Setup and validate file system structure
############################################################################################
    def __init__(self):
        storage.__init__(self)

        self.vrs_path  = "versions"
        self.head_file = "head"

    # find all existing versions
        versions = None
        try: 
            versions = self.r_listdir(self.vrs_path)
            versions = [int(ver) for ver in versions] 
        except ValueError:
            raise Exception('Error: non integer directory name in versions.') 
        except: pass

    # If versions is none, dir does not exist. Create it, an initial version and
    # record in the head file.
        actual_head = None
        if versions == None:
            self.r_makedirs(p.join(self.vrs_path, '1'))

            self.begin()
            self.r_put(self.head_file, '1')
            self.commit()

            actual_head = 1

    # If versions dir does not exist, make sure head file exists and that it contains the latest
    # version number. Correct it if anything is wrong.
        else:
            actual_head = max(versions)

            try:
                head = int(self.r_get(self.head_file))
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
            head = int(self.file_get_contents(self.head_file))

            cur_rv = (p.join(self.vrs_dir, head))
            new_rv = (p.join(self.vrs_dir, head + 1))

            # move current head forward one step, then make an empty prior revision
            self.r_move(cur_rv, new_rv)
            self.r_mkdir(cur_rv)

            self.r_put(p.join(cur_rv, MANIFEST_FILE), '')

            self.r_put(self.head_file, head)
            self.commit()
        except:
            raise
            self.rollback()

############################################################################################
# Add a file to the FS
############################################################################################
    def new_file(self, tmp_path, sys_path):
        head_file = p.join(DATA_DIR, "head")
        head = int(file_get_contents(head_file))

        # does file exist in current rv?
            # if yes
                # Does the file exist in the parent rv?
                    # if yes, step revision
                    # if no, move current file back to prior RV
            #if no add the file to the current revision

    # add new file to current revision
        cur_rv = (p.join(DATA_DIR, head))
        shutil.move(tmp_path, p.join(cur_rv, sys_path))

        manifest = json.loads(file_get_contents(p.join(cur_rv, MANIFEST_FILE)))
        manifest['files'].append(get_file_info(p.join(cur_rv, sys_path)))
        file_put_contents(p.join(cur_rv, MANIFEST_FILE), json.dumps(manifest))


############################################################################################
# Update a file in the FS
############################################################################################
    def update_file(self, tmp_path, sys_path):
        if(stepped == False):
            return

        head_file = p.join(DATA_DIR, "head")
        head = int(file_get_contents(head_file))

        cur_rv = (p.join(DATA_DIR, head))
        old_rv = (p.join(DATA_DIR, head - 1))


    # Copy old file to prior revision
        shutil.move(p.join(cur_rv, sys_path), p.join(old_rv, sys_path))

        manifest = json.loads(file_get_contents(p.join(old_rv, MANIFEST_FILE)))
        manifest['files'].append(get_file_info(p.join(old_rv, sys_path)))
        file_put_contents(p.join(old_rv, MANIFEST_FILE), json.dumps(manifest))


    # Replace current revision file with prior file
        shutil.move(tmp_path, p.join(cur_rv, sys_path))

        manifest = json.loads(file_get_contents(p.join(cur_rv, MANIFEST_FILE)))
        manifest['files'].append(get_file_info(p.join(cur_rv, sys_path)))
        file_put_contents(p.join(cur_rv, MANIFEST_FILE), json.dumps(manifest))


############################################################################################
# Remove file from current revision
############################################################################################
    def delete_file(self, sys_path):
        if(stepped == False):
            return

        head_file = p.join(DATA_DIR, "head")
        head = int(file_get_contents(head_file))

        cur_rv = (p.join(DATA_DIR, head))
        old_rv = (p.join(DATA_DIR, head - 1))

    #Move file from current revision to previous revision.
        shutil.move(tmp_path, p.join(cur_rv, sys_path))

        manifest = json.loads(file_get_contents(p.join(cur_rv, MANIFEST_FILE)))
        manifest['files'].remove(get_file_info(p.join(cur_rv, sys_path)))
        file_put_contents(p.join(cur_rv, MANIFEST_FILE), json.dumps(manifest))



store = versioned_storage()
store.step_version()
