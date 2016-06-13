import __builtin__

__builtin__.DATA_DIR       = './vers_test/'
__builtin__.TMP_DIR        = '.tmp/'
__builtin__.BACKUP_DIR       = 'back/'
__builtin__.MANIFEST_FILE  = '.manifest_xzf.json'
__builtin__.JOURNAL_FILE       = '.journal.json'
__builtin__.JOURNAL_STEP_FILE  = '.journal_step'

import os.path as p
import os, shutil, json, errno


############################################################################################
# Journaling file storage subsystem, only use one instance at any time, not thread safe
############################################################################################
class storage():
    j_file = ''
    journal = None
    tmp_idx = 0

    def __init__(self):
        self.j_file = p.join(DATA_DIR, JOURNAL_FILE)
        self.j_step_file = p.join(DATA_DIR, JOURNAL_STEP_FILE)
        try: os.makedirs(p.join(DATA_DIR, TMP_DIR))
        except: pass

    def new_tmp(self):
        self.tmp_idx += 1
        return p.join(DATA_DIR, TMP_DIR, 'tmp_' + str(self.tmp_idx)) 

    def new_backup(self, src):
        try:
            backup_num = int(self.file_get_contents(p.join(DATA_DIR, BACKUP_DIR, '.bk_idx')))
        except:
            backup_num = 1
        backup_name = str(backup_num) + "_" + os.path.basename(src)
        backup_num += 1
        bk_path = p.join(DATA_DIR, BACKUP_DIR)

        try: os.makedirs(bk_path)
        except: pass

        with open(p.join(DATA_DIR, BACKUP_DIR, '.bk_idx'), 'w') as f: 
            f.write(str(backup_num))
        return p.join(bk_path, backup_name)

############################################################################################
# Begin a transaction
############################################################################################
    def begin(self):
        # under normal operation journal is deleted at end of transaction
        # if it does we need to roll back
        if os.path.isfile(self.j_file):  
            self.rollback()

        self.journal = open(DATA_DIR + JOURNAL_FILE, 'w')

############################################################################################
# Do journal rollback
############################################################################################
    def rollback(self):
        journal = self.file_get_contents(self.j_file)

        journ_list = []
        with open(self.j_file) as fle:
            for l in fle:
                journ_list.append(json.loads(l))

        step = 0 # need to check if step file exists

        try: 
            targ_step = int(self.file_get_contents(self.j_step_file))
        except OSError:
           targ_step = 1 

        for j_itm in reversed(journ_list):
            # step is always recorded at the end of each rollback item, if the step file
            #records '2', that means something failed during step 3 and we need to start there.  
            if step > targ_step: 
                print j_itm

            # ------------------------------------------------------
                if j_itm[0] == 'put_contents':
                    src = j_itm[1]
                    tmp = j_itm[2]

                    # If the new file exists, move it to backup dir
                    if os.path.isfile(src):  
                        bk = self.new_backup(src)

                        shutil.move(src, bk)

                    # if tmp file exists move the temp file back to it's original location
                    if tmp != None:
                        if os.path.isfile(tmp):  
                            shutil.move(tmp, src)


            # ------------------------------------------------------
                if j_itm[0] == 'move_file':
                    src = j_itm[1]
                    dst = j_itm[2]

                    if os.path.isfile(dst):  
                        shutil.move(dst, src)


            # ------------------------------------------------------
                if j_itm[0] == 'overwrite_file':
                    src = j_itm[1]
                    dst = j_itm[2]
                    tmp = j_itm[3]

                    if os.path.isfile(dst):  
                        shutil.move(dst, src)

                    if os.path.isfile(dst):  
                        shutil.move(tmp, dst)


            # ------------------------------------------------------
                if j_itm[0] == 'delete_file':
                    src = j_itm[1]
                    tmp = j_itm[2]

                    if os.path.isfile(tmp):  
                        shutil.move(tmp, src)


            # ---------------------------------

            # Record how many journal stages have been stepped back in case something fails
            # during the rollback and we need to pick up later.
            with open(self.j_step_file, 'w') as f: 
                f.write(str(step))
                os.fsync(f)
            
            step += 1


        # Rollback is complete, delete the step file and journal
        os.remove(self.j_step_file)
        os.remove(self.j_file)

############################################################################################
# Finish a transaction
############################################################################################
    def commit(self):
        self.journal.close()
        self.journal = None
        os.remove(self.j_file)

############################################################################################
# Returns contents of file located at 'path', not changing FS so does not require journaling
############################################################################################
    def file_get_contents(self, path):
        if os.path.isfile(path):  
            with open(path, 'r') as f:
                result = f.read()
        else:
            raise OSError(errno.ENOENT, 'No such file or directory', path)

        return result

############################################################################################
# Put passed contents into file located at 'path'
############################################################################################
    def file_put_contents(self, path, data):
        # if file exists, create a temp copy to allow rollback
        if os.path.isfile(path):  
            tmp_path = self.new_tmp()
            shutil.copy(path, tmp_path)

            self.journal.write(json.dumps(['put_contents', path, tmp_path]) + "\n")
            os.fsync(self.journal)
        else:
            self.journal.write(json.dumps(['put_contents', path, None]) + "\n")
            os.fsync(self.journal)

        with open(path, 'w') as f:
            f.write(data)


############################################################################################
# Move file from src to dst
############################################################################################
    def move_file(self, src, dst):
        # record where file moved

        if os.path.isfile(src):  
            if os.path.isfile(dst): # if destination file exists, copy it to tmp first
                tmp_path = self.new_tmp()
                shutil.move(dst, tmp_path)
                self.journal.write(json.dumps(['overwrite_file', src, dst, tmp_path]) + "\n")
                os.fsync(self.journal)
            else: # if dst does not exist, just log the move
                self.journal.write(json.dumps(['move_file', src, dst]) + "\n")
                os.fsync(self.journal)

        shutil.move(src, dst)


############################################################################################
# delete a file
############################################################################################
    def delete_file(self, path):
        # if file exists, create a temp copy to allow rollback
        if os.path.isfile(path):  
            tmp_path = self.new_tmp()

            self.journal.write(json.dumps(['delete_file', path, tmp_path]) + "\n")
            os.fsync(self.journal)

            shutil.move(path, tmp_path)

        else:
            raise OSError(errno.ENOENT, 'No such file or directory', path)


s = storage()
s.begin()
s.file_put_contents(DATA_DIR + 'hello', 'test content')
s.move_file(DATA_DIR + 'hello', DATA_DIR + 'hello2')
#s.file_put_contents(DATA_DIR + 'hello2', 'test content')
s.file_put_contents(DATA_DIR + 'hello3', 'test content')
s.delete_file(DATA_DIR + 'hello3')
s.rollback()





class versioned_storage(storage):
    stepped = False
############################################################################################
# Step version forward once per sync request
############################################################################################
    def __init__(self):
        self.step_version()

############################################################################################
# Step version number forward
############################################################################################
    def step_version(self):
        try:
            head_file = p.join(DATA_DIR, "head")
            head = int(file_get_contents(head_file))

            cur_rv = (p.join(DATA_DIR, head))
            new_rv = (p.join(DATA_DIR, head + 1))

            shutil.move(cur_rv, new_rv)
            os.mkdir(cur_rv)
            shutil.copy(p.join(new_rv, MANIFEST_FILE))

            file_put_contents(head_file, head)
        except:
            head = 1
            cur_rv = (p.join(DATA_DIR, head))
            os.mkdir(cur_rv)
            file_put_contents(head_file, head)

############################################################################################
# Add a file to the FS
############################################################################################
    def new_file(self, tmp_path, sys_path):
        head_file = p.join(DATA_DIR, "head")
        head = int(file_get_contents(head_file))

    # add new file to current revision
        cur_rv = (p.join(DATA_DIR, head))
        shutil.move(tmp_path, p.join(cur_rv, sys_path))

        manifest = json.loads(file_get_contents(p.join(cur_rv, MANIFEST_FILE)))
        manifest['files'].append(get_file_info(p.join(cur_rv, sys_path)))
        file_put_contents(p.join(cur_rv, MANIFEST_FILE), json.dumps(manifest))


############################################################################################
# Update a file in the FS (must step before using this)
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
















