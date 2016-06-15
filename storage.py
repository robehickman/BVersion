import os.path as p
import os, shutil, json, errno

############################################################################################
# Journaling file storage subsystem, only use one instance at any time, not thread safe
############################################################################################
class storage(object):
    data_dir = ''
    tmp_dir = ''
    j_file = ''
    j_step_file = ''
    journal = None
    tmp_idx = 0

############################################################################################
# init
############################################################################################
    def __init__(self, data_dir, j_file, j_step_file, tmp_dir, backup_dir):
        self.data_dir    = data_dir
        self.j_file      = self.mkfs_path(j_file)
        self.j_step_file = self.mkfs_path(j_step_file)
        self.tmp_dir     = self.mkfs_path(tmp_dir)
        self.backup_dir  = self.mkfs_path(backup_dir)

    # Make sure tmp dir exists
        try: os.makedirs(self.tmp_dir)
        except: pass

    # Make sure backup dir exists
        try: os.makedirs(self.backup_dir)
        except: pass

############################################################################################
# write to journal
############################################################################################
    def to_journel(self, data):
        self.journal.write(json.dumps(data) + "\n")
        self.journal.flush()

############################################################################################
# make path relative to DATA DIR from a system relative path
############################################################################################
    def mkfs_path(self, *args):
        return p.join(self.data_dir, *args)

############################################################################################
# Create a new temp file allocation
############################################################################################
    def new_tmp(self):
        self.tmp_idx += 1
        return p.join(self.tmp_dir, 'tmp_' + str(self.tmp_idx)) 

############################################################################################
# Create a new backup file allocation
############################################################################################
    def new_backup(self, src):
        backup_id_file = p.join(self.backup_dir, '.bk_idx')
        try:
            backup_num = int(self.file_get_contents(backup_id_file))
        except:
            backup_num = 1

        backup_name = str(backup_num) + "_" + os.path.basename(src)
        backup_num += 1

        try: os.makedirs(bk_path)
        except: pass

        with open(backup_id_file, 'w') as f: 
            f.write(str(backup_num))
        return p.join(self.backup_dir, backup_name)

############################################################################################
# Begin a transaction
############################################################################################
    def begin(self):
        if self.journal != None:
            raise Exception('Storage is already active, nested begin not supported')

        # under normal operation journal is deleted at end of transaction
        # if it does we need to roll back
        if os.path.isfile(self.j_file):  
            self.rollback()

        self.journal = open(self.j_file, 'w')

############################################################################################
# Do journal rollback
############################################################################################
    def rollback(self):
        # Close the journal for writing
        self.journal.close()
        self.journal = None

        # Read the journal
        journal = self.file_get_contents(self.j_file)


        journ_list = []
        with open(self.j_file) as fle:
            for l in fle:
                journ_list.append(json.loads(l))

        # If step file exists there has been a failure in the middle of rollback.
        # Continue from where it failed.
        try: targ_step = int(self.file_get_contents(self.j_step_file))
        except OSError:
           targ_step = -1 

        step = 0
        for j_itm in reversed(journ_list):
            # step is always recorded at the end of each rollback item, if the step file
            #records '2', that means something failed during step 3 and we need to start there.  

            if step > targ_step: 
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

                    if os.path.isfile(tmp):  
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
                f.flush()
            
            step += 1


        # Rollback is complete, delete the step file and journal
        os.remove(self.j_step_file)
        os.remove(self.j_file)

############################################################################################
# Finish a transaction
############################################################################################
    def commit(self, cont = False):
        self.journal.close()
        self.journal = None
        os.remove(self.j_file)

        if(cont == True):
            self.begin()
            

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

            self.to_journel(['put_contents', path, tmp_path])
        else:
            self.to_journel(['put_contents', path, None])

        with open(path, 'w') as f:
            f.write(data)


############################################################################################
# Move file from src to dst
############################################################################################
    def move_file(self, src, dst):
        # record where file moved

        if os.path.isfile(src):  
            # if destination file exists, copy it to tmp first
            if os.path.isfile(dst):
                tmp_path = self.new_tmp()
                shutil.move(dst, tmp_path)
                self.to_journel(['overwrite_file', src, dst, tmp_path])

            # if dst does not exist, just log the move
            else: 
                self.to_journel(['move_file', src, dst])

        shutil.move(src, dst)


############################################################################################
# delete a file
############################################################################################
    def delete_file(self, path):
        # if file exists, create a temp copy to allow rollback
        if os.path.isfile(path):  
            tmp_path = self.new_tmp()

            self.to_journel(['delete_file', path, tmp_path])

            shutil.move(path, tmp_path)

        else:
            raise OSError(errno.ENOENT, 'No such file or directory', path)

