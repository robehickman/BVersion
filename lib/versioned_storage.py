#import os.path as p
import os, shutil, json, errno
import pdb

from rel_storage import *
from common import *


############################################################################################
# Versioning data store
############################################################################################
class versioned_storage(rel_storage):
    stepped   = False
    vrs_dir  = ''
    head_file = ''

############################################################################################
# Setup and validate file system structure
############################################################################################
    def __init__(self, data_dir, conf_dir, manifest_file):
        rel_storage.__init__(self, data_dir, conf_dir)

        self.vrs_dir       = "versions"
        self.head_file     = "head"
        self.manifest_file = manifest_file

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
            self.r_makedirs(cpjoin(self.vrs_dir, '1'))

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
# Get the number of the head revision
############################################################################################
    def get_head(self):
        return int(self.r_get(self.head_file))

############################################################################################
# Get the full path of a file
############################################################################################
    def get_full_file_path(self, r_path, version = None):
        if version == None:
            version = self.get_head()

        v_path = cpjoin(self.vrs_dir, str(version), r_path)
        f_path = self.mkfs_path(v_path)
        return f_path

############################################################################################
# Gets last change time for a single file
############################################################################################
    def get_single_file_info(self, f_path, int_path, version = None):
        if version == None:
            version = self.get_head()

        f_path = cpjoin(self.vrs_dir, str(version), f_path)
        f_path = self.mkfs_path(f_path)

        int_path = pfx_path(os.path.normpath(force_unicode(int_path).strip()))
        return { 'path'     : int_path,
                 'created'  : os.path.getctime(f_path),
                 'last_mod' : os.path.getmtime(f_path)}
            
############################################################################################
# Read local manifest file.
############################################################################################
    def read_local_manifest(self, vrs = None):
        if vrs == None:
            vrs = self.get_head()

        # Read Manifest
        try:
            manifest = json.loads(self.r_get(cpjoin('versions', str(vrs), self.manifest_file)))
        except:
            # no manifest, create one, manifest stores file access times as
            # of last run to detect files which have changed
            manifest = {
                'format_vers' : 1,
                'root'        : '/',
                'files'       : []
            }

        return manifest

############################################################################################
# Write local manifest file
############################################################################################
    def write_local_manifest(self, vrs, manifest):
        self.r_put(cpjoin('versions', str(vrs), self.manifest_file), json.dumps(manifest))


############################################################################################
# Step version number forward
############################################################################################
    def step_version(self):
        self.begin()

        try:
            head = self.get_head()

            cur_rv = (cpjoin(self.vrs_dir, str(head)))
            new_rv = (cpjoin(self.vrs_dir, str(head + 1)))

            # move current head forward one step, then make an empty prior revision
            self.r_move(cur_rv, new_rv)
            self.r_makedirs(cur_rv)

            self.r_put(cpjoin(cur_rv, self.manifest_file), '')

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
        if self.r_isfile(cpjoin(self.vrs_dir, str(head), rpath)):
            exists = True
            # Does the file exist in the parent rv?
            if head == 1 or self.r_isfile(cpjoin(self.vrs_dir, str(head - 1), rpath)):
                # if yes, step revision
                self.step_version()

        # reload head in case we stepped version
        head = self.get_head()


        self.begin()

        # If there is a current file, move it back to prior RV
        if exists == True:
            try: self.r_makedirs(os.path.dirname(cpjoin(self.vrs_dir, str(head - 1), rpath)))
            except OSError: pass

            self.r_move(cpjoin(self.vrs_dir, str(head), rpath), cpjoin(self.vrs_dir, str(head - 1), rpath))

            # Add up-moved file to parent manifest,
            manifest = self.read_local_manifest(head - 1)
            manifest['files'].append(self.get_single_file_info(rpath, rpath, head - 1))
            manifest = self.write_local_manifest(head - 1, manifest)

            # Remove it from head manifest
            manifest = self.read_local_manifest(head)
            manifest = self.remove_from_manifest(manifest, rpath)
            manifest = self.write_local_manifest(head, manifest)

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
            self.r_put(cpjoin(self.vrs_dir, str(head), rpath), data)

            # Add to the manifest
            manifest = self.read_local_manifest(head)
            manifest['files'].append(self.get_single_file_info(rpath, rpath, head))
            manifest = self.write_local_manifest(head, manifest)

        except:
            raise
            self.rollback()

        self.commit()

############################################################################################
# Save an uploaded file
############################################################################################
    def fs_save_upload(self, rpath, file_obj):
        
        try:
            # does file exist in current rv? If it does move it to prior rv
            self.step_if_exists(rpath)

            self.begin()
            # reload head in case we stepped version
            head = self.get_head()

            # Add the file to the current revision
            self.r_save_upload(cpjoin(self.vrs_dir, str(head), rpath), file_obj)

            # Add to the manifest
            manifest = self.read_local_manifest(head)
            manifest['files'].append(self.get_single_file_info(rpath, rpath, head))
            manifest = self.write_local_manifest(head, manifest)

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

            cur_vrs = cpjoin(self.vrs_dir, str(head))

            # Move the file
            self.r_move(cpjoin(cur_vrs, r_src), cpjoin(cur_vrs, r_dst))

            # Rename the file in the manifest
            manifest = self.read_local_manifest(head)
            manifest = self.remove_from_manifest(manifest, r_src)
            manifest['files'].append(self.get_single_file_info(r_dst, r_dst, head))

            manifest = self.write_local_manifest(head, manifest)

        except:
            self.rollback()
            raise

        self.commit()


############################################################################################
# Remove file from current revision
############################################################################################
    def fs_delete(self, rpath):
        # does target file exist in current rv? If it does move it to prior rv
        self.step_if_exists(rpath)

        # do not need to do anything else, step_if_exists moves the file into the
        # previous revision so it is effectively 'deleted' from the current one.



