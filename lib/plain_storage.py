#import os.path as p
import os, shutil, json, errno

#from storage import *
from rel_storage import *
from common import *

############################################################################################
# Plain (non-versioned) data store
############################################################################################
class plain_storage(rel_storage):

############################################################################################
# Setup and validate file system structure
############################################################################################
    def __init__(self, data_dir, conf_dir, manifest_file, remote_manifest_file = None):
        rel_storage.__init__(self, data_dir, conf_dir)

        self.manifest_file        = manifest_file
        self.remote_manifest_file = remote_manifest_file

############################################################################################
# Get the full path of a file
############################################################################################
    def get_full_file_path(self, r_path):
        return self.mkfs_path(r_path)

############################################################################################
# Gets last change time for a single file
############################################################################################
    def get_single_file_info(self, f_path, int_path):
        f_path = self.mkfs_path(f_path)

        int_path = pfx_path(os.path.normpath(force_unicode(int_path).strip()))
        return { 'path'     : int_path,
                 'created'  : os.path.getctime(f_path),
                 'last_mod' : os.path.getmtime(f_path)}
            
############################################################################################
# Read local manifest file.
############################################################################################
    def read_local_manifest(self):
        # Read Manifest
        try:
            manifest = json.loads(self.r_get(self.manifest_file))
        except OSError:
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
    def write_local_manifest(self, manifest):
        self.r_put(cpjoin(self.manifest_file), json.dumps(manifest))

############################################################################################
# Read locally stored remote manifest. The server sends this data at the end of each
# sync request. It is used to detect file changes on the server since the last run.
# Storing this client side saves having to store individual copies of this data for every
# connected client on the server.
############################################################################################
    def read_remote_manifest(self):
        try:
            manifest = json.loads(self.r_get(self.remote_manifest_file))
        except OSError:

            manifest = {
                'format_vers' : 1,
                'root'        : '/',
                'files'       : []
            }

        return manifest

############################################################################################
# Write remote manifest file
############################################################################################
    def write_remote_manifest(self, manifest):
        file_put_contents(self.r_get(self.remote_manifest_file), json.dumps(manifest))

############################################################################################
# Remove named path from the manifest files array
############################################################################################
    def remove_from_manifest(self, manifest, rpath):
        filter_manifest = []
        for f in manifest['files']:
            if pfx_path(f['path']) == pfx_path(rpath): pass
            else: filter_manifest.append(f)

        manifest['files'] = filter_manifest
        return manifest

############################################################################################
# Add a file to the FS
############################################################################################
    def fs_put(self, rpath, data):
        
        try:
            self.begin()

            # Add the file to the fs
            self.r_put(rpath, data)

            # Add to the manifest
            manifest = self.read_local_manifest()
            manifest['files'].append(self.get_single_file_info(rpath, rpath))
            manifest = self.write_local_manifest(manifest)

        except:
            self.rollback()
            raise

        self.commit()

############################################################################################
# Save an uploaded file
############################################################################################
    def fs_save_upload(self, rpath, file_obj):
        
        try:
            self.begin()

            # Add the file to the current revision
            self.r_save_upload(rpath, file_obj)

            # Add to the manifest
            manifest = self.read_local_manifest()
            manifest['files'].append(self.get_single_file_info(rpath, rpath))
            manifest = self.write_local_manifest(manifest)

        except:
            self.rollback()
            raise

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
            self.begin()

            # Move the file
            self.r_move(r_src, r_dst)

            # Rename the file in the manifest
            manifest = self.read_local_manifest()
            manifest = self.remove_from_manifest(manifest, r_src)
            manifest['files'].append(self.get_single_file_info(r_dst, r_dst))

            manifest = self.write_local_manifest(manifest)

        except:
            self.rollback()
            raise

        self.commit()


############################################################################################
# Remove file from current revision
############################################################################################
    def fs_delete(self, rpath):

        try:
            self.begin()

            # Delete the file
            self.r_delete(rpath)

            # Rename the file in the manifest
            manifest = self.read_local_manifest()
            manifest = self.remove_from_manifest(manifest, rpath)
            manifest = self.write_local_manifest(manifest)

        except:
            self.rollback()
            raise

        self.commit()



