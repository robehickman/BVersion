import os, shutil, json, errno

from storage import *
from common import *

############################################################################################
# Relative storage layer, all paths are rooted to DATA_DIR
############################################################################################
class rel_storage(storage):
############################################################################################
# Setup and validate file system structure
############################################################################################
    def __init__(self, data_dir, conf_dir):
        storage.__init__(self, data_dir, conf_dir)

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
# Relative file delete
############################################################################################
    def r_delete(self, rpath):
        return self.delete_file(self.mkfs_path(rpath))

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
# Remove named path from the manifest files array
############################################################################################
    def remove_from_manifest(self, manifest, rpath):
        filter_manifest = []
        for f in manifest['files']:
            if pfx_path(f['path']) == pfx_path(rpath): pass
            else: filter_manifest.append(f)

        manifest['files'] = filter_manifest
        return manifest


