from rel_storage import *
from common import *

############################################################################################
# Versioning data store
############################################################################################
class versioned_storage_2(rel_storage):

############################################################################################
# Setup and validate file system structure
############################################################################################
    def __init__(self, data_dir, conf_dir, manifest_file):
        rel_storage.__init__(self, data_dir, conf_dir)

        self.vrs_dir       = "versions"
        self.head_file     = "head"
        self.stage_dir     = "stage"
        self.manifest_file = manifest_file

        try: self.r_makedirs(self.vrs_dir)
        except OSError: pass


############################################################################################
# find all existing versions
############################################################################################
    def get_versions(self):
        try: 
            versions = self.r_listdir(self.vrs_dir)
            return [int(ver) for ver in versions] 
        except ValueError:
            raise Exception('Error: non integer directory name in versions.') 

############################################################################################
# Get the full path of a file
############################################################################################
    def get_full_file_path(self, r_path, version = None):
        if version == None: version = self.get_head() 
        return self.mkfs_path(cpjoin(self.vrs_dir, str(version), r_path))

############################################################################################
# Gets last change time for a single file
############################################################################################
    def get_single_file_info(self, f_path, int_path, version = None):
        f_path = cpjoin(self.vrs_dir, str(version), f_path)
        f_path = self.mkfs_path(f_path)

        int_path = pfx_path(os.path.normpath(force_unicode(int_path).strip()))
        return { 'path'     : int_path,
                 'created'  : os.path.getctime(f_path),
                 'last_mod' : os.path.getmtime(f_path)}


############################################################################################
# Create new manifest structure
############################################################################################
    def new_manifest(self):
        return {
            'format_vers' : 1,
            'root'        : '/',
            'files'       : []}
            
############################################################################################
# Read local manifest file.
############################################################################################
    def read_local_manifest(self, vrs = None):
        if vrs == None: vrs = self.get_head()
        return json.loads(self.r_get(cpjoin('versions', str(vrs), self.manifest_file)))

############################################################################################
# Read staging manifest file
############################################################################################
    def read_staging_manifest(self):
        return json.loads(self.r_get(cpjoin(self.stage_dir, self.manifest_file)))

############################################################################################
# Write staging manifest file
############################################################################################
    def write_staging_manifest(self, manifest):
        self.r_put(cpjoin(self.stage_dir, self.manifest_file), json.dumps(manifest))

############################################################################################
# Work out what has changed between the version specified and head
############################################################################################
    def diff_forward(self, vrs):
        pass


############################################################################################
    def varify_staging(self, msg = 'Must be staging before adding'):
        if not os.path.isdir(cpjoin(self.data_dir, self.stage_dir)): raise Exception(msg) 

############################################################################################
# Begin staging of a new version
############################################################################################
    def begin_staging(self):
        # check for past versions and begin staging
        if os.path.isdir(cpjoin(self.data_dir, self.stage_dir)):
            raise Exception('Already staging') 

        self.begin()
        self.r_makedirs(self.stage_dir) # TODO make sure this is revertible
        # copy manifest from past version into staging area
        head = None
        try:
            head = max(self.get_versions())
            manifest = self.read_local_manifest(head)
        except ValueError:
            manifest = self.new_manifest()

        self.write_staging_manifest(manifest)
        self.commit()


############################################################################################
# Commit current staging as a new version
############################################################################################
    def commit_version(self):
        try: head = max(get_versions())
        except ValueError: head = 0
        self.r_move(cpjoin(self.vrs_dir, self.stage_dir), cpjoin(self.vrs_dir, head + 1))

############################################################################################
# Get a files contents from the FS
############################################################################################
    def fs_get(self, rpath):
        return self.r.get(rpath)

############################################################################################
# Add a file to the FS
############################################################################################
    def fs_put(self, rpath, data):
        self.varify_staging()

        self.begin()
        # add a new file to staging
        self.r_put(cpjoin(self.stage_dir, rpath), data)

        # Add to the manifest
        manifest = self.read_staging_manifest()
        manifest['files'].append({'path' : rpath})
        manifest = self.write_staging_manifest(manifest)
        self.commit()


############################################################################################
# Move a file in the FS
############################################################################################
    def fs_move(self, r_src, r_dst):
        self.varify_staging()

        # Move the file
        self.r_move(cpjoin(cur_vrs, r_src), cpjoin(cur_vrs, r_dst))

        # Rename the file in the manifest
        manifest = self.read_local_manifest(head)
        manifest = self.remove_from_manifest(manifest, r_src)
        manifest['files'].append(self.get_single_file_info(r_dst, r_dst, head))

        manifest = self.write_local_manifest(head, manifest)


############################################################################################
# Remove file from current revision
############################################################################################
    def fs_delete(self, rpath):
        self.varify_staging()

        # remove a file from the staging manifest
        manifest = self.read_local_manifest(head)
        manifest = self.remove_from_manifest(manifest, r_src)
        manifest = self.write_local_manifest(head, manifest)

