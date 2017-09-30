from rel_storage import *
from common import *
import os.path

############################################################################################
# Versioning data store
############################################################################################
class versioned_storage_2(rel_storage):

############################################################################################
    def __init__(self, data_dir, conf_dir, manifest_file):
        """ Setup and validate file system structure """
        rel_storage.__init__(self, data_dir, conf_dir)

        self.vrs_dir       = "versions"
        self.head_file     = "head"
        self.stage_dir     = "stage"
        self.manifest_file = manifest_file

        try: self.r_makedirs(self.vrs_dir)
        except OSError: pass


############################################################################################
    def get_versions(self):
        """ find all existing versions """
        try: 
            versions = self.r_listdir(self.vrs_dir)
            return [int(ver) for ver in versions] 
        except ValueError:
            raise Exception('Error: non integer directory name in versions.') 

############################################################################################
    def get_full_file_path(self, r_path, version = None):
        """ Get the full path of a file """

        if version == None: version = self.get_head() 
        return self.mkfs_path(cpjoin(self.vrs_dir, str(version), r_path))

############################################################################################
    def get_single_file_info(self, f_path, int_path, version = None):
        """ Gets last change time for a single file """

        f_path = cpjoin(self.vrs_dir, str(version), f_path)
        f_path = self.mkfs_path(f_path)

        int_path = pfx_path(os.path.normpath(force_unicode(int_path).strip()))
        return { 'path'     : int_path,
                 'created'  : os.path.getctime(f_path),
                 'last_mod' : os.path.getmtime(f_path)}

############################################################################################
    def new_manifest(self):
        """ Create new manifest structure """

        return { 'format_vers' : 1,
                 'files'       : {},
                 'changes'     : []}
            
############################################################################################
    def new_manifest_item(self, physical_path, version_no):
        """ Create new manifest structure """

        return { 'physical_path' : physical_path,
                 'version_no'    : version_no}

############################################################################################
    def read_local_manifest(self, vrs = None):
        """ Read local manifest file. """
        if vrs == None: vrs = self.get_head()
        return json.loads(self.r_get(cpjoin('versions', str(vrs), self.manifest_file)))

############################################################################################
    def read_staging_manifest(self):
        """ Read staging manifest file """

        return json.loads(self.r_get(cpjoin(self.stage_dir, self.manifest_file)))

############################################################################################
    def write_staging_manifest(self, manifest):
        """ Write staging manifest file """
        self.r_put(cpjoin(self.stage_dir, self.manifest_file), json.dumps(manifest))

############################################################################################
    def diff_forward(self, vrs):
        """ Work out what has changed between the version specified and head """
        pass

############################################################################################
    def varify_staging(self, msg = 'Must be staging before adding'):
        if not os.path.isdir(cpjoin(self.data_dir, self.stage_dir)): raise Exception(msg) 

############################################################################################
    def begin_staging(self):
        """ Begin staging of a new version """

        # check for past versions and begin staging
        if os.path.isdir(cpjoin(self.data_dir, self.stage_dir)):
            raise Exception('Already staging') 

        self.begin()
        self.r_makedirs(self.stage_dir)
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
    def commit_version(self):
        """ Commit current staging as a new version """

        try: head = max(self.get_versions())
        except ValueError: head = 0
        self.begin()
        self.r_move(self.stage_dir, cpjoin(self.vrs_dir, str(head + 1)))
        self.commit()

############################################################################################
    def fs_get(self, rpath):
        """ Get a files contents from the FS """

        return self.r.get(rpath)

############################################################################################
    def fs_put(self, rpath, data):
        """ Add a file to the FS """

        self.varify_staging()

        self.begin()

        # add a new file to staging
        # TODO if already exists replace the file and track it as a change in 'changes' instead of new
        # TODO add a lower level 'data' dir

        try: self.r_makedirs(cpjoin(self.stage_dir, os.path.dirname(rpath)))
        except OSError: pass
        self.r_put(cpjoin(self.stage_dir, rpath), data)

        # Add to the manifest
        manifest = self.read_staging_manifest()

        if rpath in manifest['files']: raise OSError('already exists')

        manifest['files'][rpath] = self.new_manifest_item(rpath, None)
        manifest['changes'].append({'status' : 'new', 'path' : rpath})

        self.write_staging_manifest(manifest)
        self.commit()

############################################################################################
    def fs_move(self, r_src, r_dst):
        """ Move a file by renaming it in the manifest """

        self.varify_staging()

        self.begin()
        # Rename the file in the manifest
        manifest = self.read_staging_manifest()
        manifest['files'][r_dst] = manifest['files'].pop(r_src)
        manifest['changes'].append({'status' : 'moved', 'path' : r_dst, 'moved_from' : r_src})
        # TODO handle moved of new items

        self.write_staging_manifest(manifest)
        self.commit()

############################################################################################
    def fs_delete(self, rpath):
        """ Removes it from the current manifest, effectively removing it from this version """

        self.varify_staging()

        self.begin()
        manifest = self.read_staging_manifest()
        manifest['files'].pop(rpath)
        manifest['changes'].append({'status' : 'deleted', 'path' : rpath})
        manifest = self.write_local_manifest(head, manifest)
        self.commit()

