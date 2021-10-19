import json
from shttpfs3.storage import storage
from shttpfs3.common import cpjoin, get_single_file_info, file_or_default

class plain_storage(storage):
    """ Plain (non-versioned) data store used by the client """

#===============================================================================
    def __init__(self, data_dir):
        """ Setup and validate file system structure """

        storage.__init__(self, data_dir, '.shttpfs')
        self.manifest_file = cpjoin('.shttpfs', 'manifest.json')

#===============================================================================
    def get_single_file_info(self, rel_path):
        """ Gets last change time for a single file """

        f_path = self.get_full_file_path(rel_path)
        return get_single_file_info(f_path, rel_path)

#===============================================================================
    def read_local_manifest(self):
        """ Read the file manifest, or create a new one if there isn't one already """

        manifest = json.loads(file_or_default(self.get_full_file_path(self.manifest_file), """{
            "format_version" : 2,
            "root"           : "/",
            "have_revision"  : "root",
            "files"          : {}}"""))

        if 'format_version' not in manifest or manifest['format_version'] < 2:
            raise SystemExit('Please update the client manifest format')
        return manifest

#===============================================================================
    def write_local_manifest(self, manifest):
        self.file_put_contents(self.manifest_file, json.dumps(manifest).encode('utf8'))

#===============================================================================
    def remove_from_manifest(self, manifest, rpath):
        """ Remove named path from the manifest files array """
        del manifest['files'][rpath]
        return manifest

#===============================================================================
    def fs_put(self, rpath, data, additional_manifest_data = None):
        """ Add a file to the FS """

        if additional_manifest_data is None:
            additional_manifest_data = {}

        try:
            self.begin()

            # Add the file to the fs
            tmppath = cpjoin('.shttpfs', 'downloading')
            self.file_put_contents(tmppath, data)
            self.move_file(tmppath, rpath)

            # Add to the manifest
            manifest = self.read_local_manifest()
            manifest['files'][rpath] = self.get_single_file_info(rpath)
            manifest['files'][rpath].update(additional_manifest_data)
            self.write_local_manifest(manifest)

            self.commit()
        except:
            self.rollback(); raise

#===============================================================================
    def fs_get(self, rpath):
        """ Get a files contents from the FS """

        return self.file_get_contents(rpath)

#===============================================================================
    def fs_move(self, r_src, r_dst):
        try:
            self.begin()

            # Move the file
            self.move_file(r_src, r_dst)

            # Rename the file in the manifest
            manifest = self.read_local_manifest()
            manifest['files'][r_dst] = self.get_single_file_info(r_dst)
            self.write_local_manifest(manifest)

            self.commit()
        except:
            self.rollback(); raise

#===============================================================================
    def fs_delete(self, rpath):
        try:
            self.begin()

            # Delete the file
            self.delete_file(rpath)

            # Rename the file in the manifest
            manifest = self.read_local_manifest()
            del manifest['files'][rpath]
            self.write_local_manifest(manifest)

            self.commit()
        except:
            self.rollback(); raise
