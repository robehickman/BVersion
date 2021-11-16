from bversion.common import cpjoin, get_single_file_info

class client_filesystem_interface:

#===============================================================================
    def __init__(self, client_db, journaling_filesystem):
        """ Setup and validate file system structure """

        self.client_db     = client_db
        self.jfs           = journaling_filesystem

#===============================================================================
    def get_single_file_info(self, rel_path):
        """ Gets last change time for a single file """

        f_path = self.jfs.get_full_file_path(rel_path)
        return get_single_file_info(f_path, rel_path)

#===============================================================================
    def fs_put(self, rpath, data, additional_manifest_data = None):
        """ Add a file to the FS """

        if additional_manifest_data is None:
            additional_manifest_data = {}

        try:
            self.jfs.begin()

            # Add the file to the fs
            tmppath = cpjoin('.bvn', 'downloading')
            self.jfs.file_put_contents(tmppath, data)
            self.jfs.move_file(tmppath, rpath)

            # Add to the manifest
            file_info = self.get_single_file_info(rpath)
            file_info.update(additional_manifest_data)
            self.client_db.add_file_to_manifest(file_info)

            self.jfs.commit()
        except:
            self.jfs.rollback(); raise

#===============================================================================
    def fs_get(self, rpath):
        """ Get a files contents from the FS """

        return self.jfs.file_get_contents(rpath)

#===============================================================================
    def fs_move(self, r_src, r_dst):
        try:
            self.jfs.begin()

            # Move the file
            self.jfs.move_file(r_src, r_dst)

            # Rename the file in the manifest
            file_info = self.client_db.get_single_file_from_manifest(r_src)
            self.client_db.remove_file_from_manifest(r_src)
            file_info.update(self.get_single_file_info(r_dst))
            self.client_db.add_file_to_manifest(file_info)

            self.jfs.commit()
        except:
            self.jfs.rollback(); raise

#===============================================================================
    def fs_delete(self, rpath):
        try:
            self.jfs.begin()

            # Delete the file
            self.jfs.delete_file(rpath)

            # Remove the file from the manifest
            self.client_db.remove_file_from_manifest(rpath)

            self.jfs.commit()
        except:
            self.jfs.rollback()
            raise
