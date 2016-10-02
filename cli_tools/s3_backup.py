"""
Backup server contents to s3 in encrypted form
"""
import __builtin__

__builtin__.SERVER_CONF_FILE = 'server.ini'
__builtin__.MANIFEST_FILE  = '.manifest.json'

salt     = 'E3FF99EDCFAC383EF0AB726D1AAC0'

#########################################################
# Imports
#########################################################
from multiprocessing import Process
import time, sys
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import hashlib
import json
#import scrypt
#import nacl.signing
#import nacl.secret

sys.path = [ './lib' ] + sys.path

from versioned_storage import *
from common import *
from crypto import *





"""
 s3 backup main class
"""
class s3_backup:
############################################################################################
    def __init__(self, aws_access_key, aws_secret_key, s3_bucket):
        """ Create s3 connection and get the bucket.  """

        self.conn = S3Connection(aws_access_key, aws_secret_key)
        self.bucket = self.conn.get_bucket(s3_bucket)


############################################################################################
    def hash_path(self, path):
        """ Hash a file path to create am obscure 'key' for s3.  """
        return hashlib.sha256((salt + path).encode('utf-8')).hexdigest()

############################################################################################
    def update_manifest_s3(self, manifest):
        """ Update the manifest file stored on s3.  """

        key_name = self.hash_path('manifest_xzf.json')

        k = Key(self.bucket)
        k.key = key_name

        #file_put_contents('manifest', json.dumps(manifest))

        contents = encrypt_private('test', json.dumps(manifest), False)

        k.set_contents_from_string(contents)


############################################################################################
    def get_remote_manifest_edit_time(self):
        """ Get remote manifest access time """
        key = self.bucket.get_key(self.hash_path('manifest_xzf.json'))
        if key == None:
            raise Exception('Could not obtain remote manifest')
        return key.last_modified 

############################################################################################
    def get_validate_remote_manifest(self):
        """ Get and validate the remote manifest. """

    # Check if the manifest exists and validate it if it does
        key = self.bucket.get_key(self.hash_path('manifest_xzf.json'))
        if key != None:
            print 'Getting manifest'

            manifest = key.get_contents_as_string()
            manifest = decrypt_private('test', manifest, False)
            manifest = json.loads(manifest)

            manifest_dict = {}

            for fle in manifest['files']:
                manifest_dict[fle['hashed_path']] = fle

        # make sure objects listed in the manifest actually exist on S3
            filter_manifest = []
            listing = self.bucket.list()
            for itm in listing:
            # make sure this key exists in the manifest
                if itm.key in manifest_dict:
                    filter_manifest.append(manifest_dict.pop(itm.key))

            manifest['files'] = filter_manifest

            """
            print filter_manifest
            print ''
            # manifest dict now contains files which do not exist on the remote
            print manifest_dict
            print ''
            """
        
    # If no manifest, create one
        else:
            print 'creating manifest'

            manifest = {'files' : []}

            self.update_manifest_s3(manifest)

        return manifest


############################################################################################
    def find_new_files(self, manifest, local_file_dict):
        """ Compare manifest to local file system finding new files """

        local_missing = []

        for fle in manifest['files']:
            if fle['path'] in local_file_dict:
                local_file_dict.pop(fle['path']) # item already exists on s3
            else:
                # file does not exist locally
                local_missing.append(fle)

        new_files = []

        # Any thing still in the dict does not exist on the remote
        for key, val in local_file_dict.iteritems():
            val['hashed_path'] = self.hash_path(val['path'])
            manifest['files'].append(val)
            new_files.append(val)
    
        return (new_files, local_missing)


############################################################################################
    def send_file(self, fle):
        """ Send a local file to s3 """

        k = Key(self.bucket)
        k.key = fle['hashed_path']

        contents = file_get_contents(fle['full_path'])

        crypt_contents = encrypt_private('test', contents, False)

        k.set_contents_from_string(crypt_contents)




#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
def run_backup():
    """ s3 backup system main loop """
    global repositories

    while True:
        for key, value in repositories.iteritems():
            if 's3_obj' not in value:
                continue

            store = value['data_store']
            s3    = value['s3_obj']

        # Get local manifest of latest revision
            local_manifest = store.read_local_manifest()

        # Get the remote manifest from s3 if it has not been obtained already
            manifest = None
            if 'manifest' not in value:
                manifest = s3.get_validate_remote_manifest()
                value['manifest'] = manifest
                repositories[key]['manifest'] = manifest

                value['mnfst_et'] = s3.get_remote_manifest_edit_time()
                repositories[key]['mnfst_et'] = value['mnfst_et']

            else:
                manifest = value['manifest']

        # Check when the manifest was last changed on the server
            mnfst_at = s3.get_remote_manifest_edit_time()
            if mnfst_at != value['mnfst_et']:
                raise Exception('Remote manifest has been changed, probable multiple client access')

        # See if anything has changed 
            new_files, local_missing = s3.find_new_files(manifest, make_dict(local_manifest['files']))

            if local_missing != []:
                raise Exception('Files exist on remote which do not exist locally')

            if new_files == []:
                print 'Nothing to do'
            else:
                # Update the manifest on s3. All new files are added to the manifest at once.
                # This is done to reduce network traffic. Manifest is updated before any uploads
                # happen as we want to make sure we can map the hashed keys back to file names
                # in the future even if we are not able to upload all of them in this session
                s3.update_manifest_s3(manifest)

            # Update manifest access time
                value['mnfst_et'] = s3.get_remote_manifest_edit_time()
                repositories[key]['mnfst_et'] = value['mnfst_et']

                # Upload any files that don't exist 
                for fle in new_files:
                    print 'Sending: ' + fle['path']
                    fle['full_path'] = store.get_full_file_path(fle['path'])

                    s3.send_file(fle)
                    

                # Remove files from remote that have been removed locally



        # Pause
        time.sleep(10)


config = read_config(SERVER_CONF_FILE)

if 'global' in config:
    glb = config.pop('global')

    if 'manifest_file' in glb:
        __builtin__.MANIFEST_FILE  = glb['manifest_file']

repositories = {}
for key, val in config.iteritems():

    repositories[key] = {
        # The root path of the repository
        'path'          : val['repository_path'],

        # Data store object for the above path
        'data_store'    : versioned_storage(val['repository_path'], '', MANIFEST_FILE),
    }

    # s3 backup object
    if all (k in val for k in ("s3_access_key", "s3_secret_key", 's3_bucket')):
        repositories[key]['s3_obj'] = s3_backup(
            val['s3_access_key'],
            val['s3_secret_key'],
            val['s3_bucket'])

run_backup()
