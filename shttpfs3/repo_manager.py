import sys
from shttpfs3.common import cpjoin, file_get_contents

from shttpfs3.backup import s3_interface, pipeline
from shttpfs3.storage.versioned_storage import versioned_storage

import pprint

config = {}

#===============================================================================
def init(new_config):
    global config
    config = new_config

#===============================================================================
def backup():
    for repository_name, details in config['repositories'].items(): 
        if 'backup' in details and details['backup'] is True:

            # Connect to the remote
            s3_conn = s3_interface.connect(config)
            # s3_interface.wipe_all(s3_conn)

            # Get the commits that we have locally
            repository_path = details['path']
            data_store = versioned_storage(repository_path)

            # -----------
            commits_order = []
            commits = {}

            head = data_store.get_head()

            if head == 'root':
                return # If head is root, we have no commits and don't need to do anything

            pointer = head

            while True:
                commit = data_store.read_commit_index_object(pointer)
                commits[pointer] = commit
                commits_order.append(pointer)
                pointer = commit['parent']
                if pointer == 'root': break

            # Get the commits that already exist on the remote
            result = s3_conn['client'].list_objects(Bucket=s3_conn['bucket'], Prefix='commits/', Delimiter='')

            # Subtract the commits that have already been uploaded
            if 'Contents' in result:
                for item in result['Contents']: 
                    commit_hash = item['Key'].split('/')[-1]
                    commits.pop(commit_hash)
                    commits_order.remove(commit_hash)

            # Cache a list of objects that already exist on the remote
            files_on_remote = s3_conn['client'].list_objects(Bucket=s3_conn['bucket'], Prefix='files/', Delimiter='')
            pprint.pprint(files_on_remote)

            # Go through versions from newest to oldest, uploading anything that doesn't already exist on the remote
            for commit_hash in commits_order:
                commit = commits[commit_hash]

                # Upload head first, then the meta info for the revision, and files last,
                # following this order allows structure to be recovered even if all files
                # happen to not get backed up.

                objects_to_upload = []

                # ========================================================================
                # Store head
                # ========================================================================
                objects_to_upload.append({
                    'type' : 'literal',
                    'path' : 'head',
                    'value' : head
                })

                # ========================================================================
                # Store file index
                # ========================================================================

                # TODO get and store the file index


                # ========================================================================
                # Store files
                # ========================================================================
                objects_in_commit = data_store.flatten_dir_tree(data_store.read_dir_tree(commit['tree_root']))

                objects_in_commit = {it['hash'] : it for it in objects_in_commit.values()}

                # subtract files that already exist
                if 'Contents' in files_on_remote:
                    for item in files_on_remote['Contents']: 
                        file_hash = ''.join(item['Key'].split('/')[-2:])
                        print(file_hash)
                        objects_in_commit.pop(file_hash)

                # TODO need to do something about empty files as s3 won't store them
                for it in objects_in_commit.values():
                    objects_to_upload.append({
                        'type'   : 'file',
                        'source' : cpjoin(repository_path, 'files', it['hash'][:2], it['hash'][2:]),
                        'dest'   : cpjoin('files', it['hash'][:2], it['hash'][2:])
                    })

                # ========================================================================
                # Upload things that need uploading
                # ========================================================================
                for it in objects_to_upload:
                    if it['type'] == 'literal':
                        s3_interface.put_object(s3_conn, it['path'], it['value'])

                    else:
                        pipeline_configuration = pipeline.get_default_pipeline_format()
                        print(pipeline_configuration)

                        #-----
                        upload = s3_interface.streaming_upload()
                        pl     = pipeline.build_pipeline_streaming(upload, 'out')
                        pl.pass_config(config, pipeline.serialise_pipeline_format(pipeline_configuration))

                        print(it['source'])

                        upload.begin(s3_conn, it['dest'])

                        try:
                            with open(it['source'], 'rb') as fle:
                                while True:
                                    print('.', end =" ")

                                    chunk = fle.read(1048576 * 5)
                                    if chunk == b'': break
                                    pl.next_chunk(chunk)
                                print()
                            upload.finish()

                        # If file no longer exists at this stage assume it has been deleted and ignore it
                        except IOError:
                            upload.abort()
                            raise

                # ========================================================================
                # Finally, create a 'commit' object to mark that everything in this
                # commit has been uploaded.
                # ========================================================================
                s3_interface.put_object(s3_conn, 'commits/' + commit_hash, 'exists')


#===============================================================================
def run():
    args = list(sys.argv)[1:]

    #----------------------------
    if len(args) == 0 or args[0] == '-h': print("""

    Usage: shttpfs_repo [command]

    verify                       : Verify repository contents has not been courrupted

    backup                       : Backup repositories

    """)

    #----------------------------
    elif args[0] == 'verify':
        for repository_name, details in config['repositories'].items(): 
            repository_path = details['path']
            data_store = versioned_storage(repository_path)

            data_store.verify_fs()


    #----------------------------
    elif args[0] == 'backup':
        backup()

    else:
        print('Unknowm command. Run shttpfs_repo -h to list commands.')


# 
