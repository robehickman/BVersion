import sys, os
from io import BytesIO

from bversion.common import cpjoin, merge_config, make_dirs_if_dont_exist
from bversion.backup import s3_interface, pipeline, crypto
from bversion.storage.versioned_storage import versioned_storage

# ---------------
config = {}
empty_file_identifier = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'


#===============================================================================
def init(new_config):
    global config

    base_conf = {
        'local_lock_file'                : 'repo_manager_lock',  # name of the local lock file
        'chunk_size'                     : 1048576 * 5,          # minimum chunk size is 5MB on s3, 1mb = 1048576
    }

    base_conf = crypto.add_default_config(base_conf)

    config = merge_config(base_conf, new_config)


#===============================================================================
def init_backup():
    global config

    s3_conn = s3_interface.connect(config)
    s3_interface.delete_failed_uploads(s3_conn)
    config = pipeline.preprocess_config(s3_interface, s3_conn, config)

    return s3_conn


#===============================================================================
def streaming_upload(s3_conn, lconfig, remote_file_path, file_handle):
    pipeline_format = ["encrypt"]

    #----
    pipeline_configuration = pipeline.get_default_pipeline_format()
    pipeline_configuration['chunk_size'] = lconfig['chunk_size']
    pipeline_configuration['format'] = {i : None for i in pipeline_format}
    if 'encrypt' in pipeline_configuration['format']:
        pipeline_configuration['format']['encrypt'] = lconfig['crypto']['encrypt_opts']

    #-----
    upload = s3_interface.streaming_upload()
    pl     = pipeline.build_pipeline_streaming(upload, 'out')
    pl.pass_config(lconfig, pipeline.serialise_pipeline_format(pipeline_configuration))

    upload.begin(s3_conn, remote_file_path)

    try:
        while True:
            print('.', end =" ")

            chunk = file_handle.read(lconfig['chunk_size'])
            if chunk == b'': break
            pl.next_chunk(chunk)
        print()
        upload.finish()

    except IOError:
        upload.abort()
        raise


#===============================================================================
def streaming_download(s3_conn, lconfig, remote_file_path, version_id, file_handle):
    download_stream  = s3_interface.streaming_download()
    header = download_stream.begin(s3_conn, remote_file_path, version_id)[0]
    pl                = pipeline.build_pipeline_streaming(download_stream, 'in')
    pl.pass_config(lconfig, header)

    while True:
        res = pl.next_chunk()
        if res is None: break
        file_handle.write(res)


#===============================================================================
def backup():
    # Connect to the remote
    s3_conn = init_backup()

    for repository_name, details in config['repositories'].items():
        if 'backup' in details and details['backup'] is True:
            # Get the commits that we have locally
            repository_path = details['path']
            data_store = versioned_storage(repository_path)

            # -----------
            commits_order = []
            commits = {}

            head = data_store.get_head()

            if head == 'root':
                continue # If head is root, we have no commits and don't need to do anything

            # ========================================================================
            # Read commit chain
            # ========================================================================
            pointer = head
            while True:
                commit = data_store.read_commit_index_object(pointer)
                commits[pointer] = commit
                commits_order.append(pointer)
                pointer = commit['parent']
                if pointer == 'root': break

            # ========================================================================
            # Get the commits that already exist on the remote
            # Subtract the commits that have already been uploaded
            # ========================================================================
            commits_on_remote = s3_conn['client'].list_objects(Bucket=s3_conn['bucket'], Prefix=repository_name + '/commits/', Delimiter='')

            if 'Contents' in commits_on_remote:
                for item in commits_on_remote['Contents']:
                    commit_hash = item['Key'].split('/')[-1]
                    commits.pop(commit_hash)
                    commits_order.remove(commit_hash)

            # ========================================================================
            # Go through versions from newest to oldest, uploading anything that doesn't already exist on the remote
            # ========================================================================
            for commit_hash in commits_order:
                commit = commits[commit_hash]

                # Cache a list of objects that already exist on the remote
                index_on_remote = s3_conn['client'].list_objects(Bucket=s3_conn['bucket'], Prefix=repository_name + '/index/', Delimiter='')
                files_on_remote = s3_conn['client'].list_objects(Bucket=s3_conn['bucket'], Prefix=repository_name + '/files/', Delimiter='')

                # We Upload head first, then the meta info for the revision, and files last,
                # following this order allows structure to be recovered even if all files
                # happen to not get backed up.
                objects_to_upload   = []

                # ========================================================================
                # Store head
                # ========================================================================
                if commit_hash == head:
                    objects_to_upload.append({
                        'type' : 'literal',
                        'path' : 'head',
                        'value' : head
                    })

                # ========================================================================
                # Store commit
                # ========================================================================
                index_objects      = {}
                index_object_order = []

                index_objects[commit_hash] = {
                    'type'   : 'file',
                    'source' : cpjoin(repository_path, 'index', commit_hash[:2], commit_hash[2:]),
                    'dest'   : cpjoin('index', commit_hash[:2], commit_hash[2:])
                }
                index_object_order.append(commit_hash)

                # ========================================================================
                # Store file index objects for this commit
                # ========================================================================
                tree_objects = {}
                def recursive_helper(data_store, tree_object_hash):
                    nonlocal tree_objects

                    it = data_store.read_tree_index_object(tree_object_hash)
                    tree_objects[tree_object_hash] = it # pylint: disable=W0640

                    for dir_hash in it['dirs'].values():
                        recursive_helper(data_store, dir_hash) # pylint: disable=W0640

                recursive_helper(data_store, commit['tree_root'])

                # ===============================================
                for tree_object_hash in tree_objects:
                    index_objects[tree_object_hash] = {
                        'type'   : 'file',
                        'source' : cpjoin(repository_path, 'index', tree_object_hash[:2], tree_object_hash[2:]),
                        'dest'   : cpjoin('index', tree_object_hash[:2], tree_object_hash[2:])
                    }

                    index_object_order.append(tree_object_hash)

                # subtract objects that already exist on remote
                if 'Contents' in index_on_remote:
                    for item in index_on_remote['Contents']:
                        file_hash = ''.join(item['Key'].split('/')[-2:])
                        if file_hash in index_objects:
                            index_objects.pop(file_hash)
                            index_object_order.remove(file_hash)

                # Add everything remaining to upload
                for it in index_object_order:
                    objects_to_upload.append(index_objects[it])

                # ========================================================================
                # Store files
                # ========================================================================
                file_objects = {}

                for tree_object in tree_objects.values():
                    for fle in tree_object['files'].values():
                        file_objects[fle['hash']] = fle

                # subtract files that already exist
                if 'Contents' in files_on_remote:
                    for item in files_on_remote['Contents']:
                        file_hash = ''.join(item['Key'].split('/')[-2:])
                        if file_hash in file_objects:
                            file_objects.pop(file_hash)

                # -------------------------------
                for it in file_objects.values():
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
                        print('Uploading: ' + repository_name + '/' + it['path'])

                        fle = BytesIO(it['value'].encode('utf8'))
                        streaming_upload(s3_conn, config,
                                         remote_file_path = repository_name + '/' + it['path'],
                                         file_handle = fle)

                    else:
                        file_hash = ''.join(it['dest'].split('/')[-2:])

                        # We skip uploading empty files, as s3 won't store them, they
                        # are recreated during the download stage.
                        if file_hash == empty_file_identifier:
                            continue

                        #-----
                        print('Uploading: ' + repository_name + '/' + it['dest'])

                        with open(it['source'], 'rb') as fle:
                            streaming_upload(s3_conn, config,
                                             remote_file_path = repository_name + '/' + it['dest'],
                                             file_handle = fle)

                # ========================================================================
                # Finally, create a 'commit' object to mark that everything in this
                # commit has been uploaded.
                # ========================================================================
                fle = BytesIO(b'exists')
                streaming_upload(s3_conn, config,
                                 remote_file_path = repository_name + '/commits/' + commit_hash,
                                 file_handle = fle)

                print('Backed up commit: ' + commit_hash)


#===============================================================================
def restore():
    # Connect to the remote
    s3_conn = init_backup()

    for repository_name, details in config['repositories'].items():
        repository_path = details['path']
        repository_path += '_restore'

        data_store = versioned_storage(repository_path)

        # Download head pointer
        fle = BytesIO()
        streaming_download(s3_conn, config, repository_name + '/head',
                           version_id = None,
                           file_handle = fle)

        fle.seek(0)
        commit_pointer = fle.read().decode('utf8')

        with open(cpjoin(repository_path, 'head'), 'w') as fle:
            fle.write(commit_pointer)

        while True:
            # Download commit
            object_path = commit_pointer[:2] + '/' + commit_pointer[2:]
            fs_path = cpjoin(repository_path, 'index', object_path)

            print('Downloading commit: ' + object_path)

            make_dirs_if_dont_exist(os.path.dirname(fs_path))
            with open(fs_path, 'wb') as fle:
                streaming_download(s3_conn, config,
                                   remote_file_path = repository_name + '/index/' + object_path,
                                   version_id       = None,
                                   file_handle      = fle)

            commit = data_store.read_commit_index_object(commit_pointer)

            # download tree objects pointed to by commit
            tree_objects = {}
            def recursive_helper(tree_object_hash):
                nonlocal tree_objects

                object_path = tree_object_hash[:2] + '/' + tree_object_hash[2:]
                fs_path = cpjoin(repository_path, 'index', object_path) # pylint: disable=W0640

                print('Downloading tree: ' + object_path)

                make_dirs_if_dont_exist(os.path.dirname(fs_path)) # pylint: disable=W0640
                with open(fs_path, 'wb') as fle: # pylint: disable=W0640
                    streaming_download(s3_conn, config,
                                       remote_file_path = repository_name + '/index/' + object_path, # pylint: disable=W0640
                                       version_id       = None,
                                       file_handle      = fle)

                it = data_store.read_tree_index_object(tree_object_hash) # pylint: disable=W0640
                tree_objects[tree_object_hash] = it # pylint: disable=W0640

                for dir_hash in it['dirs'].values():
                    recursive_helper(dir_hash) # pylint: disable=W0640

            recursive_helper(commit['tree_root'])

            # download files in commit
            for tree in tree_objects.values():
                for fle in tree['files'].values():

                    object_path = fle['hash'][:2] + '/' + fle['hash'][2:]
                    fs_path = cpjoin(repository_path, 'files', object_path)

                    print('Downloading file: ' + object_path)

                    make_dirs_if_dont_exist(os.path.dirname(fs_path))

                    # Recreate empty files as they don't get stored
                    if fle['hash'] == empty_file_identifier:
                        print('empty')
                        with open(fs_path, 'w'):
                            pass

                    else:
                        with open(fs_path, 'wb') as fle:
                            streaming_download(s3_conn, config,
                                            remote_file_path = repository_name + '/files/' + object_path,
                                            version_id       = None,
                                            file_handle      = fle)

            # update commit pointer if not 'root'
            commit_pointer = commit['parent']
            if commit_pointer == 'root':
                break


#===============================================================================
def run():
    args = list(sys.argv)[1:]

    #----------------------------
    if len(args) == 0 or args[0] == '-h': print("""

    Usage: bvn_repo [command]

    verify                       : Verify repository contents has not been corrupted

    backup                       : Backup repositories

    restore                      : Restore repositories

    """)

    #----------------------------
    elif args[0] == 'verify':
        results = []
        print()
        for repository_name, details in config['repositories'].items():
            print('Verifying repository: ' + repository_name)
            print()

            repository_path = details['path']
            data_store = versioned_storage(repository_path)

            valid, issues = data_store.verify_fs()

            results.append({
                'name'   : repository_name,
                'valid'  : valid,
                'issues' : issues
            })

        for result in results:
            print()
            print('Results for: ' + result['name'])
            print()
            if result['valid']:
                print('No issues')
            else:
                print('Repo has the following issues:')

                for it in result['issues']:
                    print(it)

            print()

    #----------------------------
    elif args[0] == 'gc':

        print()

        for repository_name, details in config['repositories'].items():
            print('Running GC on repository: ' + repository_name)
            print()

            repository_path = details['path']
            data_store = versioned_storage(repository_path)

            data_store.garbage_collect()

    #----------------------------
    elif args[0] == 'backup':
        backup()

    #----------------------------
    elif args[0] == 'restore':
        restore()

    else:
        print('Unknowm command. Run bvn_repo -h to list commands.')
