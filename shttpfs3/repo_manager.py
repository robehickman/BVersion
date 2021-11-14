import sys
from shttpfs3.common import cpjoin, file_get_contents

from shttpfs3.backup import s3_interface, pipeline
from shttpfs3.storage.versioned_storage import versioned_storage

import pprint

# ---------------
config = {}
empty_file_identifier = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'

###################################################################################
def streaming_file_upload(s3_conn, config, remote_file_path, local_file_path):
    pipeline_configuration = pipeline.get_default_pipeline_format()

    #-----
    upload = s3_interface.streaming_upload()
    pl     = pipeline.build_pipeline_streaming(upload, 'out')
    pl.pass_config(config, pipeline.serialise_pipeline_format(pipeline_configuration))

    upload.begin(s3_conn, remote_file_path)

    try:
        with open(local_file_path, 'rb') as fle:
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

###################################################################################
def streaming_file_download(s3_conn, config, remote_file_path, version_id, local_file_path):
    download_stream  = s3_interface.streaming_download()
    header = download_stream.begin(s3_conn, remote_file_path, version_id)[0]
    pl                = pipeline.build_pipeline_streaming(download_stream, 'in')
    pl.pass_config(config, header)

    sfs.make_dirs_if_dont_exist(local_file_path)
    with open(local_file_path, 'wb') as fle:
        while True:
            res = pl.next_chunk()
            if res is None: break
            fle.write(res)

#===============================================================================
def init(new_config):
    global config

    """
    args = []


    conf = { 'local_lock_file'                : 'backup_lock',  # Path and name of the local lock file
             'chunk_size'                     : 1048576 * 5,    # minimum chunk size is 5MB on s3, 1mb = 1048576
             'meta_pipeline'                  : [],             # pipeline applied to meta files like manifest diffs
             'file_pipeline'                  : [[ '*', []]],   # pipeline applied to backed up files, list as sort order is important
             'split_chunk_size'               : 0}

    # Read and merge configuration file
    conf_file = 'configuration.json'
    if len(args) > 0 and args[0] == '--c':
        if len(args) < 2: raise SystemExit('Expected argument following --c to be a path to configuration file, nothing found.')
        args.pop(0); conf_file = args.pop(0)

    try:    parsed_config = json.loads(sfs.file_get_contents(conf_file))
    except FileNotFoundError: raise SystemExit(f"Configuration file {conf_file} not found.")

    core.validate_config(parsed_config)

    config = core.merge_config(config, parsed_config)

    # Setup the interface and core
    conn = interface.connect(config)
    config = pipeline.preprocess_config(interface, conn, config)
    core.init(interface, conn, config)

    # Set up format of the pipeline used for storing meta-data like manifest diffs
    global meta_pl_format, pl_in, pl_out
    meta_pl_format = pipeline.get_default_pipeline_format()
    meta_pl_format['format'].update({i : None for i in config['meta_pipeline']})
    if 'encrypt' in meta_pl_format['format']: meta_pl_format['format']['encrypt'] = config['crypto']['encrypt_opts']

    # ----
    pl_in  = pipeline.build_pipeline(functools.partial(interface.read_file, conn), 'in')
    pl_out = pipeline.build_pipeline(functools.partial(interface.write_file, conn), 'out')

    # Check for previous failed uploads and delete them
    if 'read_only' in config and not config['read_only']:
        interface.delete_failed_uploads(conn)
        garbage_collect(interface, conn, config, 'simple')
    """

    config = new_config

#===============================================================================
def backup():
    # Connect to the remote
    s3_conn = s3_interface.connect(config)

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
                def recursive_helper(tree_object_hash):
                    nonlocal tree_objects

                    it = data_store.read_tree_index_object(tree_object_hash)
                    tree_objects[tree_object_hash] = it

                    for dir_name, dir_hash in it['dirs'].items():
                        recursive_helper(dir_hash)

                recursive_helper(commit['tree_root'])

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
                        s3_interface.put_object(s3_conn, repository_name + '/' + it['path'], it['value'])

                    else:
                        file_hash = ''.join(it['dest'].split('/')[-2:])

                        # We skip uploading empty files, as s3 won't store them, they
                        # are recreated during the download stage.
                        if file_hash == empty_file_identifier:
                            continue

                        #-----
                        print('Uploading: ' + repository_name + '/' + it['dest'])

                        streaming_file_upload(s3_conn, config,
                                              remote_file_path = repository_name + '/' + it['dest'],
                                              local_file_path  = it['source'])

                # ========================================================================
                # Finally, create a 'commit' object to mark that everything in this
                # commit has been uploaded.
                # ========================================================================
                s3_interface.put_object(s3_conn, repository_name + '/commits/' + commit_hash, 'exists')
                print('Backed up commit: ' + commit_hash)

#===============================================================================
def restore():
    # Connect to the remote
    s3_conn = s3_interface.connect(config)

    for repository_name, details in config['repositories'].items():
        repository_path = details['path']
        repository_path += '_restore'

        data_store = versioned_storage(repository_path)

        # Download head

        print(repository_name + '/head')
        commit_pointer = s3_interface.get_object(s3_conn, repository_name + '/head')['body'].read().decode('utf8')

        print(commit_pointer)

        while True:
            pass

            # Download commit
            object_path = commit_pointer[:2] + '/' + commit_pointer[2:]
            print('Downloading commit: ' + object_path)
            streaming_file_download(s3_conn, config,
                                    remote_file_path = repository_name + '/index/' + object_path,
                                    version_id       = None,
                                    local_file_path  = repository_path + '/index/' + object_path)

            commit = data_store.read_commit_index_object(commit_pointer)

            # download tree objects pointed to by commit
            tree_objects = {}
            def recursive_helper(tree_object_hash):
                nonlocal tree_objects

                object_path = tree_object_hash[:2] + '/' + tree_object_hash[2:]
                print('Downloading tree: ' + object_path)
                streaming_file_download(s3_conn, config,
                                        remote_file_path = repository_name + '/index/' + object_path,
                                        version_id       = None,
                                        local_file_path  = repository_path + '/index/' + object_path)

                it = data_store.read_tree_index_object(tree_object_hash)
                tree_objects[tree_object_hash] = it

                for dir_name, dir_hash in it['dirs'].items():
                    recursive_helper(dir_hash)

            recursive_helper(commit['tree_root'])

            # download files in commit
            for tree in tree_objects.values():
                for fle in tree['files']:
                    object_path = fle['hash'][:2] + '/' + fle['hash'][2:]
                    print('Downloading file: ' + object_path)
                    streaming_file_download(s3_conn, config,
                                            remote_file_path = repository_name + '/files/' + object_path,
                                            version_id       = None,
                                            local_file_path  = repository_path + '/files/' + object_path)

            # update commit pointer if not 'root'
            commit_pointer = commit['parent']
            if commit_pointer == 'root':
                break


#===============================================================================
def run():
    args = list(sys.argv)[1:]

    #----------------------------
    if len(args) == 0 or args[0] == '-h': print("""

    Usage: shttpfs_repo [command]

    verify                       : Verify repository contents has not been courrupted

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
    elif args[0] == 'backup':
        backup()

    #----------------------------
    elif args[0] == 'restore':
        restore()

    else:
        print('Unknowm command. Run shttpfs_repo -h to list commands.')
