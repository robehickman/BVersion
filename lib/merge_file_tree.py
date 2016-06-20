def merge_file_tree(remote, local):
    remote_copy = remote.copy()
    local_copy  = local.copy()

    push_files          = []
    pull_files          = []
    conflict_files      = []
    local_delete_files  = []
    remote_delete_files = []

    # First handle file change detection from the servers perspective
    for key, value in remote.iteritems():
        remote_copy.pop(key)
        if key in local_copy:
            local_copy.pop(key)

        # If file new on client and does not exist on the server, push it to the server
        if key not in local and value['status'] == 'new':
            push_files.append(value)
            continue

        # If file changed on client and unchanged the server, push it to the server
        # Server unchanged files are filtered out before this step, so only changed
        # files will be listed in the 'local' duct.
        if key not in local and value['status'] == 'changed':
            push_files.append(value)
            continue

        # If file deleted on client and unchanged on the server, delete it on the server
        if key not in local and value['status'] == 'deleted':
            local_delete_files.append(value)
            continue

        # If file was added on the client which conflicts with one added to the server by another client
        if key in local and value['status'] == 'new' and local[key]['status'] == 'new':
            conflict_files.append(value)
            continue

        # If file was added on the client which conflicts with one added to the server by another client
        if key in local and value['status'] == 'changed' and local[key]['status'] == 'new':
            conflict_files.append(value)
            continue

        # If file was added on the client which conflicts with one added to the server by another client
        if key in local and value['status'] == 'deleted' and local[key]['status'] == 'new':
            conflict_files.append(value)
            continue

        # If file was added on the client which conflicts with one changed on the server by another client
        if key in local and value['status'] == 'new' and local[key]['status'] == 'changed':
            conflict_files.append(value)
            continue

        # If file was changed on the client which conflicts with one changed on the server by another client
        if key in local and value['status'] == 'changed' and local[key]['status'] == 'changed':
            conflict_files.append(value)
            continue

        # If file was deleted on the client which conflicts with one changed on the server by another client
        if key in local and value['status'] == 'deleted' and local[key]['status'] == 'changed':
            conflict_files.append(value)
            continue

        # If file was added on the client which conflicts with one deleted on the server by another client
        if key in local and value['status'] == 'new' and local[key]['status'] == 'deleted':
            conflict_files.append(value)
            continue

        # If file was changed on the client which conflicts with one deleted on the server by another client
        if key in local and value['status'] == 'changed' and local[key]['status'] == 'deleted':
            conflict_files.append(value)
            continue

        # If file was changed on the client which conflicts with one deleted on the server by another client
        if key in local and value['status'] == 'deleted' and local[key]['status'] == 'deleted':
            continue

    # remote_copy should now be empty
    print remote_copy

    # Secondly deal with any files left over from the client, these will mainly be new files
    for key, value in local_copy.iteritems():
        # If file new on server and does not exist on the client, get client to pull it
        if key not in remote_copy and value['status'] == 'new':
            pull_files.append(value)
            continue

        # If file changed on server and unchanged the client, push it to the client
        if key not in remote_copy and value['status'] == 'changed':
            pull_files.append(value)
            continue

        # If file deleted on server and unchanged on the client, delete it on the client
        if key not in remote_copy and value['status'] == 'deleted':
            remote_delete_files.append(value)
            continue

    return (push_files, pull_files, conflict_files, local_delete_files, remote_delete_files)

