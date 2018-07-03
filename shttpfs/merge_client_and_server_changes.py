def merge_client_and_server_changes(server, client):
    server_copy = server.copy()
    client_copy  = client.copy()

    result = {'client_push_files' : [], 'client_pull_files' : [], 'to_delete_on_client' : [], 'to_delete_on_server' : [], 'conflict_files' : []}

    # First handle file change detection from the servers perspective
    for server_file_name, server_file_info in server.iteritems():
        # Remove files from the dict as we go, what is left over after this is files that are new on the client and do not exist on the server
        server_copy.pop(server_file_name)
        if server_file_name in client_copy: client_copy.pop(server_file_name)

        # If file new or changed on server and does not exist, or has not changed on the client, push it to the client
        if server_file_name not in client and server_file_info['status'] in ['new', 'changed']:
            result['client_pull_files'].append(server_file_info)

        # If file deleted on server and unchanged on the client, delete it from the client
        elif server_file_name not in client and server_file_info['status'] == 'deleted':
            result['to_delete_on_client'].append(server_file_info)

        #===================================
        # Handle items which are changed on the client and changed or deleted on the server
        #===================================
        elif server_file_name in client and client[server_file_name]['status'] in ['new', 'changed']:
            # Files changed on the client and server are conflicts
            if server_file_info['status'] in ['new', 'changed']:
                result['conflict_files'].append({
                    'client_status' : 'Changed',
                    'server_status' : 'Changed',
                    'file_info'     : server_file_info})

            # Files changed on the client and deleted on the server are conflicts
            elif server_file_info['status'] == 'deleted':
                result['conflict_files'].append({
                    'client_status' : 'Changed',
                    'server_status' : 'Deleted',
                    'file_info'     : server_file_info})

        #===================================
        # Handle items which are deleted on the client and changed or deleted on the server
        #===================================
        elif server_file_name in client and client[server_file_name]['status'] == 'deleted':
            # Files deleted on the client and changed on the server are conflicts
            if server_file_info['status'] in ['new', 'changed']:
                result['conflict_files'].append({
                    'client_status' : 'Deleted',
                    'server_status' : 'Changed',
                    'file_info'     : server_file_info})

            # If the same file was deleted on the client and the server, delete the client file,
            # there is no point in treating this as a conflict
            elif server_file_info['status'] == 'deleted':
                result['to_delete_on_client'].append(server_file_info)

    # Secondly deal with any files left over from the client
    for key, value in client_copy.iteritems():
        # If file new on server or changed by another client and does not exist on the client, get client to pull it
        if value['status'] in ['new', 'changed']:
            result['client_push_files'].append(value)

        # If file has not changed on the on server and has been deleted on the client, delete it on the server
        if value['status'] == 'deleted':
            result['to_delete_on_server'].append(value)

    return result
