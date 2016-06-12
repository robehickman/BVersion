#Client configuration
import __builtin__

__builtin__.SERVER_URL           = 'http://localhost:5000/'
__builtin__.DATA_DIR             = './'
__builtin__.MANIFEST_FILE        = '.manifest_xzf.json'
__builtin__.REMOTE_MANIFEST_FILE = '.remote_manifest_xzf.json'
__builtin__.IGNORE_FILTER_FILE   = '.pysync_ignore'
__builtin__.PULL_IGNORE_FILE     = '.pysync_pull_ignore'


# need to implement a 'local delete' command, delete files
# locally, remove from manifest and add to pull ignore

#########################################################
# Imports
#########################################################
# client.py
from common import *
import json
from poster.encode import multipart_encode
from poster.streaminghttp import register_openers
import urllib2

# Register the streaming http handlers with urllib2
register_openers()

#########################################################
# Detect which, if any files have been changed locally
#########################################################
def detect_local_changes(manifest):
    #extract old local file list from manifest
    old_f_list = manifest['files']
    old_f_list_dict = make_dict(old_f_list)


    # create a list of all files including there modification times

    f_list = get_file_list(DATA_DIR)

    f_list_dict = make_dict(f_list)

    # Check for changes since last run
    new_files = []
    changed_files = []
    deleated_files = []

    for r_itm in f_list:
        if  r_itm['path'] in old_f_list_dict:
            # Remove and check for changes
            old_itm = old_f_list_dict.pop(r_itm['path'])

            if r_itm['last_mod'] !=  old_itm['last_mod']:
                changed_files.append(r_itm)
            else:
                pass # file has not changed, do nothing

        else:
            new_files.append(r_itm)

    # anything remaining in the dict has been deleted
    for key, value in old_f_list_dict.iteritems():
        deleated_files.append(value)

    return (new_files, changed_files, deleated_files)

#########################################################
# Do actual file sync
#########################################################
def sync_files(manifest, new_files, changed_files, deleated_files):
    # Get previous server manifest
    remote_manifest = read_remote_manifest()

    #send list to server, which will return changes
    result = do_request("find_changed", {
        "client_manifest" : json.dumps(manifest),
        "prev_manifest"   : json.dumps(remote_manifest),
        "new_files"       : json.dumps(new_files),
        "changed_files"   : json.dumps(changed_files),
        "deleted_files"   : json.dumps(deleated_files)});

    result = json.loads(result)

    if(result['status'] == 'ok'):
    # update manifests to account for files deleted on the remote
        remote_manifest = result['remote_manifest']
        write_remote_manifest(remote_manifest)


        if deleated_files != []:
            deleted_dict = make_dict(deleated_files)
            manifest = read_manifest()
            filter_manifest = []
            for f in manifest['files']:
                if f['path'] in deleted_dict:
                    pass # file is deleted, remove it from the manifest
                else:
                    filter_manifest.append(f)
            manifest['files'] = filter_manifest
            write_manifest(manifest) 

        

    #see if there is anything that needs pulling or pushing
        errors = []

        if result['push_files'] == [] and result['pull_files'] == []:
            print 'Nothing to do'

        # Push files
        for file in result['push_files']:

            print 'Sending: ' + file

            req_result = do_request("push_file", {
                "file": open(DATA_DIR + file, "rb"), 'path' : file})

            responce = json.loads(req_result)
            if responce['status'] == 'ok':
                last_change = responce['last_change']

                print 'Uploaded: ' + last_change['path']

                # update local and remote manifest after every upload to not re-upload files
                # if the system fails mid-sync


                manifest = read_manifest()
                manifest['files'].append(get_single_file_info(
                    DATA_DIR + last_change['path'], last_change['path']))
                write_manifest(manifest)

                remote_manifest = read_remote_manifest()
                remote_manifest['files'].append(last_change)
                write_remote_manifest(remote_manifest)
            else:
                errors.append(responce['last_path'])
            


        try:
            pull_ignore = file_get_contents(DATA_DIR + PULL_IGNORE_FILE)
            pull_ignore = pull_ignore.splitlines()

            filtered_pull_files = []
            for f in result['pull_files']:
                matched = False
                for i in pull_ignore:
                    if fnmatch.fnmatch(f, i):
                        matched = True

                if matched == False:
                    filtered_pull_files.append(f)

            result['pull_files'] = filtered_pull_files
        except:
            print 'Warning, pull ignore file does not exist'
                    
        # Get files
        for file in result['pull_files']:
            path = DATA_DIR + file
            print 'Pulling file: ' + path

            req_result = do_request("pull_file", {
                'path' : file})

            try:
                os.makedirs(os.path.dirname(path))
            except:
                pass # dir already exists
            
            file_put_contents(path, req_result)

            manifest = read_manifest()
            manifest['files'].append(get_single_file_info(path, file))
            write_manifest(manifest)

            print 'Done' 

    # write manifest
    manifest = read_manifest()
    f_list = get_file_list(DATA_DIR)
    manifest['files'] = f_list
    write_manifest(manifest)

    write_remote_manifest(result['remote_manifest'])

    return errors

#########################################################
#########################################################
manifest = read_manifest()

new_files, changed_files, deleated_files = detect_local_changes(manifest);

display_list('New: ',     new_files, 'green')
display_list('Changed: ', changed_files, 'yellow')
display_list('Deleted: ', deleated_files, 'red')


sync_files(manifest, new_files, changed_files, deleated_files)



