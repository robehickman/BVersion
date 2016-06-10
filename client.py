#Client configuration
import __builtin__

__builtin__.SERVER_URL           = 'http://localhost:5000/'
__builtin__.DATA_DIR             = './'
__builtin__.MANIFEST_FILE        = '.manifest_xzf.json'
__builtin__.REMOTE_MANIFEST_FILE = '.remote_manifest_xzf.json'


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
    f_list = filter_f_list(f_list, '*' + MANIFEST_FILE)
    f_list = filter_f_list(f_list, '*' + REMOTE_MANIFEST_FILE)


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
def sync_files(new_files, changed_files, deleated_files):
    # Get previous server manifest
    remote_manifest = read_remote_manifest()

    #send list to server, which will return changes
    result = do_request("find_changed", {
        "prev_manifest" : json.dumps(remote_manifest),
        "new_files"     : json.dumps(new_files),
        "changed_files" : json.dumps(changed_files),
        "deleted_files" : json.dumps(deleated_files)});

    result = json.loads(result)

    if result['push_files'] == [] and result['pull_files'] == []:
        print 'Nothing to do'

    # Push files
    for file in result['push_files']:
        req_result = do_request("push_file", {
            "file": open(DATA_DIR + file, "rb"), 'path' : file})
        # do better display of this
        print req_result

    # Get files
    for file in result['pull_files']:
        result = do_request("pull_file", {
            'path' : file})

        path = sauce + file
        
        file_put_contents(path, result)

        print 'Create file: ' + path 

    # write manifest
    f_list = get_file_list(DATA_DIR)
    f_list = filter_f_list(f_list, '*' + MANIFEST_FILE)
    f_list = filter_f_list(f_list, '*' + REMOTE_MANIFEST_FILE)
    manifest['files'] = f_list
    write_manifest(manifest)

    write_remote_manifest(result['remote_manifest'])

#########################################################
#########################################################
manifest = read_manifest()

new_files, changed_files, deleated_files = detect_local_changes(manifest);

display_list('New: ',     new_files, 'green')
display_list('Changed: ', changed_files, 'yellow')
display_list('Deleted: ', deleated_files, 'red')


sync_files(new_files, changed_files, deleated_files)



