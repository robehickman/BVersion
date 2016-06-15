import os.path, time
from termcolor import colored
import fnmatch
import json


from poster.encode import multipart_encode
from poster.streaminghttp import register_openers
import urllib2

############################################################################################
# Makes sure a string is utf-8
############################################################################################
def force_unicode(text):
    try:
        text = unicode(text, 'utf-8')
        return text
    except TypeError:
        return text


############################################################################################
# Prefix a path with the OS path separator if it is not already
############################################################################################
def pfx_path(path):
    if(path[0] != os.path.sep): return os.path.sep + path
    return path


############################################################################################
# Gets last change time for a single file
############################################################################################
"""
def get_single_file_info(f_path, int_path):

    return { 'path'     : force_unicode(int_path),
             'created'  : os.path.getctime(f_path),
             'last_mod' : os.path.getmtime(f_path)}
"""

############################################################################################
# Obtains a list of all files in a file system.
############################################################################################

"""
def get_file_list(path):

    f_list = []
    def recur_dir(path, newpath = '/'):
        files = os.listdir(path) 

        for file in files:
            f_path = path + file
            if os.path.isdir(f_path):
                recur_dir(f_path + '/', newpath + file + '/')
            elif os.path.isfile(f_path):
                f_list.append(get_single_file_info(f_path, newpath + file))

    recur_dir(path)

    f_list = apply_ignore_filters(f_list)

    return f_list
"""


############################################################################################
# Convert file list into a dictionary with the file path as its key, and meta data as a
# list stored as the keys value. This format change makes searching easier.
############################################################################################
def make_dict(s_list):
    new_dict = {} 
    for l_itm in s_list:
        new_dict[l_itm['path']] = l_itm

    return new_dict

############################################################################################
# Looks in dict for key, and prints an error plus raises an exception if the key is missing.
############################################################################################
def error_not_in_dict(dct, what, msg):
    if what not in dct:
        print msg
        raise Exception(msg)

############################################################################################
# Prints a file list to terminal, allows colouring output.
############################################################################################
def display_list(prefix, l, color):
    for itm in l:
        print colored(prefix + itm['path'], color)

############################################################################################
# Removes files from list by unix-type wild cards, used to implement ignored files.
############################################################################################
def filter_f_list(f_list, filters):
    f_list_filter = []
    for itm in f_list:
        if fnmatch.fnmatch(itm['path'], filters):
            pass
        else:
            f_list_filter.append(itm)
    return f_list_filter;

############################################################################################
# Loads file ignore filters from IGNORE_FILTER_FILE and applies them to file list passed
############################################################################################
def apply_ignore_filters(f_list):
    filters = []

    try:
        IGNORE_FILTER_FILE

        try:
            f_file = file_get_contents(DATA_DIR + IGNORE_FILTER_FILE)
            lines = f_file.splitlines()
            filters = filters + lines
        except:
            print 'Warning: filters file does not exist'
    except NameError:
        print 'Warning: configuration var IGNORE_FILTER_FILE is not defined'
    
    try:
        filters.append('/' + MANIFEST_FILE)
        filters.append('/' + REMOTE_MANIFEST_FILE)
        filters.append('/' + PULL_IGNORE_FILE)
    except:
        pass # on the server remote manifest does not exist

    for f in filters:
        f_list = filter_f_list(f_list, f)

    return f_list
    


############################################################################################
# Read client manifest file.
############################################################################################
"""
def read_manifest():
    # Read Manifest
    try:
        manifest = json.loads(file_get_contents(DATA_DIR + MANIFEST_FILE))
    except:
        # no manifest, create one, manifest stores file access times as
        # of last run to detect files which have changed
        manifest = {
            'format_vers' : 1,
            'root'        : '/',
            'files'       : []
        }

    return manifest

############################################################################################
# Read server manifest file
############################################################################################
def read_server_manifest():
    manifest = read_manifest()
    if(manifest['files'] == []):
        manifest['files'] = get_file_list(DATA_DIR)
    return manifest

############################################################################################
# Write manifest file
############################################################################################
def write_manifest(manifest):
    file_put_contents(DATA_DIR + MANIFEST_FILE, json.dumps(manifest))

############################################################################################
# Write remote manifest file
############################################################################################
def write_remote_manifest(manifest):
    file_put_contents(DATA_DIR + REMOTE_MANIFEST_FILE, json.dumps(manifest))

############################################################################################
# Read locally stored remote manifest. The server sends this data at the end of each
# sync request. It is used to detect file changes on the server since the last run.
# Storing this client side saves having to store individual copies of this data for every
# connected client on the server.
#
# If this is a first run the remote manifest is obtained from the server.
############################################################################################
def read_remote_manifest():
    try:
        manifest = json.loads(file_get_contents(DATA_DIR + SERVER_MANIFEST_FILE))
    except:
        result = do_request("get_manifest", {})
        manifest = json.loads(result)

    return manifest

############################################################################################
# Write remote manifest file
############################################################################################
def write_remote_manifest(manifest):
    file_put_contents(DATA_DIR + REMOTE_MANIFEST_FILE, json.dumps(manifest))
"""

############################################################################################
# Block '..' from occurring in file paths, this should not happen under normal operation.
############################################################################################
def allowed_path(path):
    udir = path.split('/')
    for x in udir:
        if(x == '..'):
            e = '.. in file paths not aloud'
            print e
            raise Exception(e)

############################################################################################
# Validate sync request
############################################################################################
def validate_request(r):
    if 'file' not in r.files:
        e = 'file var does not exist'
        print e
        raise Exception(e)

    if r.files['file'].filename == '':
        e = 'file name cannot be empty'
        print e
        raise Exception(e)

    if 'path' not in r.form:
        e = 'path var does not exist'
        print e
        raise Exception(e)

    allowed_path(r.form['path'])

"""
############################################################################################
# Returns contents of file located at 'path'
############################################################################################
def file_get_contents(path):
    if os.path.isfile(path):  
        with open(path, 'r') as f:
            result = f.read()
    else:
        raise Exception('file not found')
    return result

############################################################################################
# Put passed contents into file located at 'path'
############################################################################################
def file_put_contents(path, data):
    with open(path, 'w') as f:
        f.write(data)
"""

############################################################################################
# Send a HTTP request to the server
############################################################################################
def do_request(url, data):
    datagen, headers = multipart_encode(data)
    request = urllib2.Request(SERVER_URL + url, datagen, headers)
    return urllib2.urlopen(request).read()



