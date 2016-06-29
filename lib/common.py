import os.path, time, fnmatch, json, getpass
from termcolor import colored
import ConfigParser


from poster.encode import multipart_encode
from poster.streaminghttp import register_openers
import urllib2

############################################################################
# Prompt the user to enter a new password, with confirmation
############################################################################
def prompt_for_new_password():
    while True:
        passw = getpass.getpass()
        passw2 = getpass.getpass()

        if passw == passw2:
            break
        else:
            print 'Passwords do not match'
    return passw

############################################################################
# Prompt the user to enter there password
############################################################################
def prompt_for_password():
    return getpass.getpass()

############################################################################
# Read config file into dictionary
############################################################################
def read_config(file):
	conf_file = ConfigParser.ConfigParser()
	conf_file.read(file)

	config = {}

	for section in conf_file.sections():
		sect = {}

		options = conf_file.options(section)

		for option in options:
			sect[option] = conf_file.get(section, option)
		config[section] = sect
	return config

############################################################################
# Create directories in path if they do not exist
############################################################################
def make_dirs_if_dont_exist(path):
    path = os.path.dirname(path)
    if path != '':
        try:
            os.makedirs(path)
        except OSError:
            pass

############################################################################
# Make sure path ends with the correct extension
############################################################################
def exsure_extension(path, ext):
    if path.endswith(ext):
        return path
    else:
        return path + ext

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
# custom path join
############################################################################################
def cpjoin(*args):
    rooted = False
    if args[0].startswith('/'):
        rooted = True

    # remove leading and trailing slashes
    newargs = []
    for arg in args:
        acopy = arg
        if acopy.startswith('/'):
            acopy = acopy[1:] # remove leading slashes
        newargs.append(acopy)

    path = os.path.join(*newargs)
    if rooted == True:
        path = os.path.sep + path 

    return path

############################################################################################
# Gets last change time for a single file
############################################################################################
def get_single_file_info(f_path, int_path):
    return { 'path'     : force_unicode(int_path),
             'created'  : os.path.getctime(f_path),
             'last_mod' : os.path.getmtime(f_path)}


############################################################################################
# Obtains a list of all files in a file system.
############################################################################################
def get_file_list(path):
    f_list = []
    def recur_dir(path, newpath = os.path.sep):
        files = os.listdir(path) 

        for fle in files:
            f_path = cpjoin(path, fle)
            if os.path.isdir(f_path):
                recur_dir(f_path, cpjoin(newpath, fle))
            elif os.path.isfile(f_path):
                f_list.append(get_single_file_info(f_path, cpjoin(newpath, fle)))

    recur_dir(path)

    f_list = apply_ignore_filters(f_list)

    return f_list

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
        filters.append('/' + CLIENT_CONF_DIR + '*')
        filters.append('/' + REMOTE_MANIFEST_FILE)
        filters.append('/' + PULL_IGNORE_FILE)
    except:
        pass # on the server remote manifest does not exist

    for f in filters:
        f_list = filter_f_list(f_list, f)

    return f_list
    



############################################################################################
# Find what has changed between two manifests
############################################################################################
def find_manifest_changes(files_state1, files_state2):
    prev_state_dict = make_dict(files_state2)

    changed_files = {}

    # Find files which are new on the server
    for itm in files_state1:
        if itm['path'] in prev_state_dict:
            d_itm = prev_state_dict.pop(itm['path'])
            
        # If the file has been modified
            if itm['last_mod'] != d_itm['last_mod']:
                n_itm = itm.copy()
                n_itm['status'] = 'changed'
                changed_files[itm['path']] = n_itm
            else:
                pass # The file has not changed

        else:
            # anything here was not found in the remote manifest is new on the server
            n_itm = itm.copy()
            n_itm['status'] = 'new'
            changed_files[itm['path']] = n_itm


# any files remaining in the remote manifest have been deleted locally
    for key, itm in prev_state_dict.iteritems():
        n_itm = itm.copy()
        n_itm['status'] = 'deleted'
        changed_files[itm['path']] = n_itm

    return changed_files


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

############################################################################################
# Send a HTTP request to the server
############################################################################################
def do_request(url, data):
    datagen, headers = multipart_encode(data)
    request = urllib2.Request(SERVER_URL + url, datagen, headers)
    result = urllib2.urlopen(request)
    return result.read()


############################################################################################
# Send a HTTP request to the server, get body and headers
############################################################################################
def do_request_full(url, data):
    datagen, headers = multipart_encode(data)
    request = urllib2.Request(SERVER_URL + url, datagen, headers)
    result = urllib2.urlopen(request)
    return (result.read(), result.info())


