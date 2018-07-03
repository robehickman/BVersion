import os.path, json, hashlib, errno, copy
from termcolor import colored
from pprint import pprint

############################################################################
def ignore(*args):
    """ Calls function passed as argument zero and ignores any exceptions raised by it """
    try: return args[0](*args[1:])
    except: pass

############################################################################################
def force_unicode(text):
    """ Encodes a string as UTF-8 if it isn't already """
    try: return unicode(text, 'utf-8')
    except TypeError: return text

############################################################################################
def display_list(prefix, l, color):
    """ Prints a file list to terminal, allows colouring output. """
    for itm in l: print colored(prefix + itm['path'], color)


# +++++
# File system related stuff
# +++++
############################################################################################
def pfx_path(path):
    """ Prefix a path with the OS path separator if it is not already """
    if(path[0] != os.path.sep): return os.path.sep + path
    return path


############################################################################################
def file_get_contents(path):
    """ Returns contents of file located at 'path' """
    with open(path, 'r') as f:
        return f.read()

############################################################################################
def file_put_contents(path, data):
    """ Put passed contents into file located at 'path' """
    with open(path, 'w') as f:
        f.write(data); f.flush()

############################################################################################
def file_or_default(path, default, function = None):
    """ Return a default value if a file does not exist """
    try:
        result = file_get_contents(path)
        if function != None: return function(result)
        return result
    except IOError as e:
        if e.errno == errno.ENOENT: return default
        raise

############################################################################################
def make_dirs_if_dont_exist(path):
    """ Create directories in path if they do not exist """
    if path[-1] not in ['/']: path += '/'
    path = os.path.dirname(path)
    if path != '':
        try: os.makedirs(path)
        except OSError: pass

############################################################################################
def allowed_path(path):
    """ Block '..' from occurring in file paths, this should not happen under normal operation. """

    split = path.split('/')
    if any(True for x in split if x in ['..', '.']):
        raise Exception(e)

############################################################################################
def cpjoin(*args):
    """ custom path join """
    rooted = True if args[0].startswith('/') else False
    def deslash(a): return a[1:] if a.startswith('/') else a
    newargs = [deslash(arg) for arg in args]
    path = os.path.join(*newargs)
    if rooted == True: path = os.path.sep + path 
    return path

############################################################################################
def get_single_file_info(f_path, int_path):
    """ Gets the creates and last change times for a single file,
    f_path is the path to the file on disk, int_path is an internal
    path relative to a root directory.  """
    return { 'path'     : force_unicode(int_path),
             'created'  : os.path.getctime(f_path),
             'last_mod' : os.path.getmtime(f_path)}

############################################################################################
def hash_file(file_path, block_size = 65536):
    """ Hashes a file with sha256 """
    sha = hashlib.sha256()
    with open(file_path, 'rb') as h_file:
        file_buffer = h_file.read(block_size)
        while len(file_buffer) > 0:
            sha.update(file_buffer)
            file_buffer = h_file.read(block_size)
    return sha.hexdigest()

############################################################################################
def get_file_list(path):
    """ Recursively lists all files in a file system below 'path'. """
    f_list = []
    def recur_dir(path, newpath = os.path.sep):
        files = os.listdir(path) 
        for fle in files:
            f_path = cpjoin(path, fle)
            if os.path.isdir(f_path): recur_dir(f_path, cpjoin(newpath, fle))
            elif os.path.isfile(f_path): f_list.append(get_single_file_info(f_path, cpjoin(newpath, fle)))

    recur_dir(path)
    return f_list

############################################################################################
def find_manifest_changes(new_file_state, old_file_state):
    """ Find what has changed between two sets of files """
    prev_state_dict = copy.deepcopy(old_file_state)
    changed_files = {}

    # Find files which are new on the server
    for itm in new_file_state:
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
            n_itm = itm.copy()
            n_itm['status'] = 'new'
            changed_files[itm['path']] = n_itm

    # any files remaining in the old file state have been deleted locally
    for key, itm in prev_state_dict.iteritems():
        n_itm = itm.copy()
        n_itm['status'] = 'deleted'
        changed_files[itm['path']] = n_itm

    return changed_files

