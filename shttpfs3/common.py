import os.path, hashlib, errno, copy
from typing import List, Dict, Any, cast
from typing_extensions import TypedDict
from termcolor import colored

#===============================================================================
def find_shttpfs_dir() -> str:
    """ Looks up the directory tree from the pwd to find a directory containing
    a .shttpfs directory, and returns that path """

    cwd:        str = os.getcwd() + '/'
    split_path: str = cwd.split('/')

    while True:
        joined_path = '/'.join(split_path)
        joined_path = '/' if joined_path == '' else '/' + joined_path + '/'

        if os.path.isdir(joined_path + '.shttpfs'):
            return joined_path
            break

        if joined_path == []:
            raise SystemExit('Not a shttpfs checkout, could not find a .shttpfs directory in parent dirs')

        split_path.pop()

#===============================================================================
def question_user(prompt_text: str, valid_choices) -> str:
    choice = None
    while True:
        print(prompt_text)
        choice = input()
        if choice.lower() in ['y', 'n']: break

    return choice

############################################################################
def ignore(*args):
    """ Calls function passed as argument zero and ignores any exceptions raised by it """

    try: return args[0](*args[1:])
    except Exception: pass # pylint: disable=broad-except

############################################################################################
def force_unicode(text) -> str:
    """ Encodes a string as UTF-8 if it isn't already """
    try: return str(text, 'utf-8')
    except TypeError: return text

############################################################################################
def display_file_path_list(prefix: str, l : List[Dict], color : str) -> None:
    """ Prints a file list to terminal, allows colouring output. """
    for itm in l: print(colored(prefix + itm['path'], color))


# +++++
# File system related stuff
# +++++
############################################################################################
def pfx_path(path : str) -> str:
    """ Prefix a path with the OS path separator if it is not already """
    if path[0] != os.path.sep: return os.path.sep + path
    else:                      return path


############################################################################################
def file_get_contents(path : str) -> bytes:
    """ Returns contents of file located at 'path' """
    with open(path, 'rb') as f:
        return f.read()

############################################################################################
def file_put_contents(path: str, data: bytes) -> None:
    """ Put passed contents into file located at 'path' """
    with open(path, 'wb') as f:
        f.write(data); f.flush()

############################################################################################
def file_or_default(path: str, default: Any) -> bytes:
    """ Return a default value if a file does not exist """
    try:
        return file_get_contents(path)
    except IOError as e:
        if e.errno == errno.ENOENT: return default
        raise

############################################################################################
def make_dirs_if_dont_exist(path: str) -> None:
    """ Create directories in path if they do not exist """
    if path[-1] not in ['/']: path += '/'
    path = os.path.dirname(path)
    if path != '':
        try: os.makedirs(path)
        except OSError: pass

############################################################################################
def cpjoin(*args: str) -> str:
    """ custom path join """
    rooted = bool(args[0].startswith('/'))
    def deslash(a): return a[1:] if a.startswith('/') else a
    newargs = [deslash(arg) for arg in args]
    path = os.path.join(*newargs)
    if rooted: path = os.path.sep + path
    return path

############################################################################################
class fileDetails (TypedDict):
    path:     str
    created:  float
    last_mod: float

############################################################################################
def get_single_file_info(f_path: str, int_path: str) -> fileDetails:
    """ Gets the creates and last change times for a single file,
    f_path is the path to the file on disk, int_path is an internal
    path relative to a root directory.  """
    return { 'path'     : force_unicode(int_path),
             'created'  : os.path.getctime(f_path),
             'last_mod' : os.path.getmtime(f_path)}

############################################################################################
def hash_file(file_path: str, block_size: int = 65536) -> str:
    """ Hashes a file with sha256 """
    sha = hashlib.sha256()
    with open(file_path, 'rb') as h_file:
        while True:
            file_buffer = h_file.read(block_size)
            if len(file_buffer) == 0: break
            sha.update(file_buffer)
    return sha.hexdigest()

############################################################################################
def get_file_list(path: str) -> List[fileDetails]:
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
class manifestFileDetails(fileDetails):
    status: str

############################################################################################
def find_manifest_changes(new_file_state : List[fileDetails], old_file_state : Dict[str, manifestFileDetails]) -> Dict[str, manifestFileDetails]:
    """ Find what has changed between two sets of files """
    prev_state_dict = copy.deepcopy(old_file_state)
    changed_files = {}

    # Find files which are new on the server
    for itm in new_file_state:
        if itm['path'] in prev_state_dict:
            d_itm = prev_state_dict.pop(itm['path'])

            # If the file has been modified
            if itm['last_mod'] != d_itm['last_mod']:
                n_itm = cast(manifestFileDetails, itm.copy())
                n_itm['status'] = 'changed'
                changed_files[itm['path']] = n_itm
            else:
                pass # The file has not changed

        else:
            n_itm = cast(manifestFileDetails, itm.copy())
            n_itm['status'] = 'new'
            changed_files[itm['path']] = n_itm

    # any files remaining in the old file state have been deleted locally
    for itm in prev_state_dict.values():
        n_itm = cast(manifestFileDetails, itm.copy())
        n_itm['status'] = 'deleted'
        changed_files[itm['path']] = n_itm

    return changed_files
