DATA_DIR           = './vers_test/'
TMP_DIR            = '.tmp/'
BACKUP_DIR         = 'back/'
MANIFEST_FILE      = '.manifest_xzf.json'
JOURNAL_FILE       = '.journal.json'
JOURNAL_STEP_FILE  = '.journal_step'

import os, shutil

try: os.mkdir(DATA_DIR)
except: pass

############################################################################################
# Helpers
############################################################################################
def remove_dont_care(path):
    try: os.remove(path)
    except: pass

def empty_dir(path):
    try: shutil.rmtree(path)
    except: pass
    os.makedirs(path)

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


