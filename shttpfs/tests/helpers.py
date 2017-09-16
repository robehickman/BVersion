import os, shutil

DATA_DIR           = os.path.dirname(__file__) + '/vers_test/'
MANIFEST_FILE      = 'manifest_xzf.json'

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

