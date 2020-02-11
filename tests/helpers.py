import shutil, os
DATA_DIR           = os.path.dirname(__file__) + '/filesystem_tests/'

############################################################################################
# Helpers
############################################################################################
def make_data_dir():
    try: os.mkdir(DATA_DIR)
    except OSError: pass

def delete_data_dir():
    try: shutil.rmtree(DATA_DIR)
    except OSError: pass

