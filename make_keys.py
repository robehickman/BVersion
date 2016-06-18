import os, getpass, argparse, binascii
import sys

sys.path = [ './lib' ] + sys.path

from common import *
from crypto import *

print ''

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
# Handle command line arguments
############################################################################
def put_if_does_not_exist(path, contents):
    if not os.path.isfile(path):
        with open(path + '.key', 'w') as f:
            f.write(contents)

    else:
        print 'Error, key file already exists'

############################################################################
# Handle command line arguments
############################################################################
parser = argparse.ArgumentParser(description='Generate public and private key pair for authentication.')
parser.add_argument('pubkey', nargs=1 , help='Public key file')
parser.add_argument('privkey', nargs=1, help='Private key file')
args = parser.parse_args()

# read args
pubkey_file  = args.privkey[0]
privkey_file = args.pubkey[0]

# need to check if these files exist already

if pubkey_file == privkey_file:
    print 'Error: public and private key file names are identical.'
else:
    # get password to encrypt private key
    password = prompt_for_new_password()

    # make the key pair
    public, private = make_keypair()
    encrypted_private = encrypt_private(password, private)

    make_dirs_if_dont_exist(pubkey_file)
    make_dirs_if_dont_exist(privkey_file)

    put_if_does_not_exist(pubkey_file + '.key', binascii.hexlify(public))
    put_if_does_not_exist(privkey_file + '.key', binascii.hexlify(encrypted_private))

