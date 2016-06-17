import os, getpass, argparse, binascii
from crypto import *

print ''

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

if pubkey_file == privkey_file:
    print 'Error: public and private key file names are identical.'
else:
    # get password to encrypt private key
    key = key_from_password(prompt_for_new_password())

    # make the key pair
    public, private = make_keypair()
    encrypted_private = encrypt_private(key, private)

    make_dirs_if_dont_exist(pubkey_file)
    make_dirs_if_dont_exist(privkey_file)

    put_if_does_not_exist(pubkey_file + '.key', binascii.hexlify(public))
    put_if_does_not_exist(privkey_file + '.key', binascii.hexlify(encrypted_private))

