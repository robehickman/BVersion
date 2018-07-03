import pysodium, base64, getpass
from binascii import hexlify, unhexlify
#from common import *

#===============================================================================
def prompt_for_new_password():
    """ Prompt the user to enter a new password, with confirmation """
    while True:
        passw = getpass.getpass()
        passw2 = getpass.getpass()
        if passw == passw2: return passw
        print 'Passwords do not match'

#===============================================================================
def hash_password(password, salt):
    return  pysodium.crypto_pwhash(pysodium.crypto_secretbox_KEYBYTES, password, salt,
                                   pysodium.crypto_pwhash_argon2i_OPSLIMIT_INTERACTIVE,
                                   pysodium.crypto_pwhash_argon2i_MEMLIMIT_INTERACTIVE,
                                   pysodium.crypto_pwhash_ALG_ARGON2I13)

#===============================================================================
def make_keypair():
    public_key, private_key = pysodium.crypto_sign_keypair()
    print 'Do you wish to encrypt the private key under a password? (y/n)'
    answer = raw_input().lower()
    if answer not in ['y', 'n']: raise SystemExit('Invalid answer')
    if answer == 'y':
        salt = pysodium.randombytes(pysodium.crypto_pwhash_SALTBYTES)
        key = hash_password(prompt_for_new_password(), salt)
        nonce = pysodium.randombytes(pysodium.crypto_box_NONCEBYTES)
        cyphertext = pysodium.crypto_secretbox(private_key, nonce, key)
        private_key = b'y'  + salt + nonce + cyphertext
    else:
        private_key = b'n' + private_key

    return base64.b64encode(private_key), base64.b64encode(public_key)

#===============================================================================
def unlock_private_key(private_key):
    private_key = base64.b64decode(private_key)

    if private_key[0] == b'y':
        sbytes = pysodium.crypto_pwhash_SALTBYTES
        nbytes = pysodium.crypto_box_NONCEBYTES
        salt  = private_key[1:sbytes+1]
        nonce = private_key[sbytes+1: sbytes+nbytes+1]
        cyphertext = private_key[sbytes+nbytes+1:]
        key = hash_password(getpass.getpass(), salt)
        private_key = pysodium.crypto_secretbox_open(cyphertext, nonce, key)
        return private_key
    return private_key[1:]

