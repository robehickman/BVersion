import  base64, getpass, pysodium
from typing import Tuple

#===============================================================================
def prompt_for_new_password() -> str:
    """ Prompt the user to enter a new password, with confirmation """
    while True:
        passw: str  = getpass.getpass()
        passw2: str = getpass.getpass()
        if passw == passw2: return passw
        else: print('Passwords do not match')

#===============================================================================
def hash_password(password: str, salt: bytes) -> bytes:
    return  pysodium.crypto_pwhash(pysodium.crypto_secretbox_KEYBYTES, password, salt,
                                   pysodium.crypto_pwhash_argon2i_OPSLIMIT_INTERACTIVE,
                                   pysodium.crypto_pwhash_argon2i_MEMLIMIT_INTERACTIVE,
                                   pysodium.crypto_pwhash_ALG_ARGON2I13)

#===============================================================================
def make_keypair() -> Tuple[bytes, bytes]:
    public_key, private_key = pysodium.crypto_sign_keypair()
    print('Do you wish to encrypt the private key under a password? (y/n)')
    
    answer: str
    while True:
        answer = input('>').lower()
        if answer not in ['y', 'n']: print('Invalid answer')
        else: break

    if answer == 'y':
        salt:       bytes = pysodium.randombytes(pysodium.crypto_pwhash_SALTBYTES)
        key:        bytes = hash_password(prompt_for_new_password(), salt)
        nonce:      bytes = pysodium.randombytes(pysodium.crypto_box_NONCEBYTES)
        cyphertext: bytes = pysodium.crypto_secretbox(private_key, nonce, key)
        private_key = b'y'  + salt + nonce + cyphertext
    else:
        private_key = b'n' + private_key

    return base64.b64encode(private_key), base64.b64encode(public_key)

#===============================================================================
def unlock_private_key(private_key_b64: str) -> bytes:
    private_key: bytes = base64.b64decode(private_key_b64)

    if private_key[0] == b'y':
        sbytes: int = pysodium.crypto_pwhash_SALTBYTES
        nbytes: int = pysodium.crypto_box_NONCEBYTES
        salt: bytes = private_key[1:sbytes+1]
        nonce: bytes = private_key[sbytes+1: sbytes+nbytes+1]
        cyphertext: bytes = private_key[sbytes+nbytes+1:]
        key: bytes = hash_password(getpass.getpass(), salt)
        private_key = pysodium.crypto_secretbox_open(cyphertext, nonce, key)
        return private_key
    else:
        return private_key[1:]

