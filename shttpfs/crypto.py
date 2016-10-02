import scrypt
import nacl.signing
import nacl.secret
import nacl.utils
from binascii import hexlify, unhexlify

from common import *

############################################################################################
# Generate a key from a password
############################################################################################
def key_from_password(password):
    return scrypt.hash(password, 'random salt', 4096 , 100, 1, nacl.secret.SecretBox.KEY_SIZE)

############################################################################################
# Create a new public and private key pair
############################################################################################
def make_keypair():
    # Generate a new random signing key
    signing_key = nacl.signing.SigningKey.generate()

    private_key = signing_key.encode(encoder=nacl.encoding.HexEncoder)
    public_key = signing_key.verify_key.encode(encoder=nacl.encoding.HexEncoder)

    return (private_key, public_key)

############################################################################################
# Create a new public and private key pair and write to files
############################################################################################
def write_keypair(password, pubkey_file, pubkey_ext, privkey_file, privkey_ext):
    private, public = make_keypair()
    encrypted_private = encrypt_private(password, private)

    make_dirs_if_dont_exist(pubkey_file)
    make_dirs_if_dont_exist(privkey_file)

    file_put_contents(exsure_extension(pubkey_file,  pubkey_ext), public)
    file_put_contents(exsure_extension(privkey_file, privkey_ext), encrypted_private)

    return (encrypted_private, public) 

############################################################################################
# Encrypt a private key
############################################################################################
def encrypt_private(password, private, dohex = True):
    key = key_from_password(password)
    box = nacl.secret.SecretBox(key)
    # nonce must only be used once, make a new one every time
    nonce = nacl.utils.random(nacl.secret.SecretBox.NONCE_SIZE)

    # Encrypted result stores authentication information and nonce alongside it,
    # do not need to store these separately.

    result = box.encrypt(private, nonce)
    if dohex == True:
        result = hexlify(result)
    return result

############################################################################################
# Decrypt a private key
############################################################################################
def decrypt_private(password, crypt_private, dohex = True):
    if dohex == True:
        crypt_private = unhexlify(crypt_private)

    key = key_from_password(password)
    box = nacl.secret.SecretBox(key)
    return box.decrypt(crypt_private)

############################################################################################
# Get random bytes
############################################################################################
def random_bytes(length):
    nonce = nacl.utils.random(length)
    return hexlify(nonce)

############################################################################################
# Digitally sign some data
############################################################################################
def sign_data(private_key, data):
    signing_key = nacl.signing.SigningKey(private_key, encoder=nacl.encoding.HexEncoder)
    signed_token = signing_key.sign(data)

    return signed_token

############################################################################################
# Verify the signature of some data
############################################################################################
def varify_signiture(public_key, signed_data):
    try:
        verify_key = nacl.signing.VerifyKey(public_key, encoder=nacl.encoding.HexEncoder)
        verify_key.verify(signed_data)
        return True
    except:
        return False

