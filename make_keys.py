#import nacl.encoding

import os
#import hashlib
import binascii
import nacl.signing
#import nacl.secret
import scrypt

import nacl.secret
import nacl.utils

import getpass

import argparse

print ''




def process_password(password):
    return scrypt.hash(password, 'random salt', 2048, 100, 1, nacl.secret.SecretBox.KEY_SIZE)


def make_keypair():
    # Generate a new random signing key
    signing_key = nacl.signing.SigningKey.generate()

    private_key = signing_key.encode(encoder=nacl.encoding.HexEncoder)
    public_key = signing_key.verify_key.encode(encoder=nacl.encoding.HexEncoder)

    return (private_key, public_key)


# This must be kept secret, this is the combination to your safe
#key = nacl.utils.random(nacl.secret.SecretBox.KEY_SIZE)
# This is your safe, you can use it to encrypt or decrypt messages
# This is a nonce, it *MUST* only be used once, but it is not considered
#   secret and can be transmitted or stored alongside the ciphertext. A
#   good source of nonce is just 24 random bytes.
# Encrypt our message, it will be exactly 40 bytes longer than the original
#   message as it stores authentication information and nonce alongside it.

def encrypt_private(key, private):

    box = nacl.secret.SecretBox(key)

    nonce = nacl.utils.random(nacl.secret.SecretBox.NONCE_SIZE)
    encrypted = box.encrypt(private, nonce)

    return encrypted


# Decrypt our message, an exception will be raised if the encryption was
#   tampered with or there was otherwise an error.
def decrypt_private(key, crypt_private):
    box = nacl.secret.SecretBox(key)
    return box.decrypt(crypt_private)

    print plaintext

public, private = make_keypair()

def get_password():
    while True:
        passw = getpass.getpass()
        passw2 = getpass.getpass()

        if passw == passw2:
            break
        else:
            print 'Passwords do not match'
    return passw


key = process_password(get_password())

encrypted_private = encrypt_private(key, private)


parser = argparse.ArgumentParser(description='Generate public and private key pair for authentication.')
parser.add_argument('pubkey', nargs=1 , help='Public key file')
parser.add_argument('privkey', nargs=1, help='Public key file')
args = parser.parse_args()


pubkey_file  = args.privkey[0]
privkey_file = args.pubkey[0]


os.makedirs(os.path.dirname(pubkey_file))
os.makedirs(os.path.dirname(privkey_file))


with open(pubkey_file + '.key', 'w') as f:
    f.write(binascii.hexlify(public))

with open(privkey_file + '.key', 'w') as f:
    f.write(binascii.hexlify(encrypted_private))

quit()


print ''







def request_auth():
    nonce = nacl.utils.random(nacl.secret.SecretBox.KEY_SIZE)
    return nonce

def client_auth(private_key, nonce):
    pass
    # load the private key
    signed = signing_key.sign(nonce)
    return signed

def varify_auth(public_key, msg):
    verify_key = nacl.signing.VerifyKey(public_key, encoder=nacl.encoding.HexEncoder)
    verify_key.verify(signed)



#hashlib.pbkdf2_hmac('sha256', 'test', salt, 200000, dklen=nacl.secret.SecretBox.KEY_SIZE)


"""
# Sign a message with the signing key
signed = signing_key.sign("Attack at Dawn")

# Obtain the verify key for a given signing key
verify_key = signing_key.verify_key

# Serialize the verify key to send it to a third party
verify_key_hex = verify_key.encode(encoder=nacl.encoding.HexEncoder)
"""


#import nacl.signing

# Create a VerifyKey object from a hex serialized public key
#verify_key = nacl.signing.VerifyKey(verify_key_hex, encoder=nacl.encoding.HexEncoder)

# Check the validity of a message's signature
# Will raise nacl.signing.BadSignatureError if the signature check fails

#print signed

#verify_key.verify(signed)
