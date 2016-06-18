import scrypt
import nacl.signing
import nacl.secret
import nacl.utils
from binascii import hexlify, unhexlify

def key_from_password(password):
    return scrypt.hash(password, 'random salt', 4096 , 100, 1, nacl.secret.SecretBox.KEY_SIZE)

def make_keypair():
    # Generate a new random signing key
    signing_key = nacl.signing.SigningKey.generate()

    private_key = signing_key.encode(encoder=nacl.encoding.HexEncoder)
    public_key = signing_key.verify_key.encode(encoder=nacl.encoding.HexEncoder)

    return (hexlify(private_key), hexlify(public_key))

def encrypt_private(password, private):
    key = key_from_password(password)
    box = nacl.secret.SecretBox(key)
    # nonce must only be used once, make a new one every time
    nonce = nacl.utils.random(nacl.secret.SecretBox.NONCE_SIZE)

    # Encrypted result stores authentication information and nonce alongside it,
    # do not need to store these separately.
    return hexlify(box.encrypt(private, nonce))

# Decrypt our message, an exception will be raised if the encryption was
# tampered with or there was otherwise an error.
def decrypt_private(password, crypt_private):
    crypt_private = unhexlify(crypt_private)
    key = key_from_password(password)
    box = nacl.secret.SecretBox(key)
    return box.decrypt(crypt_private)

    print plaintext

def request_auth():
    nonce = nacl.utils.random(32)
    return hexlify(nonce)

def client_auth(private_key, nonce):
    nonce = unhexlify(nonce)

    signing_key = nacl.signing.SigningKey(private_key, encoder=nacl.encoding.HexEncoder)
    signed = signing_key.sign(nonce)
    return hexlify(signed)

def varify_auth(public_key, msg):
    verify_key = nacl.signing.VerifyKey(public_key, encoder=nacl.encoding.HexEncoder)
    verify_key.verify(signed)

