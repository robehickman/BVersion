import scrypt
import nacl.signing
import nacl.secret
import nacl.utils

def key_from_password(password):
    return scrypt.hash(password, 'random salt', 4096 , 100, 1, nacl.secret.SecretBox.KEY_SIZE)


def make_keypair():
    # Generate a new random signing key
    signing_key = nacl.signing.SigningKey.generate()

    private_key = signing_key.encode(encoder=nacl.encoding.HexEncoder)
    public_key = signing_key.verify_key.encode(encoder=nacl.encoding.HexEncoder)

    return (private_key, public_key)


def encrypt_private(key, private):
    box = nacl.secret.SecretBox(key)
    # nonce must only be used once, make a new one every time
    nonce = nacl.utils.random(nacl.secret.SecretBox.NONCE_SIZE)

    # Encrypted result stores authentication information and nonce alongside it,
    # do not need to store these separately.
    return box.encrypt(private, nonce)


# Decrypt our message, an exception will be raised if the encryption was
#   tampered with or there was otherwise an error.
def decrypt_private(key, crypt_private):
    box = nacl.secret.SecretBox(key)
    return box.decrypt(crypt_private)

    print plaintext

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

