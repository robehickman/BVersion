#import nacl.encoding
import nacl.signing

def make_keypair():
    # Generate a new random signing key
    signing_key = nacl.signing.SigningKey.generate()

    private_key = signing_key..encode(encoder=nacl.encoding.HexEncoder)
    public_key = signing_key.verify_key.encode(encoder=nacl.encoding.HexEncoder)

    return (private_key, public_key)

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
