from unittest import TestCase
from shttpfs import crypto
import pysodium, base64

class TestCrypto(TestCase):
#######################################################################################
    def test_private_key_no_encrypt(self):
        crypto.raw_input = lambda : 'n'
        private_key, public_key = crypto.make_keypair()
        key = crypto.unlock_private_key(private_key)
        sig = pysodium.crypto_sign_detached('test', key)
        pysodium.crypto_sign_verify_detached(sig, 'test', base64.b64decode(public_key))


#######################################################################################
    def test_private_key_encrypt_decrypt(self):
        crypto.raw_input = lambda : 'y'
        crypto.getpass.getpass = lambda : 'test'
        private_key, public_key = crypto.make_keypair()
        key = crypto.unlock_private_key(private_key) # this will throw if there is any problem

