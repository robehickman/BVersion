from unittest import TestCase
import base64, pysodium

from shttpfs3 import crypto

class TestCrypto(TestCase):
#######################################################################################
    def test_private_key_no_encrypt(self):
        crypto.input = lambda prompt : 'n'
        private_key, public_key = crypto.make_keypair()
        key = crypto.unlock_private_key(private_key)
        sig = pysodium.crypto_sign_detached('test', key)
        pysodium.crypto_sign_verify_detached(sig, 'test', base64.b64decode(public_key))


#######################################################################################
    def test_private_key_unlock(self):
        crypto.input = lambda prompt : 'y'
        crypto.getpass.getpass = lambda : 'test'
        private_key = crypto.make_keypair()[0]
        crypto.unlock_private_key(private_key) # this will throw if there is any problem

