from helpers import *
from unittest import TestCase

from shttpfs.crypto import *

class TestCrypto(TestCase):
#######################################################################################
    def test_key_encrypt_decrypt(self):
        """ Test that encryption and decryption of a item returns the same item """

        prvate, public = make_keypair()

        encrypted = encrypt_private('test', prvate)
        decrypted = decrypt_private('test', encrypted)

        self.assertEqual(decrypted, prvate,
            msg = 'Decrypted result does not match source')

#######################################################################################
    def test_sign_varify(self):
        """ Test signing and verification that signature """

        data = 'something to sign'

        prv, pub = make_keypair()

        signed = sign_data(prv, data)

        self.assertTrue(varify_signiture(pub, signed),
            msg='Signature verification failed')

#######################################################################################
    def test_key_wirte(self):
        """ Test keys are written to files correctly """

        empty_dir('keys')

        prv, pub = write_keypair('test', 'keys/public', '.key', 'keys/private', '.key')
        private = file_get_contents('keys/private.key')
        public  = file_get_contents('keys/public.key')

        self.assertEqual(prv, private,
            msg = 'private key file corrupted')

        self.assertEqual(pub, public,
            msg = 'public key file corrupted')

        data = 'something to sign'

        prv     = decrypt_private('test', prv)
        private = decrypt_private('test', private)

        signed = sign_data(prv, data)

        self.assertTrue(varify_signiture(pub, signed),
            msg='Signature verification failed')

        signed = sign_data(private, data)
        self.assertTrue(varify_signiture(public, signed),
            msg='Signature verification failed')

        empty_dir('keys')
        os.rmdir('keys')
        
