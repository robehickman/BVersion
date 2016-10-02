from test_common import *

import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
parentdir = parentdir + '/lib'
sys.path.insert(0,parentdir) 

from crypto import *

#######################################################################################
# Test that encryption and decryption of a item returns the same item
#######################################################################################
def test_key_encrypt_decrypt():
    prvate, public = make_keypair()

    encrypted = encrypt_private('test', prvate)
    decrypted = decrypt_private('test', encrypted)

    if decrypted != prvate:
        raise Exception('Decrypted result does not match source')

    print "Encrypt and decrypt pass"

#######################################################################################
# Test signing and verification that signature
#######################################################################################
def test_sign_varify():
    data = 'something to sign'

    prv, pub = make_keypair()

    signed = sign_data(prv, data)
    varify_signiture(pub, signed)

    print "Sign and verify pass"

#######################################################################################
# Test keys are written to files correctly
#######################################################################################
def test_key_wirte():
    empty_dir('keys')

    prv, pub = write_keypair('test', 'keys/public', '.key', 'keys/private', '.key')
    private = file_get_contents('keys/private.key')
    public  = file_get_contents('keys/public.key')

    if prv != private:
        print 'private key file corrupted'

    if pub != public:
        print 'public key file corrupted'

    data = 'something to sign'

    prv     = decrypt_private('test', prv)
    private = decrypt_private('test', private)

    signed = sign_data(prv, data)
    varify_signiture(pub, signed)

    signed = sign_data(private, data)
    varify_signiture(public, signed)

    empty_dir('keys')
    os.rmdir('keys')
    
    print "Key write to file pass"


# run tests
test_key_encrypt_decrypt()
test_sign_varify()
test_key_wirte()

    
