import binascii, base64
import pysodium
from shttpfs3.backup import pipeline

def add_default_config(config: dict):
    """ The default configuration structure. """
    config['crypto'] =  {'remote_password_salt_file'  : 'salt_file',  # Remote file used to store the password salt
                         'crypt_password'             : None }
    return config

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
def preprocess_config(interface, conn, config: dict) -> dict:
    if config['crypto']['crypt_password'] is None or config['crypto']['crypt_password'] == '':
        raise SystemExit('Please set key "crypto" : {"crypt_password"} in the config file')

    config['crypto']['crypt_password'] = config['crypto']['crypt_password'].encode('utf8') #must be a byte array

    # attempt to get salt from remote, if does not exist
    # randomly generate a salt and store it on the remote
    try:
        res = interface.get_object(conn, config['crypto']['remote_password_salt_file'])
        salt = binascii.unhexlify(res['body'].read())

    except ValueError:
        salt =pysodium.randombytes(pysodium.crypto_pwhash_SALTBYTES)
        interface.put_object(conn, config['crypto']['remote_password_salt_file'], binascii.hexlify(salt))

    # Everything in here is included as a header, never put anything in this dict that must be private
    config['crypto']['encrypt_opts'] = {
        'A' : 'ARGON2I13',
        'O' : pysodium.crypto_pwhash_argon2i_OPSLIMIT_INTERACTIVE,
        'M' : pysodium.crypto_pwhash_argon2i_MEMLIMIT_INTERACTIVE,
        'S' : base64.b64encode(salt).decode('utf-8')
    }

    key = pysodium.crypto_pwhash(pysodium.crypto_secretstream_xchacha20poly1305_KEYBYTES,
                                 config['crypto']['crypt_password'], salt,
                                 config['crypto']['encrypt_opts']['O'],
                                 config['crypto']['encrypt_opts']['M'],
                                 pysodium.crypto_pwhash_ALG_ARGON2I13)

    config['crypto']['stream_crypt_key'] = key
    return config


#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
# Streaming (chunked) encryption and decryption
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
class streaming_encrypt:
    def __init__(self, child):
        self.child = child; self.chunk_id = 0
        self.state = self.header = None; self.enable = False
        self.pipeline_header = None

    def pass_config(self, config, pipeline_header):

        if 'encrypt' in pipeline.parse_pipeline_format(pipeline_header)['format']:
            self.enable = True; crypt_key = config['crypto']['stream_crypt_key']
            self.state, self.header = pysodium.crypto_secretstream_xchacha20poly1305_init_push(crypt_key)
            self.pipeline_header = pipeline_header

        self.child.pass_config(config, pipeline_header)

    def next_chunk(self, chunk):
        if self.enable:
            if not isinstance(chunk, bytes): raise TypeError('Data must be a byte string')
            res = pysodium.crypto_secretstream_xchacha20poly1305_push(self.state, chunk, self.pipeline_header, 0)
            if self.chunk_id == 0: res = self.header + res
            chunk = res
        self.child.next_chunk(chunk); self.chunk_id += 1

class streaming_decrypt:
    def __init__(self, child):
        self.child           = child
        self.crypt_key       = None
        self.chunk_id        = 0
        self.tag             = None
        self.enable          = False
        self.pipeline_header = None
        self.state           = None

    def pass_config(self, config, pipeline_header):
        self.pipeline_header = pipeline_header

        if 'encrypt' in pipeline.parse_pipeline_format(pipeline_header)['format']:
            self.crypt_key = config['crypto']['stream_crypt_key']
            self.enable = True

    def next_chunk(self):
        if self.enable:
            if self.chunk_id == 0:
                chunk = self.child.next_chunk(pysodium.crypto_secretstream_xchacha20poly1305_ABYTES
                                              + pysodium.crypto_secretstream_xchacha20poly1305_HEADERBYTES)
                if not isinstance(chunk, bytes): raise TypeError('Data must be a byte string')
                header = chunk[:pysodium.crypto_secretstream_xchacha20poly1305_HEADERBYTES]
                chunk = chunk[pysodium.crypto_secretstream_xchacha20poly1305_HEADERBYTES:]
                self.state = pysodium.crypto_secretstream_xchacha20poly1305_init_pull(header, self.crypt_key)
            else:
                chunk = self.child.next_chunk(pysodium.crypto_secretstream_xchacha20poly1305_ABYTES)
                if chunk is not None and not isinstance(chunk, bytes): raise TypeError('Data must be a byte string')

            if chunk is None: return None
            msg, self.tag = pysodium.crypto_secretstream_xchacha20poly1305_pull(self.state, chunk, self.pipeline_header)
            self.chunk_id += 1
            return msg
        else:
            return self.child.next_chunk()
