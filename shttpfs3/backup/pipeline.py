"""
When data is uploaded or downloaded an arbitrary set of transformations
may be applied to the data in transit including encryption. This file
assembles pipelines to apply these transformations depending on configuration.
"""
import functools, json, re
import rrbackup.crypto as crypto
import rrbackup.compress as compress

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++==
def preprocess_config(interface, conn, config: dict):
    """ apply transformations to configuration data which should only be done once,
    for example key derivation """

    return crypto.preprocess_config(interface, conn, config)

#================================================================
#================================================================
def get_default_pipeline_format() -> dict:
    return {'version' : 1,
            'format'  : {}}

#------------------
serialise_mapper = {'encrypt'    : 'E',
                    'compress'   : 'C',
                    'hash_names' : 'H'}

def serialise_pipeline_format(pl_format: dict) -> bytes:
    """ For a given version the output of this MUST NOT CHANGE as it
    is used as additional data for validation"""
    if not isinstance(pl_format, dict): raise TypeError('pipeline format must be a dict')
    if not isinstance(pl_format['version'], int):
        raise TypeError('Version must be an integer')

    to_json = {'V' : str(pl_format['version'])}

    if'chunk_size' in  pl_format:
        to_json['S'] = str(pl_format['chunk_size'])

    for i in pl_format['format']:
        if not isinstance(i, str):
            raise TypeError('Format specifiers must be strings')

        if isinstance(pl_format['format'][i], dict):  to_json[serialise_mapper[i]] = pl_format['format'][i]
        elif pl_format['format'][i] is None: to_json[serialise_mapper[i]] = ''
        else: raise TypeError('Unexpected type')

    return json.dumps(to_json, separators=(',',':')).encode('utf-8')

def parse_pipeline_format(serialised_pl_format: bytes) -> dict:
    raw = json.loads(serialised_pl_format)

    if 'V' not in raw: raise ValueError('Version not found')
    version = raw.pop('V')
    if not re.compile(r'^[0-9]+$').match(version): raise ValueError('Invalid version number')
    pl_format = {'version' : int(version), 'format'  : {}}

    try: pl_format['chunk_size'] = int(raw.pop('S'))
    except: pass

    inv_map = {v: k for k, v in serialise_mapper.items()}
    for k, v in raw.items():
        if v == '': pl_format['format'][inv_map[k]] = None
        elif isinstance(v, dict): pl_format['format'][inv_map[k]] = v
        else: raise ValueError('Unknown type in serialised pipeline format')

    return pl_format

#================================================================
#================================================================
def build_pipeline(interface, direction):
    """ Build a flat pipeline of transformers,

        Direction specifies whether processing data heading to storage or returning,
        it has two valid options: out or in.
     """

    pipeline = interface

    if direction == 'out':
        # Remember that these are executed in the reverse order than they are listed
        pipeline = functools.partial(crypto.encrypt, pipeline)
        pipeline = functools.partial(compress.compress, pipeline)

    elif direction == 'in':
        pipeline = functools.partial(crypto.decrypt, pipeline)
        pipeline = functools.partial(compress.decompress, pipeline)

    else:
        raise ValueError('Unknown pipeline direction')

    return pipeline

# -----------------
def build_pipeline_streaming(interface, direction):
    """ Build a chunked (streaming) pipeline of transformers """

    pipeline = interface

    if direction == 'out':
        pipeline = crypto.streaming_encrypt(pipeline)

    elif direction == 'in':
        pipeline = crypto.streaming_decrypt(pipeline)

    else:
        raise ValueError('Unknown pipeline direction')

    return pipeline
